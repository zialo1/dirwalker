"""Tests for DataBaseCore.py."""

from unittest import mock
import DataBaseCore as core
# Importing a test module must not probe the configured production database.
with mock.patch.object(core.DataBaseConnectionCore, 'connect', side_effect=RuntimeError('offline test')):
    import version25 as app
import unittest
from datetime import datetime
from datetime import timezone
import DataBaseCore
from DataBaseCLI import DataBaseCLI
from client_operations import ClientArgumentParser
from pathlib import Path
from types import SimpleNamespace
import contextlib
import io
import multiprocessing
import mysql.connector
import os
import server_operations as server
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from datetime import timedelta
import re
import sqlite3
from DataBaseCore import AgeRescanWorkflow
from client_operations import client_operations
from sqlstructure import SQLStructure
from server_operations import _server_instance_lock


class TestDatabaseConnection(unittest.TestCase):
    def _schema_rows(self, root_collation="utf8mb4_bin", noperm_default=0, noperm_type="bigint unsigned"):
        rows = []
        for table, shape in app.DatabaseConnection.EXPECTED_TABLE_SHAPES.items():
            for column, (typ, nullable, default, collation) in shape["columns"].items():
                if table == "directories" and column == "root_path":
                    collation = root_collation
                if table == "directories" and column == "nopermdirs":
                    typ, default = noperm_type, noperm_default
                rows.append((table, column, typ, nullable, default, collation))
        return rows

    def test_utc_now_is_a_naive_datetime_property(self):
        database = app.database_connection
        before = datetime.now(timezone.utc).replace(tzinfo=None)
        value = database.utc_now
        after = datetime.now(timezone.utc).replace(tzinfo=None)
        self.assertIsInstance(value, datetime)
        self.assertIsNone(value.tzinfo)
        self.assertLessEqual(before, value)
        self.assertLessEqual(value, after)

    def test_connect_is_context_managed_and_returns_fresh_handles(self):
        database = object.__new__(app.DatabaseConnection)
        database.database = "testdb"
        database.host = "db.example"
        database.port = 3307
        database.database = "testdb"
        database.user = "tester"
        database.password = "secret"
        first = mock.MagicMock()
        second = mock.MagicMock()
        first.__enter__.return_value = first
        second.__enter__.return_value = second

        with mock.patch.object(DataBaseCore, "DATABASE_DRIVER", "MySQL Connector/Python"), \
             mock.patch.object(DataBaseCore, "_database_connect", side_effect=[first, second]) as connector:
            with database.connect() as first_handle:
                self.assertIs(first_handle, first)
            with database.connect_database() as second_handle:
                self.assertIs(second_handle, second)

        self.assertEqual(connector.call_count, 2)
        self.assertIsNot(first, second)
        expected = {
            "host": "db.example", "port": 3307, "database": "testdb",
            "user": "tester", "password": "secret",
            "autocommit": False, "charset": "utf8mb4", "connect_timeout": 10, "use_pure": True,
        }
        connector.assert_has_calls([mock.call(**expected), mock.call(**expected)])

    def test_mariadb_driver_omits_unsupported_charset_option(self):
        database = object.__new__(app.DatabaseConnection)
        database.database = "testdb"
        database.host = "db.example"
        database.port = 3307
        database.database = "testdb"
        database.user = "tester"
        database.password = "secret"
        with mock.patch.object(DataBaseCore, "DATABASE_DRIVER", "MariaDB Connector/Python"), \
             mock.patch.object(DataBaseCore, "_database_connect") as connector:
            database.connect()
        self.assertNotIn("charset", connector.call_args.kwargs)

    def test_module_wrapper_uses_global_database_object(self):
        sentinel = object()
        with mock.patch.object(app.database_connection, "connect_database", return_value=sentinel) as connect:
            self.assertIs(app.database_connection.connect_database(), sentinel)
        connect.assert_called_once_with()


class TestCentralRunRecordWriter(unittest.TestCase):
    def database(self):
        return object.__new__(app.DataBaseConnection)

    def test_current_process_identity_is_added_by_default(self):
        database, cursor = self.database(), mock.Mock()
        stamp = datetime(2026, 10, 29, 17, 0)
        with mock.patch.object(DataBaseCore.os, 'getpid', return_value=1234), \
             mock.patch.object(DataBaseCore.socket, 'gethostname', return_value='server'), \
             mock.patch.object(DataBaseCore, '_process_start_token', return_value='birth') as identify:
            database.write_run_record(cursor, 30, 13, 'scan', 'active', 'coarse', timecreated=stamp)
        identify.assert_called_once_with(1234)
        self.assertEqual(cursor.execute.call_args.args[1],
                         (30, 13, 'scan', 'active', 'coarse', stamp, 1234, 'server', 'birth', None))

    def test_parent_can_register_child_instead_of_its_own_pid(self):
        database, cursor = self.database(), mock.Mock()
        with mock.patch.object(DataBaseCore, '_process_start_token') as identify:
            database.write_run_record(cursor, 31, 13, 'binarycheck', 'active', 'highpriority',
                                      pid=5678, process_host='server', process_start='child birth')
        identify.assert_not_called()
        self.assertEqual(cursor.execute.call_args.args[1][6:], (5678, 'server', 'child birth', None))

    def test_process_identity_is_detected_for_an_explicit_child_pid(self):
        database, cursor = self.database(), mock.Mock()
        with mock.patch.object(DataBaseCore, '_process_start_token', return_value='child birth') as identify:
            database.write_run_record(cursor, 31, 13, 'scan', 'active', 'detailed', pid=5678)
        identify.assert_called_once_with(5678)
        self.assertEqual(cursor.execute.call_args.args[1][6], 5678)

    def test_missing_identity_never_writes_a_partial_history_record(self):
        for kwargs in ({'pid': 0}, {'pid': -1}, {'process_host': ''}, {'process_start': ''}):
            database, cursor = self.database(), mock.Mock()
            with self.subTest(kwargs=kwargs), \
                 mock.patch.object(DataBaseCore, '_process_start_token', return_value='birth'), \
                 self.assertRaises((ValueError, RuntimeError)):
                database.write_run_record(cursor, 30, 13, 'scan', 'active', 'coarse', **kwargs)
            cursor.execute.assert_not_called()
        database, cursor = self.database(), mock.Mock()
        with mock.patch.object(DataBaseCore, '_process_start_token', return_value=None), self.assertRaises(RuntimeError):
            database.write_run_record(cursor, 30, 13, 'scan', 'active', 'coarse')
        cursor.execute.assert_not_called()


class TestLocalStartJobs(unittest.TestCase):
    def test_read_only_status_query_is_scoped_to_positive_hostids(self):
        database = object.__new__(app.DataBaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchall.return_value = [(13, 7, 'error', 'detailed', '/data', 'failed', 'unreadable')]
        with mock.patch.object(database, 'connect', return_value=connection):
            jobs = database.local_start_jobs([8, 7, 7])
        sql, params = cursor.execute.call_args.args
        self.assertEqual(params, (7, 8))
        self.assertTrue(sql.startswith('SELECT'))
        self.assertIn("'error'", sql)
        self.assertEqual((jobs[0]['status'], jobs[0]['errmsg']), ('error', 'failed'))
        cursor.close.assert_called_once()
        connection.commit.assert_not_called()

    def test_host_zero_is_rejected_before_database_access(self):
        database = object.__new__(app.DataBaseConnection)
        with mock.patch.object(database, 'connect') as connect, self.assertRaises(ValueError):
            database.local_start_jobs([0, 7])
        connect.assert_not_called()


class TestBoundedParentRepair(unittest.TestCase):
    def fixture(self, rows):
        import sqlite3
        sql = sqlite3.connect(':memory:')
        self.addCleanup(sql.close)
        sql.execute('CREATE TABLE directories (dirid INTEGER PRIMARY KEY, hostid INTEGER, root_path TEXT, path TEXT, parentdir INTEGER)')
        sql.executemany('INSERT INTO directories VALUES (?, ?, ?, ?, ?)', rows)
        sql.commit()
        cursor = mock.Mock()
        inner = sql.cursor()
        self.addCleanup(inner.close)
        cursor.execute.side_effect = lambda query, params: inner.execute(query.replace('%s', '?'), params)
        cursor.fetchall.side_effect = inner.fetchall
        cursor.executemany.side_effect = lambda query, values: inner.executemany(query.replace('%s', '?'), values)
        connection = mock.Mock()
        connection.commit.side_effect = sql.commit
        connection.rollback.side_effect = sql.rollback
        return object.__new__(app.DataBaseConnection), sql, connection, cursor

    def test_parent_repair_is_host_root_and_case_sensitive_and_idempotent(self):
        rows = [
            (1, 7, '/data', '/data', 99),
            (2, 7, '/data', '/data/Case', 0),
            (3, 7, '/data', '/data/case', 0),
            (4, 7, '/data', '/data/Case/ä b', 3),
            (5, 7, '/data', '/data/missing/leaf', 99),
            (6, 8, '/data', '/data/Case', 88),
            (7, 7, '/other', '/data/missing', 77),
        ]
        db, sql, connection, cursor = self.fixture(rows)
        self.assertEqual(db._repair_directory_parents(connection, cursor, 7, '/data', 1, batch_size=2), 5)
        self.assertEqual(sql.execute('SELECT dirid, parentdir FROM directories ORDER BY dirid').fetchall(),
                         [(1, 0), (2, 1), (3, 1), (4, 2), (5, 0), (6, 88), (7, 77)])
        self.assertEqual([len(call.args[1]) for call in cursor.executemany.call_args_list], [2, 2, 1])
        self.assertEqual(connection.commit.call_count, 4)
        self.assertTrue(all('WHERE dirid = %s AND hostid = %s AND root_path = %s' in call.args[0]
                            for call in cursor.executemany.call_args_list))
        cursor.executemany.reset_mock()
        self.assertEqual(db._repair_directory_parents(connection, cursor, 7, '/data', 1), 0)
        cursor.executemany.assert_not_called()

    def test_filesystem_root_cannot_become_its_own_parent(self):
        db, sql, connection, cursor = self.fixture([(1, 7, '/', '/', 1), (2, 7, '/', '/leaf', 0)])
        db._repair_directory_parents(connection, cursor, 7, '/', 1)
        self.assertEqual(sql.execute('SELECT parentdir FROM directories ORDER BY dirid').fetchall(), [(0,), (1,)])

    def test_large_already_consistent_tree_needs_no_write(self):
        count = 20000
        rows = [(1, 7, '/data', '/data', 0)] + [
            (i, 7, '/data', f'/data/child{i}', 1) for i in range(2, count + 1)]
        db, sql, connection, cursor = self.fixture(rows)
        self.assertEqual(db._repair_directory_parents(connection, cursor, 7, '/data', 1), 0)
        cursor.execute.assert_called_once()
        cursor.executemany.assert_not_called()
        connection.commit.assert_called_once()

    def test_cleanup_does_not_mask_original_failure(self):
        database = object.__new__(app.DataBaseConnection)
        original = RuntimeError('2013: lost connection during UPDATE')
        connection = mock.Mock()
        connection.rollback.side_effect = RuntimeError('rollback disconnected')
        cursor = mock.Mock()
        cursor.close.side_effect = RuntimeError('close disconnected')
        with self.assertLogs(level='WARNING'), self.assertRaises(RuntimeError) as caught:
            try:
                try:
                    raise original
                except Exception:
                    database._rollback_preserving_error(connection)
                    raise
            finally:
                database._close_preserving_error(cursor)
        self.assertIs(caught.exception, original)

    def test_cleanup_failure_without_original_exception_remains_visible(self):
        database = object.__new__(app.DataBaseConnection)
        resource = mock.Mock()
        resource.close.side_effect = RuntimeError('close failed')
        with self.assertRaisesRegex(RuntimeError, 'close failed'):
            database._close_preserving_error(resource)

    def test_interrupted_batch_can_resume_without_rewriting_committed_parents(self):
        rows = [(1, 7, '/data', '/data', 0)] + [
            (i, 7, '/data', f'/data/child{i}', 0) for i in range(2, 8)]
        db, sql, connection, cursor = self.fixture(rows)
        original_write = cursor.executemany.side_effect
        calls = 0
        def fail_second_batch(query, values):
            nonlocal calls
            calls += 1
            if calls == 2:
                original_write(query, values[:1])
                raise RuntimeError('connection lost during batch')
            return original_write(query, values)
        cursor.executemany.side_effect = fail_second_batch
        with self.assertRaisesRegex(RuntimeError, 'connection lost'):
            try:
                db._repair_directory_parents(connection, cursor, 7, '/data', 1, batch_size=2)
            except Exception:
                db._rollback_preserving_error(connection)
                raise
        self.assertEqual(sql.execute('SELECT COUNT(*) FROM directories WHERE parentdir=1').fetchone()[0], 2)
        cursor.executemany.side_effect = original_write
        self.assertEqual(db._repair_directory_parents(connection, cursor, 7, '/data', 1, batch_size=2), 4)
        self.assertEqual(sql.execute('SELECT COUNT(*) FROM directories WHERE parentdir=1').fetchone()[0], 6)


class TestOwnedQueryCleanup(unittest.TestCase):
    def setup_database(self):
        database = object.__new__(DataBaseCore.DataBaseConnectionCore)
        database.database, database.user = 'testdb', 'tester'
        connection = mock.Mock()
        database._control_connection = mock.Mock(return_value=connection)
        connection.cursor.return_value.fetchone.return_value = (1,)
        return database, connection, connection.cursor.return_value

    def row(self, ident=41, hostid=7, machine=None, pid=987, start='old', group='group'):
        import json
        import socket
        payload = dict(hostid=hostid, machine=machine or socket.gethostname(), pid=pid,
                       start=start, group=group)
        sql = '/*dirwalker:' + json.dumps(payload).encode().hex() + '*/ SELECT 1'
        return (ident, 'tester', 'localhost:123', 'testdb', 'Query', 50, 'Sending data', sql)

    def test_kills_only_dead_owned_connection_and_waits_until_removed(self):
        database, connection, cursor = self.setup_database()
        row = self.row()
        cursor.fetchall.side_effect = [[row, self.row(42, hostid=8), self.row(43, machine='other')], [row], []]
        with mock.patch.object(DataBaseCore, '_process_start_token', return_value=None), \
             mock.patch.object(DataBaseCore.time, 'sleep'):
            database.cleanup_server_queries(7)
        self.assertEqual([call.args[0] for call in cursor.execute.call_args_list],
                         ['SELECT CONNECTION_ID()', 'SHOW FULL PROCESSLIST', 'KILL CONNECTION 41',
                          'SHOW FULL PROCESSLIST', 'SHOW FULL PROCESSLIST'])
        connection.close.assert_called_once()

    def test_live_owner_is_never_killed_at_smart_start(self):
        database, _, cursor = self.setup_database()
        cursor.fetchall.return_value = [self.row()]
        with mock.patch.object(DataBaseCore, '_process_start_token', return_value='old'), \
             self.assertRaisesRegex(RuntimeError, 'lebenden Prozess'):
            database.cleanup_server_queries(7)
        self.assertEqual(cursor.execute.call_count, 2)
        self.assertNotIn(mock.call('KILL CONNECTION 41'), cursor.execute.call_args_list)

    def test_shutdown_kills_only_its_group_even_if_process_is_alive(self):
        database, _, cursor = self.setup_database()
        cursor.fetchall.side_effect = [[self.row(), self.row(42, group='another')], []]
        database.cleanup_server_queries(7, group='group')
        self.assertIn(mock.call('KILL CONNECTION 41'), cursor.execute.call_args_list)
        self.assertNotIn(mock.call('KILL CONNECTION 42'), cursor.execute.call_args_list)

    def test_rollback_timeout_prevents_start(self):
        database, _, cursor = self.setup_database()
        cursor.fetchall.return_value = [self.row()]
        with mock.patch.object(DataBaseCore, '_process_start_token', return_value=None), \
             self.assertRaisesRegex(RuntimeError, 'Kein neuer Scan'):
            database.cleanup_server_queries(7, timeout=0)

    def test_legacy_query_is_reported_but_not_killed(self):
        database, _, cursor = self.setup_database()
        cursor.fetchall.return_value = [(99, 'tester', 'localhost', 'testdb', 'Query', 99, '', 'UPDATE directories SET x=1')]
        import contextlib
        import io
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertFalse(database.cleanup_server_queries(7, confirm=True))
        self.assertEqual(output.getvalue(), 'KILL CONNECTION 99;\n')
        self.assertEqual(cursor.execute.call_count, 2)

    def test_tagged_cursor_preserves_parameters_and_bulk_execution(self):
        cursor = mock.Mock()
        tagged = DataBaseCore._TaggedCursor(cursor, '/*tag*/ ')
        tagged.execute('SELECT %s', (3,))
        tagged.executemany('INSERT INTO t VALUES (%s)', [(4,)])
        cursor.execute.assert_called_once_with('/*tag*/ SELECT %s', (3,))
        cursor.executemany.assert_called_once_with('/*tag*/ INSERT INTO t VALUES (%s)', [(4,)])


    def test_smart_start_requires_y_before_kill(self):
        database, _, cursor = self.setup_database()
        cursor.fetchall.side_effect = [[self.row()], []]
        with mock.patch('builtins.input', return_value='y'):
            self.assertTrue(database.cleanup_server_queries(7, confirm=True))
        self.assertIn(mock.call('KILL CONNECTION 41'), cursor.execute.call_args_list)

    def test_smart_start_defaults_to_no(self):
        for answer in ('', 'n'):
            database, _, cursor = self.setup_database()
            cursor.fetchall.return_value = [self.row()]
            with mock.patch('builtins.input', return_value=answer):
                self.assertFalse(database.cleanup_server_queries(7, confirm=True))
            self.assertEqual(cursor.execute.call_count, 2)

    def test_control_connection_never_kills_itself(self):
        database, _, cursor = self.setup_database()
        cursor.fetchall.return_value = [(1, 'tester', 'localhost', 'testdb', 'Query', 0, '', 'SHOW FULL PROCESSLIST')]
        self.assertTrue(database.cleanup_server_queries(7, confirm=True))
        self.assertEqual(cursor.execute.call_count, 2)

    def test_unknown_queries_prevent_prompts_or_partial_kills(self):
        database, _, cursor = self.setup_database()
        cursor.fetchall.return_value = [self.row(), (99, 'tester', 'localhost', 'testdb', 'Query', 99, '', 'SELECT 1')]
        with mock.patch('builtins.input') as prompt:
            self.assertFalse(database.cleanup_server_queries(7, confirm=True))
        prompt.assert_not_called()
        self.assertEqual(cursor.execute.call_count, 2)

    def test_shutdown_ignores_unrelated_untagged_queries(self):
        database, _, cursor = self.setup_database()
        cursor.fetchall.side_effect = [[self.row(), (99, 'tester', 'localhost', 'testdb', 'Query', 99, '', 'SELECT 1')], []]
        self.assertTrue(database.cleanup_server_queries(7, group='group'))
        self.assertIn(mock.call('KILL CONNECTION 41'), cursor.execute.call_args_list)
        self.assertNotIn(mock.call('KILL CONNECTION 99'), cursor.execute.call_args_list)


class TestStatementTimeoutDetection(unittest.TestCase):
    def test_error_code_recognizes_timeout_without_translated_text(self):
        error = RuntimeError('translated server error')
        error.errno = 1969
        message = DataBaseCore.statement_timeout_message(error)
        self.assertIn('max_statement_time', message)
        self.assertIn('1969', message)
        self.assertIn('360', message)

    def test_cleanup_error_does_not_hide_original_timeout(self):
        original = RuntimeError('timeout')
        original.errno = 1969
        cleanup = RuntimeError('rollback connection lost')
        cleanup.__context__ = original
        self.assertIsNotNone(DataBaseCore.statement_timeout_message(cleanup))

    def test_priority_traceback_and_numeric_driver_args_are_recognized(self):
        for error in (RuntimeError(1969, 'timeout'), RuntimeError(
                'Priority binarycheck failed:\nOperationalError: 1969 (70100): '
                'Query execution was interrupted (max_statement_time exceeded)')):
            self.assertIsNotNone(DataBaseCore.statement_timeout_message(error))

    def test_killed_connection_and_lock_errors_are_not_statement_timeouts(self):
        for code in (1205, 1213, 1317, 2013):
            error = RuntimeError('interrupted')
            error.errno = code
            self.assertIsNone(DataBaseCore.statement_timeout_message(error))

    def test_statement_timeout_is_not_retried_as_a_lock_error(self):
        error = RuntimeError('statement timeout')
        error.errno = 1969
        operation = mock.Mock(side_effect=error)
        with mock.patch.object(app.time, 'sleep') as sleep, self.assertRaises(RuntimeError):
            app.database_connection._retry_on_lock_error(operation)
        operation.assert_called_once()
        sleep.assert_not_called()


class TestConnectionDeadlines(unittest.TestCase):
    def test_regular_server_and_control_connections_pass_deadlines_to_both_drivers(self):
        database = object.__new__(DataBaseCore.DataBaseConnectionCore)
        database.host, database.port, database.database = 'db.example', 5799, 'testdb'
        database.user, database.password = 'test', 'test'
        for driver in ('MySQL Connector/Python', 'MariaDB Connector/Python'):
            with mock.patch.object(DataBaseCore, 'DATABASE_DRIVER', driver), \
                 mock.patch.object(DataBaseCore, '_database_connect') as connect:
                database.connect()
                database.connect_server()
                database._control_connection()
                self.assertEqual([call.kwargs['connect_timeout'] for call in connect.call_args_list], [10, 10, 5])
                self.assertTrue(all('connection_timeout' not in call.kwargs for call in connect.call_args_list))


class TestZeroContentDirectoryTrees(unittest.TestCase):
    def test_zero_byte_files_and_nested_directories_are_excluded(self):
        rows = [(1,0,0,2,1,0,2,0,0), (2,1,0,1,1,0,1,0,0), (3,2,0,0,0,0,0,0,0),
                (4,0,0,0,1,0,0,0,0), (5,4,0,1,0,0,0,0,1)]
        self.assertEqual(DataBaseCore.DataBaseConnectionCore._zero_content_tree_ids(rows), {(0,1),(0,2),(0,3)})

    def test_unknown_missing_unreadable_special_and_cyclic_nodes_are_kept(self):
        rows = [(1,0,0,0,1,0,0,0,0), (2,1,0,None,0,0,None,0,0),
                (3,0,0,0,1,0,0,0,0), (4,0,0,0,0,1,0,0,0),
                (5,6,0,0,1,0,0,0,0), (6,5,0,0,1,0,0,0,0),
                (7,0,0,0,0,0,0,1,0), (8,0,0,1,0,0,None,0,0),
                (9,0,0,1,0,0,1,None,0)]
        self.assertEqual(DataBaseCore.DataBaseConnectionCore._zero_content_tree_ids(rows), set())

    def test_special_or_nonzero_descendant_keeps_all_ancestors(self):
        rows = [(1,0,0,1,1,0,1,0,0), (2,1,0,0,0,0,0,1,0),
                (3,0,0,0,1,0,0,0,0), (4,3,0,2,0,0,1,0,10)]
        self.assertEqual(DataBaseCore.DataBaseConnectionCore._zero_content_tree_ids(rows), set())

    def test_parent_links_are_scoped_to_host(self):
        rows = [(1,0,0,0,1,0,0,0,0), (2,1,0,1,0,0,1,0,0),
                (1,0,1,0,1,0,0,0,0), (2,1,1,1,0,0,0,0,1)]
        self.assertEqual(DataBaseCore.DataBaseConnectionCore._zero_content_tree_ids(rows), {(0,1),(0,2)})

    def test_long_chain_is_iterative(self):
        rows = [(i,i-1,0,1,int(i<5000),0,1,0,0) for i in range(1,5001)]
        self.assertEqual(len(DataBaseCore.DataBaseConnectionCore._zero_content_tree_ids(rows)), 5000)


class TestPersistedZeroContentFlag(unittest.TestCase):
    def database(self, rows):
        database = object.__new__(app.DataBaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchall.return_value = rows
        database.connect = mock.Mock(return_value=connection)
        return database, connection, connection.cursor.return_value

    def test_reporting_id_lookup_reads_only_flagged_ids_without_paths(self):
        database, _, cursor = self.database([(1,7),(1,8)])
        self.assertEqual(database.zero_content_directory_ids(hostid=1), {(1,7),(1,8)})
        sql, params = cursor.execute.call_args.args
        self.assertIn('SELECT hostid, dirid', sql)
        self.assertIn('zero_content_only = TRUE', sql)
        self.assertNotIn('path', sql)
        self.assertEqual(params, (1,))

    def test_backfill_updates_changed_flags_in_short_batches_and_clears_uncertain_flags(self):
        rows = [(i,0,1,1,0,0,1,0,0,0) for i in range(1,402)]
        rows.append((402,0,1,1,0,0,None,0,0,1))
        database, connection, cursor = self.database(rows)
        self.assertEqual(database.refresh_zero_content_flags(hostid=1), 402)
        calls = cursor.executemany.call_args_list
        self.assertEqual([len(call.args[1]) for call in calls], [200,200,2])
        self.assertEqual(calls[-1].args[1][-1], (0,1,402))
        self.assertEqual(connection.commit.call_count, 4)

    def test_scan_invalidation_is_host_scoped_and_committed(self):
        database, connection, cursor = self.database([])
        database.invalidate_zero_content_flags(1)
        sql, params = cursor.execute.call_args.args
        self.assertIn('SET zero_content_only = FALSE', sql)
        self.assertIn('WHERE hostid = %s', sql)
        self.assertEqual(params, (1,))
        connection.commit.assert_called_once()

    def test_completed_subtree_rebuild_refreshes_flags_after_connections_close(self):
        database = object.__new__(app.DataBaseConnection)
        reader, writer = mock.MagicMock(), mock.MagicMock()
        reader.cursor.return_value.fetchone.return_value = (1,)
        reader.cursor.return_value.fetchmany.side_effect = [[(1,0,0,1,'root',0,None,0,0)], []]
        events = mock.Mock()
        with mock.patch.object(database, 'connect', side_effect=[reader,writer]), \
             mock.patch.object(database, 'refresh_zero_content_flags') as refresh:
            events.attach_mock(reader.close, 'reader_closed')
            events.attach_mock(writer.close, 'writer_closed')
            events.attach_mock(refresh, 'refresh')
            database.rebuild_subtree_statistics('/root', hostid=1)
        self.assertEqual(events.mock_calls[-3:], [mock.call.reader_closed(), mock.call.writer_closed(), mock.call.refresh(hostid=1)])

    def test_failed_subtree_rebuild_does_not_refresh_exclusion_flags(self):
        database = object.__new__(app.DataBaseConnection)
        reader, writer = mock.MagicMock(), mock.MagicMock()
        reader.cursor.return_value.fetchone.return_value = (1,)
        reader.cursor.return_value.fetchmany.side_effect = [[(1,0,0,1,'root',0,None,0,0)], []]
        writer.cursor.return_value.execute.side_effect = RuntimeError('write failed')
        with mock.patch.object(database, 'connect', side_effect=[reader,writer]), \
             mock.patch.object(database, 'refresh_zero_content_flags') as refresh, \
             self.assertRaisesRegex(RuntimeError, 'write failed'):
            database.rebuild_subtree_statistics('/root', hostid=1)
        refresh.assert_not_called()
        writer.rollback.assert_called_once()


class TestPriorityProcessIdentity_DataBaseCore(unittest.TestCase):
    def database(self, rows):
        db = object.__new__(app.DataBaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchall.return_value = rows
        return db, connection, connection.cursor.return_value

    def test_ps_start_token_and_zombie_detection(self):
        result = mock.Mock(returncode=0, stdout='S Thu Oct 29 10:00:00 2026\n')
        with mock.patch.object(app.Path, 'exists', return_value=False), \
             mock.patch.object(DataBaseCore.subprocess, 'run', return_value=result):
            self.assertEqual(app._process_start_token(1234), 'ps:Thu Oct 29 10:00:00 2026')
            result.stdout = 'Z Thu Oct 29 10:00:00 2026\n'
            self.assertIsNone(app._process_start_token(1234))
            result.returncode, result.stdout = 1, ''
            self.assertIsNone(app._process_start_token(1234))
            result.returncode = 2
            with self.assertRaises(RuntimeError):
                app._process_start_token(1234)

    def test_linux_token_handles_spaces_in_comm(self):
        # After the comm field, starttime (field 22) is element 19.
        stat = '1234 (worker (priority)) S ' + '0 ' * 18 + '987654 0'
        with mock.patch.object(app.Path, 'exists', return_value=True), \
             mock.patch.object(app.Path, 'read_text', side_effect=[stat, 'boot-id']):
            self.assertEqual(app._process_start_token(1234), 'proc:boot-id:987654')


class ScanConnectionRecoveryTests_DataBaseCore(unittest.TestCase):
    def test_tagged_connection_cleanup_preserves_original_error(self):
        connection = mock.Mock()
        connection.close.side_effect = RuntimeError('close failed')
        lost = app.Error('connection reset')
        lost.errno = 2055
        with self.assertRaises(app.Error) as raised:
            with DataBaseCore._TaggedConnection(connection, 'tag'):
                raise lost
        self.assertIs(raised.exception, lost)


class NetworkRecoveryTests(unittest.TestCase):
    def database(self):
        db = object.__new__(app.DataBaseConnection)
        db.host, db.port, db.database = 'test', 3306, 'test'
        db.user, db.password = 'test', 'test'
        return db

    def test_each_retry_builds_a_new_socket_with_bounded_io(self):
        db = self.database()
        a, peer_a = socket.socketpair()
        b, peer_b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        self.addCleanup(peer_a.close)
        self.addCleanup(peer_b.close)
        handles = [SimpleNamespace(_socket=SimpleNamespace(sock=s)) for s in (a, b)]
        with mock.patch.object(core, '_database_connect', side_effect=handles) as connect, \
                mock.patch.object(core, 'DATABASE_DRIVER', 'MySQL Connector/Python'), \
                mock.patch.object(core, 'DB_IO_TIMEOUT', 0.02):
            first, second = db.connect(), db.connect()
            self.assertIsNot(first, second)
            self.assertEqual(connect.call_count, 2)
            self.assertTrue(all(call.kwargs['use_pure'] for call in connect.call_args_list))
            self.assertEqual(a.gettimeout(), 0.02)
            self.assertEqual(b.gettimeout(), 0.02)
            # A silent peer cannot leave the worker blocked indefinitely.
            with self.assertRaises(socket.timeout):
                b.recv(1)

    def test_first_wake_tick_discards_existing_sockets_and_signals_poll(self):
        db = self.database()
        transport, peer = socket.socketpair()
        self.addCleanup(transport.close)
        self.addCleanup(peer.close)
        db._track_socket(transport)
        wake = threading.Event()
        stop = mock.Mock()
        stop.wait.side_effect = [False, False, True]
        with mock.patch.object(core.threading, 'Event', side_effect=[wake, stop]), \
                mock.patch.object(core.threading, 'Thread') as thread, \
                mock.patch.object(core.time, 'time', side_effect=[100, 100.5, 220.5]):
            callback = mock.Mock()
            db.start_wake_monitor(callback)
            thread.call_args.kwargs['target']()
        callback.assert_called_once_with(120)
        self.assertEqual(peer.recv(1), b'')
        self.assertTrue(db.consume_wake_event())
        self.assertFalse(db.consume_wake_event())

    def run_directory_watchdog(self, counts, times, expected, alive):
        operation = app.server_operations(SimpleNamespace(mode='localserver'))
        operation.database = mock.Mock()
        operation.database.get_job_status.return_value = 'active'
        operation.database.get_job_idle_seconds.return_value = 3601
        operation.database.count_unfilled_detailed_directories.side_effect = counts
        operation.database.get_detailed_directory_progress.return_value = (100, 50)
        operation._logger = mock.Mock()
        context = mock.Mock()
        context.RawArray.return_value = SimpleNamespace(value=b'')
        process = context.Process.return_value
        process.is_alive.side_effect = alive
        process.exitcode = 0
        with mock.patch.object(server, '_runtime', return_value=app), \
                mock.patch.object(app, '_shutdown', False), \
                mock.patch.object(server.multiprocessing, 'get_context', return_value=context), \
                mock.patch.object(server.time, 'monotonic', side_effect=times), \
                mock.patch.object(operation, '_force_stop_child') as stop, \
                mock.patch.object(operation, '_network_warning'):
            self.assertEqual(operation.run_spawn('detailed', '/root', 18, hostid=1), expected)
        for call in operation.database.count_unfilled_detailed_directories.call_args_list:
            self.assertEqual(call, mock.call(1, '/root'))
        return stop


class DuplicateTimeoutTests_DataBaseCore(unittest.TestCase):
    def test_transport_uses_triple_custom_io_and_connect_timeouts(self):
        database = object.__new__(app.DataBaseConnection)
        transport = mock.Mock()
        handle = SimpleNamespace(_socket=SimpleNamespace(sock=transport))
        with mock.patch.object(core, '_database_connect', return_value=handle) as connect, \
                mock.patch.object(core, 'DATABASE_DRIVER', 'MySQL Connector/Python'), \
                mock.patch.object(core, 'DB_IO_TIMEOUT', 40), \
                mock.patch.object(database, '_track_socket'):
            database._open_connection(dict(connect_timeout=10), timeout_multiplier=3)
        self.assertEqual(connect.call_args.kwargs['connect_timeout'], 30)
        transport.settimeout.assert_called_once_with(120)


class ContentChecksumTests(unittest.TestCase):
    def trees(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        left, right = Path(folder.name) / 'left', Path(folder.name) / 'right'
        for root in (left, right):
            (root / 'empty').mkdir(parents=True)
            (root / 'nested').mkdir()
            (root / 'nested' / 'content').write_bytes(b'hello\0world')
            (root / 'zero').touch()
        return left, right

    def test_equal_contents_with_different_root_names_and_mtimes(self):
        left, right = self.trees()
        os.utime(right / 'nested' / 'content', (1, 1))
        self.assertEqual(core.directory_content_checksum(left), core.directory_content_checksum(right))

    def test_same_size_byte_changes_and_names_change_digest(self):
        left, right = self.trees()
        (right / 'nested' / 'content').write_bytes(b'HELLO\0WORLD')
        self.assertNotEqual(core.directory_content_checksum(left), core.directory_content_checksum(right))
        (right / 'nested' / 'content').write_bytes(b'hello\0world')
        (right / 'empty').rename(right / 'different-empty-name')
        self.assertNotEqual(core.directory_content_checksum(left), core.directory_content_checksum(right))

    def test_symlinks_are_not_followed_and_targets_matter(self):
        left, right = self.trees()
        (left / 'link').symlink_to('/not/readable/by/this/process')
        (right / 'link').symlink_to('/not/readable/by/this/process')
        self.assertEqual(core.directory_content_checksum(left), core.directory_content_checksum(right))
        (right / 'link').unlink()
        (right / 'link').symlink_to('/another/target')
        self.assertNotEqual(core.directory_content_checksum(left), core.directory_content_checksum(right))

    def test_read_error_produces_no_checksum(self):
        left, _ = self.trees()
        with mock.patch.object(core.os, 'open', side_effect=PermissionError('denied')):
            with self.assertRaises(PermissionError):
                core.directory_content_checksum(left)

    def test_detects_tree_changes_during_checksum(self):
        left, _ = self.trees()
        def change():
            if not (left / 'added').exists():
                (left / 'added').touch()
        # Delay the mutation until directory lstat has captured its initial identity.
        original = core.os.scandir
        def scandir(path):
            result = original(path)
            if Path(path) == left:
                change()
            return result
        with mock.patch.object(core.os, 'scandir', side_effect=scandir):
            with self.assertRaisesRegex(OSError, 'Directory changed'):
                core.directory_content_checksum(left)


class RemoteCoordinatorTests(unittest.TestCase):
    def database(self, local, remote):
        db = object.__new__(app.DataBaseConnection)
        db.binarycheck_exclusion_matcher = mock.Mock(return_value=lambda path: False)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchall.return_value = [(1, 0, 20, 10, str(local))]
        db.connect = mock.Mock(return_value=connection)
        db.register_remote_binarycheck_run = mock.Mock(return_value=101)
        db._remote_heartbeat = mock.Mock()
        db._remote_request = mock.Mock()
        db._remote_response = mock.Mock(return_value=('done', core.directory_content_checksum(remote), None, 'sha256-tree-v2'))
        db._save_remote_result = mock.Mock()
        return db

    def test_equal_and_different_are_decided_from_actual_content_checksums(self):
        with tempfile.TemporaryDirectory() as folder:
            local, remote = Path(folder) / 'local', Path(folder) / 'remote'
            local.mkdir(); remote.mkdir()
            (local / 'file').write_bytes(b'local bytes')
            for contents, expected in ((b'local bytes', 'equal'), (b'other bytes', 'different')):
                (remote / 'file').write_bytes(contents)
                db = self.database(local, remote)
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertTrue(db.run_remote_binarycheck(7, hostid=1))
                db.register_remote_binarycheck_run.assert_called_once_with(7, 1, 0)
                db._remote_request.assert_called_once_with(7, 101, 0, 10)
                self.assertEqual(db._save_remote_result.call_args.args[:7], (7, 101, 1, 0, 20, 10, expected))

    def test_remote_errors_and_invalid_checksum_never_confirm_equality(self):
        with tempfile.TemporaryDirectory() as folder:
            for response in [('error', None, 'permission denied', 'sha256-tree-v2'),
                             ('done', 'bad', None, 'sha256-tree-v2'),
                             ('done', 'a' * 64, None, 'metadata-only')]:
                db = self.database(folder, folder)
                db._remote_response.return_value = response
                with contextlib.redirect_stdout(io.StringIO()):
                    db.run_remote_binarycheck(7, hostid=1)
                self.assertEqual(db._save_remote_result.call_args.args[6], 'unreadable')

    def test_one_host_job_can_advertise_multiple_remote_hosts_in_sequence(self):
        with tempfile.TemporaryDirectory() as folder:
            db = self.database(folder, folder)
            db.connect.return_value.cursor.return_value.fetchall.return_value = [
                (1, 0, 20, 10, folder), (1, 2, 20, 30, folder)]
            db.register_remote_binarycheck_run.side_effect = [101, 102]
            with contextlib.redirect_stdout(io.StringIO()):
                db.run_remote_binarycheck(7, hostid=1)
            self.assertEqual(db.register_remote_binarycheck_run.call_args_list,
                             [mock.call(7, 1, 0), mock.call(7, 1, 2)])
            self.assertEqual(db._remote_request.call_args_list,
                             [mock.call(7, 101, 0, 10), mock.call(7, 102, 2, 30)])

    def test_wrong_local_host_cannot_lead(self):
        with tempfile.TemporaryDirectory() as folder:
            db = self.database(folder, folder)
            with self.assertRaisesRegex(RuntimeError, 'localserver'):
                db.run_remote_binarycheck(7, hostid=0)
            db._remote_request.assert_not_called()

    def test_stopped_or_superseded_job_cannot_publish_equality(self):
        with tempfile.TemporaryDirectory() as folder:
            db = self.database(folder, folder)
            db._remote_heartbeat.side_effect = InterruptedError('superseded')
            with self.assertRaises(InterruptedError):
                db.run_remote_binarycheck(7, hostid=1)
            db._save_remote_result.assert_not_called()


class RemotePersistenceTests(unittest.TestCase):
    def database(self):
        db = object.__new__(app.DataBaseConnection)
        db.binarycheck_exclusion_matcher = mock.Mock(return_value=lambda path: False)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        db.connect = mock.Mock(return_value=connection)
        return db, connection, connection.cursor.return_value

    def test_listener_claims_only_current_host_requests_and_publishes_by_token(self):
        db, connection, cursor = self.database()
        with tempfile.TemporaryDirectory() as folder:
            cursor.fetchone.return_value = (101, 7, 10, folder)
            cursor.rowcount = 1
            with mock.patch.object(core.uuid, 'uuid4', return_value=SimpleNamespace(hex='token')):
                self.assertTrue(db.serve_remote_checksum(0, listener_runid=202))
        calls = cursor.execute.call_args_list
        select = calls[0].args[0]
        self.assertIn('r.remotehost = q.hostid', select)
        self.assertIn('j.runid = q.runid', select)
        self.assertIn('p.hostidB = q.hostid', select)
        self.assertIn('p.right_dirid = q.dirid', select)
        self.assertEqual(calls[0].args[1], (0,))
        self.assertIn('listener_runid = %s', calls[1].args[0])
        self.assertEqual(calls[1].args[1][:2], ('token', 202))
        self.assertIn('q.claim_token = %s', calls[-1].args[0])
        self.assertIn('j.runid = q.runid', calls[-1].args[0])
        self.assertEqual(calls[-1].args[1][0], 'done')
        self.assertEqual(len(calls[-1].args[1][1]), 64)

    def test_listener_read_failure_publishes_error_with_no_checksum(self):
        db, _, cursor = self.database()
        cursor.fetchone.return_value = (101, 7, 10, None)
        db.serve_remote_checksum(0, listener_runid=202)
        params = cursor.execute.call_args.args[1]
        self.assertEqual(params[0], 'error')
        self.assertIsNone(params[1])

    def test_reassigned_lease_cannot_publish_old_result(self):
        db, _, cursor = self.database()
        with tempfile.TemporaryDirectory() as folder:
            cursor.fetchone.return_value = (101, 7, 10, folder)
            cursor.rowcount = 0
            self.assertTrue(db.serve_remote_checksum(0, listener_runid=202))
        self.assertFalse(any('SET q.status = %s' in call.args[0] for call in cursor.execute.call_args_list))

    def test_one_listener_run_records_pid_without_overwriting_job_run(self):
        db, _, cursor = self.database()
        db._acquire_runid = mock.Mock(return_value=('allocator', 202))
        db.write_run_record = mock.Mock()
        cursor.fetchone.side_effect = [(7, 1), (1,)]
        self.assertEqual(db.register_remote_listener_run(0), 202)
        db.write_run_record.assert_called_once_with(cursor, 202, 7, 'binarycheck', 'active', 'listener', remotehost=1)
        self.assertFalse(any('UPDATE jobs ' in call.args[0] for call in cursor.execute.call_args_list))

    def test_remote_advertisement_is_a_new_run_and_updates_only_own_job(self):
        db, _, cursor = self.database()
        db._acquire_runid = mock.Mock(return_value=('allocator', 101))
        db.write_run_record = mock.Mock()
        cursor.fetchone.side_effect = [(100,), (1,)]
        self.assertEqual(db.register_remote_binarycheck_run(7, 1, 0), 101)
        db.write_run_record.assert_called_once_with(cursor, 101, 7, 'binarycheck', 'active', 'listening', remotehost=0)
        update = next(call for call in cursor.execute.call_args_list if 'UPDATE jobs ' in call.args[0])
        self.assertEqual(update.args[1][0], 101)
        self.assertEqual(update.args[1][-2:], (7, 1))


class MissingRemoteSchemaTests(unittest.TestCase):
    def test_missing_remotehost_preserves_original_database_error(self):
        db = object.__new__(app.DataBaseConnection)
        db.binarycheck_exclusion_matcher = mock.Mock(return_value=lambda path: False)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.execute.side_effect = app.Error(
            msg="Unknown column 'r.remotehost' in 'WHERE'", errno=1054)
        with mock.patch.object(db, 'connect', return_value=connection):
            with self.assertRaisesRegex(app.Error, "Unknown column 'r.remotehost'") as raised:
                db.remote_binarycheck_runs(0)
        self.assertIs(raised.exception, connection.cursor.return_value.execute.side_effect)
        connection.cursor.return_value.close.assert_called_once()


class _ListenerDatabase(app.DataBaseConnection):
    def __init__(self, socket_path, database):
        self.socket_path = socket_path
        self.database = database

    def connect(self, **kwargs):
        return mysql.connector.connect(unix_socket=self.socket_path, user='root', use_pure=True,
                                       database=self.database)


def _listener_process(socket_path, database):
    app.database_connection = _ListenerDatabase(socket_path, database)
    server.server_operations._remote_listener_entry(0)


@unittest.skipUnless(os.environ.get('DIRWALKER_LOCAL_DB_INTEGRATION') == '1', 'isolated MariaDB integration is opt-in')
class RemoteMariaDBTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.folder = tempfile.TemporaryDirectory(prefix='dirwalker-remote-db-')
        cls.addClassCleanup(cls.folder.cleanup)
        root = Path(cls.folder.name)
        cls.socket_path = str(root / 'mysql.sock')
        cls.log = open(str(root / 'server.log'), 'w+')
        cls.addClassCleanup(cls.log.close)
        installer = shutil.which('mariadb-install-db')
        server = shutil.which('mariadbd')
        if not installer or not server:
            raise unittest.SkipTest('local MariaDB server binaries unavailable')
        result = subprocess.run([installer, '--no-defaults', '--datadir=' + str(root / 'data'),
                                 '--auth-root-authentication-method=normal', '--skip-test-db'],
                                stdout=cls.log, stderr=subprocess.STDOUT, timeout=60)
        if result.returncode:
            cls.log.seek(0)
            raise RuntimeError(cls.log.read())
        cls.process = subprocess.Popen([server, '--no-defaults', '--datadir=' + str(root / 'data'),
                                        '--socket=' + cls.socket_path, '--skip-networking',
                                        '--pid-file=' + str(root / 'server.pid')],
                                       stdout=cls.log, stderr=subprocess.STDOUT)
        cls.addClassCleanup(cls.stop_server)
        deadline = time.monotonic() + 30
        while True:
            try:
                connection = mysql.connector.connect(unix_socket=cls.socket_path, user='root', use_pure=True)
                connection.close()
                break
            except mysql.connector.Error:
                if cls.process.poll() is not None or time.monotonic() > deadline:
                    cls.log.seek(0)
                    raise RuntimeError(cls.log.read())
                time.sleep(0.1)

    @classmethod
    def stop_server(cls):
        if cls.process.poll() is None:
            cls.process.terminate()
            try:
                cls.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                cls.process.kill()
                cls.process.wait(timeout=5)

    def test_cross_host_checksum_workflow_and_one_listener_run_for_all_requests(self):
        db = object.__new__(app.DataBaseConnection)
        db.database = 'remote_test'
        def connect(database=True, **kwargs):
            return mysql.connector.connect(unix_socket=self.socket_path, user='root', use_pure=True,
                                           **({'database': db.database} if database else {}))
        db.connect = connect
        db.connect_server = lambda: connect(database=False)
        cli = DataBaseCLI(db)
        with contextlib.redirect_stdout(io.StringIO()):
            cli.init_database()
            cli.init_alldirs(reset=False)
            cli.create_hosts_table()
        with tempfile.TemporaryDirectory() as folder:
            local, remote = Path(folder) / 'local', Path(folder) / 'remote'
            for path in (local, remote):
                path.mkdir()
                (path / 'file').write_bytes(b'contents')
            (remote / '.DS_Store').write_bytes(b'NAS cache')
            (local / '.ds_STORE').write_bytes(b'other computer cache')
            (remote / '@eaDir').mkdir()
            (remote / '@eaDir' / 'index').write_bytes(b'NAS thumbnail')
            with connect() as connection:
                cursor = connection.cursor()
                cursor.executemany('INSERT INTO excluded_names (name) VALUES (%s)', [('.DS_store',), ('@EADIR',)])
                cursor.execute("INSERT INTO hosts (hostid, `user`, homepath, `system`, machine, tabellenname, timecreated) "
                               "VALUES (1,'test','/test','test',%s,'tmp1_job_work',UTC_TIMESTAMP(6))", (socket.gethostname(),))
                for hostid, path in ((0, remote), (1, local)):
                    cursor.execute("INSERT INTO directories (hostid,path_hash,root_path,path,name,parentdir,"
                                   "combined_sha256,subtree_structure,zero_content_only) "
                                   "VALUES (%s,%s,%s,%s,'root',0,%s,'0-1',FALSE)",
                                   (hostid, app._path_hash(str(path)), str(path), str(path), 'a' * 64))
                connection.commit()
            listener_runid = None
            listener = None
            with mock.patch.object(core, '_process_start_token', return_value='integration-process'):
                for case, expected in (('equal', 'equal'), ('changed', 'different'), ('missing', 'unreadable')):
                    if case == 'changed':
                        (remote / 'file').write_bytes(b'changed!')
                    if case == 'missing':
                        shutil.rmtree(str(remote))
                    created = cli.create_binarycheck_job()
                    self.assertEqual([item[1] for item in created], [0, 1])
                    jobid, count = created[1]
                    job = db.claim_job('insert', True, 1, socket.gethostname())
                    self.assertEqual(job['jobid'], jobid)
                    errors = []
                    def coordinate():
                        try:
                            db.run_binarycheck_job(jobid, hostid=1)
                        except BaseException as exc:
                            errors.append(exc)
                    worker = threading.Thread(target=coordinate)
                    worker.start()
                    deadline = time.monotonic() + 10
                    try:
                        while worker.is_alive():
                            runs = db.remote_binarycheck_runs(0)
                            if runs and listener is None:
                                listener = multiprocessing.get_context('spawn').Process(
                                    target=_listener_process, args=(self.socket_path, db.database))
                                listener.start()
                                self.addCleanup(self.stop_listener, listener)
                            if time.monotonic() > deadline:
                                self.fail('coordinator did not finish')
                            time.sleep(0.02)
                    finally:
                        worker.join(timeout=2)
                    self.assertFalse(errors, errors)
                    with connect() as connection:
                        cursor = connection.cursor()
                        cursor.execute('SELECT hostid,hostidB,status FROM tmp_doubles_letchecked WHERE jobid=%s', (jobid,))
                        self.assertEqual(cursor.fetchone(), (1, 0, expected))
                        cursor.execute('SELECT listener_runid,status FROM tmp_remotedoubles WHERE jobid=%s', (jobid,))
                        result = cursor.fetchone()
                        if listener_runid is None:
                            listener_runid = result[0]
                            self.assertIsNotNone(listener_runid)
                        self.assertEqual(result, (listener_runid, 'error' if case == 'missing' else 'done'))
                    db.set_job_status(jobid, 'closed', hostid=1)
                self.stop_listener(listener)
                with connect() as connection:
                    cursor = connection.cursor()
                    cursor.execute("SELECT COUNT(*) FROM jobsrunid WHERE stage='listener'")
                    self.assertEqual(cursor.fetchone()[0], 1)
                    cursor.execute("SELECT COUNT(*) FROM jobsrunid WHERE stage='listening' AND remotehost=0")
                    self.assertEqual(cursor.fetchone()[0], 3)
                    cursor.execute("SELECT status FROM jobsrunid WHERE runid=%s", (listener_runid,))
                    self.assertEqual(cursor.fetchone()[0], 'closed')

    @staticmethod
    def stop_listener(listener):
        if listener is None:
            return
        if listener.is_alive():
            listener.terminate()
        listener.join(timeout=5)
        if listener.is_alive():
            listener.kill()
            listener.join(timeout=2)

    def test_migration_backfills_existing_pairs_and_allows_cross_host_direction(self):
        root = mysql.connector.connect(unix_socket=self.socket_path, user='root', use_pure=True)
        cursor = root.cursor()
        cursor.execute('CREATE DATABASE remote_migration_test')
        cursor.execute('USE remote_migration_test')
        cursor.execute('CREATE TABLE jobs (jobid BIGINT UNSIGNED PRIMARY KEY) ENGINE=InnoDB')
        cursor.execute('CREATE TABLE jobsrunid (runid BIGINT UNSIGNED PRIMARY KEY) ENGINE=InnoDB')
        cursor.execute('CREATE TABLE tmp_doubles_letchecked (jobid BIGINT UNSIGNED, hostid SMALLINT, '
                       'left_dirid BIGINT UNSIGNED, right_dirid BIGINT UNSIGNED, '
                       'PRIMARY KEY(jobid,hostid,left_dirid,right_dirid), '
                       'CONSTRAINT chk_double_jobs_pair CHECK(left_dirid < right_dirid)) ENGINE=InnoDB')
        cursor.execute('INSERT INTO jobs VALUES(7)')
        cursor.execute('INSERT INTO tmp_doubles_letchecked VALUES(7,1,10,20)')
        root.commit()
        migration = (Path(__file__).parent / 'sql' / 'migrate_remote_binarycheck.sql').read_text()
        for statement in migration.split(';'):
            if statement.strip():
                cursor.execute(statement)
        cursor.execute('SELECT hostidB FROM tmp_doubles_letchecked')
        self.assertEqual(cursor.fetchone()[0], 1)
        cursor.execute('INSERT INTO tmp_doubles_letchecked VALUES(7,1,0,20,10)')
        cursor.execute("SELECT COUNT(*) FROM information_schema.columns "
                       "WHERE table_schema='remote_migration_test' AND table_name='tmp_remotedoubles' "
                       "AND column_name='listener_runid'")
        self.assertEqual(cursor.fetchone()[0], 1)
        cursor.close()
        root.close()


class ConsistentExclusionTests(unittest.TestCase):
    def trees(self, folder):
        left, right = Path(folder) / 'NAS', Path(folder) / 'Computer'
        for root in (left, right):
            root.mkdir()
            (root / 'data.zip').write_bytes(b'same real contents')
            (root / 'chapter').mkdir()
            (root / 'chapter' / 'notes').write_bytes(b'notes')
        (left / '.DS_Store').write_bytes(b'NAS metadata')
        (right / '.ds_STORE').write_bytes(b'different computer metadata')
        (left / '@eaDir').mkdir()
        (left / '@eaDir' / 'cache').write_bytes(b'NAS cache')
        (right / 'chapter' / '@EADIR').mkdir()
        (right / 'chapter' / '@EADIR' / 'cache').write_bytes(b'other cache')
        os.utime(left / 'data.zip', ns=(1748785448292992900, 1748785448292992900))
        os.utime(right / 'data.zip', ns=(1748785448000000000, 1748785448000000000))
        return left, right

    def test_names_match_case_insensitively_at_any_depth(self):
        excluded = app._directory_exclusion_matcher('.DS_store,@eaDir,.git')
        for path in ('/root/.DS_Store', '/root/@EADIR/file', '/root/a/.Git/config'):
            self.assertTrue(excluded(path), path)
        self.assertFalse(excluded('/root/@eaDirectory/file'))
        self.assertFalse(excluded('/root/file.DS_Store'))


class TestInventoryHostids(unittest.TestCase):
    def test_reads_used_hosts_including_zero_and_closes_cursor(self):
        db = object.__new__(app.DatabaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchall.return_value = [(0,), (2,), (7,)]
        with mock.patch.object(db, 'connect', return_value=connection):
            self.assertEqual(db.list_directory_hostids(include_jobs=False), [0, 2, 7])
        cursor.execute.assert_called_once_with('SELECT DISTINCT hostid FROM directories ORDER BY hostid')
        cursor.close.assert_called_once_with()

    def test_job_dataset_groups_used_runs_and_preserves_end_times_and_orphans(self):
        import sqlite3
        sql = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
        self.addCleanup(sql.close)
        sql.executescript('''
            CREATE TABLE directories (hostid INTEGER, runid INTEGER, root_path TEXT);
            CREATE TABLE jobsrunid (runid INTEGER, jobid INTEGER);
            CREATE TABLE jobs (jobid INTEGER, hostid INTEGER, timeend TIMESTAMP);
        ''')
        ended = datetime(2026, 11, 4, 12, 30)
        sql.executemany('INSERT INTO directories VALUES (?,?,?)',
                        [(0,100,'/a'), (0,100,'/a'), (0,101,'/b'),
                         (2,200,'/c'), (2,300,'/d'), (5,400,'/e'), (7,999,'/f')])
        sql.executemany('INSERT INTO jobsrunid VALUES (?,?)',
                        [(100,10), (101,10), (200,20), (300,30), (400,40)])
        sql.executemany('INSERT INTO jobs VALUES (?,?,?)', [(10,0,ended), (20,2,None), (30,2,ended)])
        db = object.__new__(app.DatabaseConnection)
        result = db.list_directory_hostids(cursor=sql.cursor())
        self.assertEqual(result, [
            dict(hostid=0, jobid=10, runids=[100,101], timeend=ended, directory_count=3, root_paths=['/a','/b']),
            dict(hostid=2, jobid=20, runids=[200], timeend=None, directory_count=1, root_paths=['/c']),
            dict(hostid=2, jobid=30, runids=[300], timeend=ended, directory_count=1, root_paths=['/d']),
            dict(hostid=5, jobid=40, runids=[400], timeend=None, directory_count=1, root_paths=['/e']),
            dict(hostid=7, jobid=None, runids=[999], timeend=None, directory_count=1, root_paths=['/f']),
        ])
        self.assertEqual(db.list_directory_hostids(cursor=sql.cursor(), include_jobs=False), [0,2,5,7])

    def test_default_dataset_uses_fresh_connection_and_closes_cursor(self):
        db = object.__new__(app.DatabaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchall.return_value = [(0, 10, 100, None, '/root', 2)]
        with mock.patch.object(db, 'connect', return_value=connection):
            self.assertEqual(db.list_directory_hostids(),
                             [dict(hostid=0, jobid=10, runids=[100], timeend=None, directory_count=2, root_paths=['/root'])])
        cursor.close.assert_called_once_with()

    def test_core_sorts_by_total_count_across_runs_not_host_order(self):
        db = object.__new__(app.DatabaseConnection)
        cursor = mock.Mock()
        cursor.fetchall.return_value = [
            (0, 10, 100, None, '/small', 2),
            (7, 70, 700, None, '/large/a', 2),
            (7, 70, 701, None, '/large/b', 3),
            (2, 20, 200, None, '/medium', 4),
        ]
        rows = db.list_directory_hostids(cursor=cursor)
        self.assertEqual([row['hostid'] for row in rows], [7, 2, 0])
        self.assertEqual([row['directory_count'] for row in rows], [5, 4, 2])
        self.assertEqual(rows[0]['root_paths'], ['/large/a', '/large/b'])

    def test_existing_cursor_and_empty_inventory(self):
        db = object.__new__(app.DatabaseConnection)
        cursor = mock.Mock()
        cursor.fetchall.return_value = []
        with mock.patch.object(db, 'connect') as connect:
            self.assertEqual(db.list_directory_hostids(cursor=cursor), [])
        connect.assert_not_called()
        cursor.close.assert_not_called()


# age_rescan regression coverage.


class TestAgeRescan(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.sql = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
        self.addCleanup(self.sql.close)
        for table in ('jobs', 'jobsrunid', 'hosts', 'directories', 'tmp_modified', 'excluded_names'):
            shape = SQLStructure.EXPECTED_TABLE_SHAPES[table]
            fields = []
            for name, (kind, nullable, default, coll) in shape['columns'].items():
                typ = 'TIMESTAMP' if 'datetime' in kind else 'INTEGER' if 'int' in kind else 'TEXT'
                if shape['primary'] == (name,):
                    typ += ' PRIMARY KEY'
                if default is not None:
                    typ += " DEFAULT '" + str(default).replace("'", "''") + "'"
                fields.append(name + ' ' + typ)
            if len(shape['primary']) > 1:
                fields.append('PRIMARY KEY (' + ','.join(shape['primary']) + ')')
            self.sql.execute('CREATE TABLE ' + table + ' (' + ','.join(fields) + ')')
        self.now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.sql.executemany('INSERT INTO hosts (hostid,machine) VALUES (?,?)', [(2,'laptop'),(3,'desktop')])
        self.add_job(10, 2, self.now-timedelta(days=30))
        self.sql.execute('INSERT INTO jobsrunid (runid,jobid) VALUES (100,10)')
        self.queries = []
        sql, queries = self.sql, self.queries

        class Cursor:
            def __init__(self, dictionary=False):
                self.inner = sql.cursor()
                self.dictionary = dictionary
            def execute(self, statement, params=()):
                queries.append((statement,params))
                if 'GET_LOCK' in statement or 'RELEASE_LOCK' in statement:
                    statement,params='SELECT 1',()
                if 'information_schema.tables' in statement:
                    statement = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?"
                    params = (params[-1],)
                statement = statement.replace('%s','?').replace('BINARY ','').replace(' ENGINE=InnoDB','')
                statement = statement.replace('ON DUPLICATE KEY UPDATE',
                    'ON CONFLICT(batch_jobid,hostid,dirid) DO UPDATE SET')
                statement = re.sub(r'VALUES\((\w+)\)', r'excluded.\1', statement)
                self.inner.execute(statement, params)
            def executemany(self, statement, rows):
                self.inner.executemany(statement.replace('%s','?'), rows)
            def convert(self,row):
                if row is None or not self.dictionary: return row
                return dict(zip((field[0] for field in self.inner.description),row))
            def fetchone(self): return self.convert(self.inner.fetchone())
            def fetchall(self): return [self.convert(row) for row in self.inner.fetchall()]
            @property
            def lastrowid(self): return self.inner.lastrowid
            @property
            def rowcount(self): return self.inner.rowcount
            def close(self): self.inner.close()

        class Connection:
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def cursor(self,dictionary=False): return Cursor(dictionary)
            def commit(self): sql.commit()
            def rollback(self): sql.rollback()

        self.db = object.__new__(app.DatabaseConnection)
        self.db.database = 'testdb'
        self.cli = DataBaseCLI(self.db)
        patch = mock.patch.object(self.db,'connect',return_value=Connection())
        patch.start(); self.addCleanup(patch.stop)
        patch = mock.patch.object(self.db,'list_live_excluded_absdirs',return_value=[])
        patch.start(); self.addCleanup(patch.stop)

    def add_job(self, jobid, hostid, ended, task='scan', source=None, stage='coarse'):
        self.sql.execute('''INSERT INTO jobs (jobid,hostid,userhost,startdirname,task,status,
            stage,skipdirs,source_jobid,timecreated,timeupdated,timeend,runid)
            VALUES (?,?,'laptop',? ,?,'closed',?,'ignored',?,?,?,?,0)''',
            (jobid,hostid,str(self.root),task,stage,source,ended,ended,ended))

    def add_directory(self, relative='', parent=0, runid=100, hostid=2):
        path = self.root / relative
        path.mkdir(exist_ok=True)
        (path/'file').write_bytes(b'x')
        baseline = datetime.fromtimestamp(path.stat().st_mtime,timezone.utc).replace(tzinfo=None)
        self.sql.execute('''INSERT INTO directories
            (hostid,root_path,path,path_hash,parentdir,name,runid,time_inode_changed,
             files_lastwrite,files_sha256,file_list_sha256,combined_sha256,subdirs_sha256,
             localfilesize,localfilecount,subdircount,emptyfilecount,otherentrycount)
            VALUES (?,?,?,?,?,?,?, ?,?,'files','list','combined','subdirs',1,1,0,0,0)''',
            (hostid,str(self.root),str(path),app._path_hash(str(path)),parent,path.name,runid,baseline,baseline))
        dirid = self.sql.execute('SELECT last_insert_rowid()').fetchone()[0]
        if parent:
            self.sql.execute('UPDATE directories SET subdircount=subdircount+1 WHERE dirid=?',(parent,))
        return dirid

    def enqueue(self):
        self.sql.commit()
        created = self.cli.enqueue_rescan_after_days(10)
        self.assertEqual(len(created),1)
        return created[0][2]

    def run_stage(self,jobid,stage,runid):
        self.sql.execute("UPDATE jobs SET status='active',stage=?,runid=? WHERE jobid=?",(stage,runid,jobid))
        self.sql.execute('INSERT OR REPLACE INTO jobsrunid (runid,jobid) VALUES (?,?)',(runid,jobid))
        self.sql.commit()
        with contextlib.redirect_stdout(io.StringIO()):
            self.db.run_age_rescan_job(jobid,2)

    def test_enqueues_one_new_job_per_aged_host_job_and_no_followups(self):
        self.add_directory()
        self.add_job(20,3,self.now-timedelta(days=20))
        self.add_job(30,2,self.now)
        self.sql.executemany('INSERT INTO jobsrunid (runid,jobid) VALUES (?,?)',[(200,20),(300,30)])
        self.sql.executemany('INSERT INTO directories (hostid,root_path,path,runid) VALUES (?,?,?,?)',
                            [(3,'/other','/other',200),(2,'/recent','/recent',300),(2,'/second','/second',100)])
        self.sql.commit()
        with mock.patch('os.scandir',side_effect=AssertionError('Enqueue must not scan')):
            created=self.cli.enqueue_rescan_after_days(10)
        self.assertEqual([(host,source) for host,source,job in created],[(2,10),(3,20)])
        self.assertEqual(self.cli.enqueue_rescan_after_days(10),[])
        for host,source,job in created:
            self.assertEqual(self.sql.execute('SELECT task,stage,source_jobid FROM jobs WHERE jobid=?',(job,)).fetchone(),
                             ('rescan-after-days','firstscan',source))

    def test_three_stages_one_job_file_mtime_detection_and_batch_isolation(self):
        root=self.add_directory()
        child=self.add_directory('child',root)
        # Rebaseline root after adding its child, so only the child's file changes.
        root_time=datetime.fromtimestamp(self.root.stat().st_mtime,timezone.utc).replace(tzinfo=None)
        self.sql.execute('UPDATE directories SET time_inode_changed=? WHERE dirid=?',(root_time,root))
        stamp=(self.root/'child'/'file').stat().st_mtime+60
        os.utime(self.root/'child'/'file',(stamp,stamp))
        job=self.enqueue()
        self.sql.execute('INSERT INTO tmp_modified (batch_jobid,hostid,dirid,runid) VALUES (999,3,?,100)',(child,))
        self.sql.commit()
        old=self.sql.execute('SELECT dirid,runid,files_sha256,combined_sha256 FROM directories ORDER BY dirid').fetchall()
        jobs_before=self.sql.execute('SELECT COUNT(*) FROM jobs').fetchone()[0]
        self.run_stage(job,'firstscan',1000)
        self.assertEqual(self.sql.execute('SELECT dirid FROM tmp_modified WHERE batch_jobid=?',(job,)).fetchall(),[(child,)])
        self.assertEqual(old,self.sql.execute('SELECT dirid,runid,files_sha256,combined_sha256 FROM directories ORDER BY dirid').fetchall())
        self.run_stage(job,'fixunreadable',1001)
        self.run_stage(job,'workmodified',1002)
        self.assertEqual(self.sql.execute('SELECT COUNT(*) FROM jobs').fetchone()[0],jobs_before)
        self.assertEqual(self.sql.execute('SELECT DISTINCT runid FROM directories').fetchall(),[(1002,)])
        row=self.sql.execute('SELECT files_sha256,combined_sha256,files_lastwrite FROM directories WHERE dirid=?',(child,)).fetchone()
        self.assertEqual(row[:2],(None,None))
        self.assertEqual(row[2],datetime.fromtimestamp(stamp,timezone.utc).replace(tzinfo=None))
        self.assertEqual(self.sql.execute('SELECT files_sha256,combined_sha256 FROM directories WHERE dirid=?',(root,)).fetchone(),('files',None))
        self.assertEqual(self.sql.execute('SELECT batch_jobid FROM tmp_modified').fetchall(),[(999,)])

    def test_fix_uses_remaining_attempts_and_deletes_only_inventory(self):
        root=self.add_directory()
        child=self.add_directory('denied',root)
        descendant=self.add_directory('denied/nested',child)
        self.sql.execute('UPDATE directories SET read_failures=1 WHERE dirid=?',(child,))
        job=self.enqueue()
        inspect=AgeRescanWorkflow._inspect
        denied_calls=[]
        def deny(worker,path):
            if path==str(self.root/'denied'):
                denied_calls.append(path)
                raise PermissionError(path)
            return inspect(worker,path)
        with mock.patch.object(AgeRescanWorkflow,'_inspect',deny):
            self.run_stage(job,'firstscan',1000)
            self.assertEqual(self.sql.execute('SELECT read_failures FROM directories WHERE dirid=?',(child,)).fetchone()[0],2)
            self.run_stage(job,'fixunreadable',1001)
            self.run_stage(job,'workmodified',1002)
        self.assertEqual(len(denied_calls),2)  # firstscan plus one remaining attempt
        self.assertIsNone(self.sql.execute('SELECT dirid FROM directories WHERE dirid=?',(child,)).fetchone())
        self.assertIsNone(self.sql.execute('SELECT dirid FROM directories WHERE dirid=?',(descendant,)).fetchone())
        self.assertTrue((self.root/'denied'/'nested'/'file').exists())

    def test_workmodified_discovers_new_subtrees(self):
        root=self.add_directory()
        new=self.root/'new'
        new.mkdir(); (new/'nested').mkdir(); (new/'nested'/'file').write_bytes(b'new')
        job=self.enqueue()
        self.run_stage(job,'firstscan',1000)
        self.run_stage(job,'fixunreadable',1001)
        self.run_stage(job,'workmodified',1002)
        rows=self.sql.execute('SELECT path,runid FROM directories ORDER BY path').fetchall()
        self.assertEqual([path for path,run in rows],[str(self.root),str(new),str(new/'nested')])
        self.assertTrue(all(run==1002 for path,run in rows))

    def test_firstscan_restarts_without_creating_extra_jobs_or_erasing_other_batches(self):
        self.add_directory()
        self.sql.execute('UPDATE directories SET time_inode_changed=NULL')
        job=self.enqueue()
        self.run_stage(job,'firstscan',1000)
        self.run_stage(job,'firstscan',1001)
        self.assertEqual(self.sql.execute('SELECT COUNT(*) FROM tmp_modified WHERE batch_jobid=?',(job,)).fetchone()[0],1)
        self.assertEqual(self.sql.execute('SELECT COUNT(*) FROM jobs').fetchone()[0],2)

    def test_remaining_read_failures_reset_after_successful_retry(self):
        root=self.add_directory()
        self.sql.execute('UPDATE directories SET read_failures=2 WHERE dirid=?',(root,))
        job=self.enqueue()
        self.run_stage(job,'firstscan',1000)
        self.run_stage(job,'fixunreadable',1001)
        self.assertEqual(self.sql.execute('SELECT read_failures FROM directories WHERE dirid=?',(root,)).fetchone()[0],0)
        self.assertEqual(self.sql.execute('SELECT unreadable FROM tmp_modified WHERE batch_jobid=?',(job,)).fetchone()[0],0)

    def test_parser_rejects_negative_days(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            ClientArgumentParser().parse(['--rescan-after-days','-1'])
        args=ClientArgumentParser().parse(['--hostid','2','--rescan-after-days','0'])
        self.assertTrue(args.hostid_explicit)
        self.assertEqual(args.rescan_after_days,0)

    def test_current_schema_defines_source_job_and_batch_report(self):
        from sqlstructure import SQLStructure, SQLStructureCLI
        self.assertFalse(hasattr(SQLStructureCLI, 'migrate_age_rescan_schema'))
        self.assertFalse(hasattr(SQLStructure, 'REMOTEHOST_MIGRATION_HINT'))
        self.assertIn('source_jobid', SQLStructure.EXPECTED_TABLE_SHAPES['jobs']['columns'])
        self.assertEqual(SQLStructure.EXPECTED_TABLE_SHAPES['tmp_modified']['primary'],
                         ('batch_jobid', 'hostid', 'dirid'))

    def test_spawn_dispatches_all_three_stages_as_one_task(self):
        import server_operations as server
        from types import SimpleNamespace
        for stage in ('firstscan','fixunreadable','workmodified'):
            runtime=mock.Mock()
            runtime._live_excluded_absdirs={}
            runtime.database_connection.run_age_rescan_job.return_value=None
            report=SimpleNamespace(value=b'')
            with mock.patch.object(server,'_runtime',return_value=runtime), \
                    mock.patch.object(server.multiprocessing,'parent_process',return_value=None), \
                    mock.patch.object(server.signal,'signal'):
                server.server_operations._child_entry(stage,'<age-rescan>',11,
                    task='rescan-after-days',hostid=2,error_report=report)
            runtime.database_connection.run_age_rescan_job.assert_called_once_with(
                11,2,note=server.server_operations._log_worker_note)
            runtime.coarse_scan.assert_not_called()
            runtime.detailed_scan.assert_not_called()
            self.assertEqual(report.value,b'')

    def test_workmodified_access_race_returns_to_fix_stage(self):
        root=self.add_directory()
        self.sql.execute('UPDATE directories SET time_inode_changed=NULL')
        job=self.enqueue()
        self.run_stage(job,'firstscan',1000)
        self.run_stage(job,'fixunreadable',1001)
        self.sql.execute("UPDATE jobs SET status='active',stage='workmodified',runid=1002 WHERE jobid=?",(job,))
        self.sql.commit()
        with mock.patch.object(AgeRescanWorkflow,'_inspect',side_effect=PermissionError), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.db.run_age_rescan_job(job,2),'fixunreadable')
        self.assertEqual(self.sql.execute('SELECT read_failures FROM directories WHERE dirid=?',(root,)).fetchone()[0],1)
        self.assertIsNotNone(self.sql.execute('SELECT dirid FROM directories WHERE dirid=?',(root,)).fetchone())

    def test_missing_root_row_deletes_inventory_and_queues_ordinary_scan_for_each_host(self):
        for host in (0,2):
            with self.subTest(host=host):
                self.sql.execute('DELETE FROM directories')
                self.sql.execute('DELETE FROM jobs WHERE jobid<>10')
                self.sql.execute('DELETE FROM jobsrunid WHERE runid<>100')
                self.sql.execute('UPDATE jobs SET hostid=? WHERE jobid=10',(host,))
                self.sql.execute('INSERT OR IGNORE INTO hosts (hostid,machine) VALUES (?,?)',(host,'laptop'))
                first=self.add_directory('first',hostid=host)
                nested=self.add_directory('first/nested',first,hostid=host)
                second=self.add_directory('second',hostid=host)
                self.sql.execute('UPDATE directories SET time_inode_changed=NULL')
                self.sql.commit()
                job=self.cli.enqueue_rescan_after_days(10,host)[0][2]
                self.sql.execute("UPDATE jobs SET status='active',stage='firstscan',runid=1000 WHERE jobid=?",(job,))
                self.sql.commit()
                output=io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.db.run_age_rescan_job(job,host)
                ids=self.sql.execute('SELECT dirid FROM tmp_modified WHERE batch_jobid=? AND hostid=? ORDER BY dirid',(job,host)).fetchall()
                self.assertEqual(ids,[])
                self.assertEqual(self.sql.execute('SELECT COUNT(*) FROM directories WHERE hostid=?',(host,)).fetchone()[0],0)
                queued=self.sql.execute("SELECT startdirname,task,status,stage,scanall,source_jobid FROM jobs WHERE jobid NOT IN (10,?)",(job,)).fetchall()
                self.assertEqual(queued,[(str(self.root),'scan','insert','coarse',0,None)])
                self.assertTrue((self.root/'first'/'nested'/'file').exists())
                self.assertIn('3 Directory-Einträge gelöscht; normaler Scan-Job',output.getvalue())

    def test_replacement_scan_keeps_effective_exclusions_on_server_and_local_hosts(self):
        for host in (0,2):
            with self.subTest(host=host):
                self.sql.execute('DELETE FROM directories')
                self.sql.execute('DELETE FROM jobs WHERE jobid<>10')
                self.sql.execute('DELETE FROM jobsrunid WHERE runid<>100')
                self.sql.execute('UPDATE jobs SET hostid=?,skipdirs=? WHERE jobid=10',(host,'source-only'))
                self.sql.execute('INSERT OR IGNORE INTO hosts (hostid,machine) VALUES (?,?)',(host,'laptop'))
                self.sql.execute('DELETE FROM excluded_names')
                self.sql.executemany('INSERT INTO excluded_names (name) VALUES (?)',[('.git',),('source-only',)])
                self.add_directory('child',hostid=host)
                self.sql.commit()
                job=self.cli.enqueue_rescan_after_days(10,host)[0][2]
                # Reproduce an older maintenance record with an empty saved list.
                self.sql.execute("UPDATE jobs SET status='active',stage='firstscan',skipdirs='' WHERE jobid=?",(job,))
                self.sql.commit()
                with mock.patch.object(self.db,'list_live_excluded_absdirs',return_value=['/excluded/path']), \
                        contextlib.redirect_stdout(io.StringIO()):
                    self.db.run_age_rescan_job(job,host)
                skipdirs=self.sql.execute("SELECT skipdirs FROM jobs WHERE task='scan' AND status='insert'").fetchone()[0]
                self.assertEqual(set(skipdirs.split(',')),{'source-only','.git','/excluded/path'})
                self.assertEqual(skipdirs.split(',').count('source-only'),1)

    def test_worker_repair_notice_goes_to_console_and_syslog(self):
        import server_operations as server
        message='Host 0: Inventarwurzel fehlt; 3 Directory-Einträge gelöscht; Scan-Job 61 eingereiht.'
        output=io.StringIO()
        handler=mock.MagicMock()
        with mock.patch.object(server,'SysLogHandler',return_value=handler), \
                mock.patch.object(server.logging,'Logger') as logger, \
                contextlib.redirect_stdout(output):
            server.server_operations._log_worker_note(message)
        self.assertIn(message,output.getvalue())
        logger.return_value.warning.assert_called_once_with(message)
        handler.close.assert_called_once_with()

    def test_final_stage_cleans_remaining_batch_entries_without_touching_other_jobs(self):
        root=self.add_directory()
        ignored=self.add_directory('ignored',root)
        job=self.enqueue()
        self.run_stage(job,'firstscan',1000)
        self.run_stage(job,'fixunreadable',1001)
        self.sql.executemany('INSERT INTO tmp_modified (batch_jobid,hostid,dirid,runid) VALUES (?,?,?,100)',
                             [(job,2,ignored),(999,2,ignored),(job,3,ignored)])
        self.sql.commit()
        self.run_stage(job,'workmodified',1002)
        self.assertEqual(self.sql.execute('SELECT batch_jobid,hostid FROM tmp_modified ORDER BY batch_jobid,hostid').fetchall(),
                         [(job,3),(999,2)])

    def test_parser_dispatch_and_stage_transitions(self):
        args=ClientArgumentParser().parse(['--rescan-after-days','10'])
        client=object.__new__(client_operations); client.args=args; client.database=mock.Mock()
        client.database.enqueue_rescan_after_days.return_value=[(2,10,11)]
        with contextlib.redirect_stdout(io.StringIO()): self.assertEqual(client.run(),0)
        client.database.enqueue_rescan_after_days.assert_called_once_with(10,None)
        for stage,next_stage in [('firstscan','fixunreadable'),('fixunreadable','workmodified'),('workmodified',None)]:
            coordinator=mock.Mock(); coordinator.run_spawn.return_value='ok'
            with mock.patch.object(app,'set_job_status') as status, contextlib.redirect_stdout(io.StringIO()):
                app.process_job(dict(jobid=11,hostid=2,task='rescan-after-days',stage=stage),stage,coordinator)
            coordinator.run_spawn.assert_called_once_with(stage,'<age-rescan>',11,task='rescan-after-days',hostid=2)
            if next_stage: status.assert_called_once_with(11,'pending',stage=next_stage,hostid=2)
            else: status.assert_called_once_with(11,'closed',hostid=2)


# directory_family_reads regression coverage.


class TestDirectoryFamilyReads(unittest.TestCase):
    def setUp(self):
        self.sql = sqlite3.connect(':memory:')
        self.addCleanup(self.sql.close)
        self.sql.executescript('''
            CREATE TABLE directories (dirid INTEGER PRIMARY KEY, hostid INTEGER,
                parentdir INTEGER, path_hash TEXT, path TEXT, finished INTEGER,
                time_inode_changed TEXT, files_sha256 TEXT, extra_metadata TEXT);
            CREATE TABLE tmp2_job_work (hostid INTEGER, jobid INTEGER, stage TEXT,
                path_hash TEXT, path TEXT, parent_path TEXT, child_count INTEGER, state TEXT);
        ''')
        self.sql.executemany('INSERT INTO directories VALUES (?,?,?,?,?,?,?,?,?)', [
            (1, 2, 0, 'root', '/root', 9, '2026-11-04', 'root-hash', 'root-extra'),
            (2, 2, 1, 'child', '/root/child', 8, '2026-11-03', 'child-hash', 'child-extra'),
            (3, 2, 1, 'sibling', '/root/sibling', 7, None, 'sibling-hash', 'sibling-extra'),
            (4, 2, 2, 'grandchild', '/root/child/grandchild', 6, None, 'grand-hash', 'grand-extra'),
            (5, 3, 1, 'other-host', '/other', 5, None, 'other-hash', 'other-extra'),
        ])
        self.sql.commit()
        self.queries = []
        sql, queries = self.sql, self.queries

        class Cursor:
            def __init__(self):
                self.inner = sql.cursor()
                self.closed = False
            def execute(self, statement, params=()):
                queries.append((statement, params))
                self.inner.execute(statement.replace('%s', '?'), params)
            def fetchall(self):
                columns = [field[0] for field in self.inner.description]
                return [dict(zip(columns, row)) for row in self.inner.fetchall()]
            def close(self):
                self.closed = True
                self.inner.close()

        class Connection:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def cursor(self, dictionary=False): return Cursor()
            def commit(self): sql.commit()
            def rollback(self): sql.rollback()

        self.connection = Connection()
        self.db = object.__new__(app.DatabaseConnection)
        patch = mock.patch.object(self.db, 'connect', return_value=self.connection)
        patch.start()
        self.addCleanup(patch.stop)

    def test_one_query_returns_all_metadata_and_only_immediate_same_host_children(self):
        result = self.db.read_directory_families([dict(path_hash='root', path='/root')], 2)
        self.assertEqual(len(self.queries), 1)
        root = result['root']
        self.assertEqual(root['directory']['extra_metadata'], 'root-extra')
        self.assertEqual(root['directory']['time_inode_changed'], '2026-11-04')
        self.assertEqual([child['dirid'] for child in root['children']], [2, 3])
        self.assertEqual(root['children'][0]['files_sha256'], 'child-hash')
        self.assertEqual(root['children'][0]['extra_metadata'], 'child-extra')
        self.assertFalse(any(key.startswith('_family') for key in root['directory']))

    def test_multiple_parents_grouped_and_collision_or_absent_paths_not_reused(self):
        works = [dict(path_hash='root', path='/root'), dict(path_hash='child', path='/root/child'),
                 dict(path_hash='missing', path='/missing')]
        result = self.db.read_directory_families(works, 2)
        self.assertEqual(len(self.queries), 1)
        self.assertEqual([row['dirid'] for row in result['child']['children']], [4])
        self.assertEqual(result['missing'], dict(directory=None, children=[]))
        result = self.db.read_directory_families([dict(path_hash='root', path='/wrong-path')], 2)
        self.assertEqual(result['root'], dict(directory=None, children=[]))

    def test_empty_work_does_not_open_connection(self):
        with mock.patch.object(self.db, 'connect') as connect:
            self.assertEqual(self.db.read_directory_families([], 2), {})
        connect.assert_not_called()

    def test_coarse_recent_check_reuses_prefetched_directory(self):
        work = dict(path_hash='hash', path='/root/one/two', parent_path='/root/one',
                    directory=dict(dirid=3, path='/root/one/two', finished=9, combined_sha256='complete', subdircount=0), subdirectories=[])
        database = mock.Mock()
        database.claim_work_batch.side_effect = [[work], []]
        database.complete_coarse_batch.return_value = 0
        with mock.patch.object(app, 'database_connection', database), \
                mock.patch.object(app.os.path, 'isdir', return_value=True), \
                mock.patch.object(app.os, 'scandir', side_effect=AssertionError('Recent directory rescanned')), \
                contextlib.redirect_stdout(io.StringIO()):
            app.coarse_scan('/root', 7, scanall=0, recent_runids=(9,), hostid=2)
        database.is_recently_finished.assert_not_called()
        result = database.complete_coarse_batch.call_args.args[2][0]
        self.assertTrue(result['skip_subtree'])

    def test_claim_batch_attaches_family_data_with_single_grouped_inventory_read(self):
        self.sql.execute("INSERT INTO tmp2_job_work VALUES (2,7,'coarse','root','/root',NULL,2,'pending')")
        self.sql.execute("INSERT INTO tmp2_job_work VALUES (2,7,'coarse','child','/root/child','/root',1,'pending')")
        self.sql.commit()
        with mock.patch.object(self.db, 'get_job_hostid', return_value=2):
            works = self.db.claim_work_batch(7, 'coarse', hostid=2)
        self.assertEqual(len(self.queries), 2)
        by_hash = {work['path_hash']: work for work in works}
        self.assertEqual(by_hash['root']['directory']['dirid'], 1)
        self.assertEqual([row['dirid'] for row in by_hash['root']['subdirectories']], [2, 3])
        self.assertEqual([row['dirid'] for row in by_hash['child']['subdirectories']], [4])


# root_runid_conflicts regression coverage.


class TestRootRunidConflicts(unittest.TestCase):
    def test_exact_paths_and_distinct_runs_across_all_hosts(self):
        sql = sqlite3.connect(':memory:')
        self.addCleanup(sql.close)
        sql.execute('CREATE TABLE directories (hostid INTEGER, root_path TEXT, runid INTEGER)')
        sql.execute('CREATE TABLE jobsrunid (runid INTEGER, jobid INTEGER)')
        sql.executemany('INSERT INTO jobsrunid VALUES (?,?)', [(1,1),(2,2),(3,3),(4,4),(5,5),(6,5)])
        sql.executemany('INSERT INTO directories VALUES (?,?,?)', [
            (0,'/Data',1), (0,'/Data',1), (0,'/data',2),
            (0,'/conflict',3), (2,'/conflict',4), (2,'/conflict',4),
            (0,'/same',5), (2,'/same',6)])
        class Cursor:
            def execute(self, statement):
                self.inner = sql.execute(statement.replace('BINARY d.root_path', 'd.root_path COLLATE BINARY'))
            def fetchall(self): return self.inner.fetchall()
        db = object.__new__(app.DatabaseConnection)
        self.assertEqual(db.list_root_path_job_conflicts(cursor=Cursor()),
                         [dict(root_path='/conflict', jobids=[3,4], directory_count=3)])

    def test_localserver_blocks_and_central_servers_warn_only(self):
        for mode in ('localserver', 'server', 'serverplus'):
            with self.subTest(mode=mode):
                operation = server.server_operations(SimpleNamespace(mode=mode, local_hostid=7))
                operation.database = mock.Mock()
                operation.database.validate_local_host.return_value = 7
                operation.database.plan_root_job_cleanup.return_value = []
                operation.database.list_root_path_job_conflicts.return_value = [
                    dict(root_path='/photos', jobids=[10,11], directory_count=3)]
                operation._run_server = mock.Mock(return_value=0)
                output = io.StringIO()
                with mock.patch.object(server, '_server_instance_lock', return_value=contextlib.nullcontext()), \
                        contextlib.redirect_stderr(output):
                    result = operation.run_server()
                operation.database.list_root_path_job_conflicts.assert_called_once_with()
                self.assertIn('/photos', output.getvalue())
                self.assertIn('10, 11', output.getvalue())
                if mode == 'localserver':
                    self.assertEqual(result, 1)
                    operation._run_server.assert_not_called()
                    operation.database.start_wake_monitor.assert_not_called()
                    self.assertIn('FEHLER', output.getvalue())
                else:
                    self.assertEqual(result, 0)
                    operation._run_server.assert_called_once_with(0)
                    self.assertIn('WARNUNG', output.getvalue())

    def test_local_repair_defaults_no_and_accepts_only_y(self):
        candidate = dict(root_path='/photos', hostid=7, jobid=10, timeend=None, directory_count=3)
        conflict = dict(root_path='/photos', jobids=[10,11], directory_count=5)
        for answer in ('', 'n', 'y', 'yes'):
            with self.subTest(answer=answer):
                operation = server.server_operations(SimpleNamespace(mode='localserver', local_hostid=7))
                operation.database = mock.Mock()
                operation.database.list_root_path_job_conflicts.side_effect = [[conflict], []]
                operation.database.plan_root_job_cleanup.return_value = [candidate]
                operation.database.delete_root_job_candidates.return_value = 3
                output = io.StringIO()
                with mock.patch('builtins.input', return_value=answer) as prompt, \
                        contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                    result = operation._check_root_job_startup()
                self.assertIn('[yN]', prompt.call_args.args[0])
                self.assertIn('keine Stoppzeit', output.getvalue())
                if answer == 'y':
                    self.assertTrue(result)
                    operation.database.delete_root_job_candidates.assert_called_once_with([candidate])
                    self.assertEqual(operation.database.list_root_path_job_conflicts.call_count, 2)
                else:
                    self.assertFalse(result)
                    operation.database.delete_root_job_candidates.assert_not_called()

    def test_plans_old_jobs_and_missing_endtime_even_when_their_ids_are_larger(self):
        sql = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
        self.addCleanup(sql.close)
        sql.executescript('''
            CREATE TABLE directories (hostid INTEGER, root_path TEXT, runid INTEGER);
            CREATE TABLE jobsrunid (runid INTEGER, jobid INTEGER);
            CREATE TABLE jobs (jobid INTEGER, hostid INTEGER, timeend TIMESTAMP, status TEXT DEFAULT 'closed');
        ''')
        old, recent = datetime(2026,11,1), datetime(2026,11,4)
        sql.executemany('INSERT INTO directories VALUES (?,?,?)',
                        [(7,'/photos',100),(7,'/photos',101),(7,'/photos',200),
                         (7,'/photos',900),(7,'/other',100)])
        sql.executemany('INSERT INTO jobsrunid VALUES (?,?)', [(100,10),(101,10),(200,20),(900,90)])
        sql.executemany('INSERT INTO jobs (jobid,hostid,timeend) VALUES (?,?,?)', [(10,7,old),(20,7,recent),(90,7,None)])
        class Cursor:
            def execute(self, statement, params=()):
                self.inner = sql.execute(statement.replace('%s','?').replace(
                    'BINARY d.root_path','d.root_path COLLATE BINARY'), params)
            def fetchall(self): return self.inner.fetchall()
        db = object.__new__(app.DatabaseConnection)
        plan = db.plan_root_job_cleanup(['/photos'], cursor=Cursor())
        self.assertEqual([(row['jobid'],row['directory_count']) for row in plan], [(10,2),(90,1)])
        self.assertEqual([row['timeend'] for row in plan], [old,None])
        sql.execute("UPDATE jobs SET status='active' WHERE jobid=90")
        plan=db.plan_root_job_cleanup(['/photos'],cursor=Cursor())
        self.assertEqual([row['jobid'] for row in plan],[10])
        # An active job is protected even if an old stop time remains stored.
        sql.execute("UPDATE jobs SET status='active' WHERE jobid=10")
        self.assertEqual(db.plan_root_job_cleanup(['/photos'],cursor=Cursor()),[])


    def test_delete_revalidates_plan_and_limits_deletion_to_host_root_and_job(self):
        candidate = dict(root_path='/photos', hostid=7, jobid=10, timeend=None, directory_count=3)
        db = object.__new__(app.DatabaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.rowcount = 3
        with mock.patch.object(db,'connect',return_value=connection), \
                mock.patch.object(db,'plan_root_job_cleanup',return_value=[candidate]):
            self.assertEqual(db.delete_root_job_candidates([candidate]),3)
        statement,params = cursor.execute.call_args.args
        self.assertEqual(params,(7,'/photos',10))
        self.assertIn('r.jobid = %s',statement)
        self.assertIn('d.hostid = %s',statement)
        connection.commit.assert_called_once_with()
        cursor.execute.reset_mock()
        with mock.patch.object(db,'connect',return_value=connection), \
                mock.patch.object(db,'plan_root_job_cleanup',return_value=[]):
            with self.assertRaisesRegex(RuntimeError,'geändert'):
                db.delete_root_job_candidates([candidate])
        cursor.execute.assert_not_called()

    def test_verification_rejects_conflicting_runs(self):
        db = DataBaseCLI(object.__new__(app.DatabaseConnection))
        db.database = 'testdb'
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        shapes = db.EXPECTED_TABLE_SHAPES
        columns = [(table, name, *definition) for table, shape in shapes.items()
                   for name, definition in shape['columns'].items()]
        def fetchall():
            statement = cursor.execute.call_args.args[0]
            if 'information_schema.columns' in statement: return columns
            if 'information_schema.statistics' in statement:
                return [(table, name, position) for table, shape in shapes.items()
                        for position, name in enumerate(shape['primary'], 1)]
            if 'SELECT hostid, tabellenname' in statement: return [(0,'tmp0_job_work')]
            if 'GROUP BY BINARY d.root_path' in statement: return [('/photos',10,2),('/photos',11,1)]
            raise AssertionError(statement)
        cursor.fetchall.side_effect = fetchall
        output = io.StringIO()
        with mock.patch.object(db, 'connect', return_value=connection), contextlib.redirect_stdout(output):
            self.assertFalse(db.check_schema())
        self.assertIn('root_path=/photos hat unterschiedliche Job-IDs: 10, 11', output.getvalue())
        self.assertFalse(any(call.args[0].lstrip().startswith(('UPDATE','DELETE','ALTER'))
                             for call in cursor.execute.call_args_list))


# resetlocks regression coverage.


class TestResetLocks(unittest.TestCase):
    def database(self):
        db=object.__new__(app.DatabaseConnection)
        db.host,db.port,db.database='localhost',3306,'testdb'
        return db

    def test_deletes_unlocked_leftover_files_for_registered_hosts(self):
        db=self.database()
        with tempfile.TemporaryDirectory() as folder:
            for host in (0,2):
                with _server_instance_lock(db,host,directory=folder): pass
            with mock.patch.object(db,'list_hosts',return_value=[(2,) ]):
                self.assertEqual(db.reset_server_locks(directory=folder),[0,2])
            self.assertEqual(list(Path(folder).iterdir()),[])
            with _server_instance_lock(db,2,directory=folder): pass

    def test_reset_trusts_stopped_server_assumption_but_preserves_other_database(self):
        db=self.database()
        with tempfile.TemporaryDirectory() as folder:
            with _server_instance_lock(db,2,directory=folder):
                self.assertEqual(db.reset_server_locks(2,directory=folder),[2])
                self.assertEqual(list(Path(folder).iterdir()),[])
            other=self.database(); other.database='other'
            self.assertEqual(other.reset_server_locks(2,directory=folder),[])

    def test_startup_reopens_file_removed_by_reset_after_open(self):
        import server_operations as server
        db=self.database()
        real_flock=server.fcntl.flock
        acquisitions=[]
        with tempfile.TemporaryDirectory() as folder:
            def reset_race(descriptor,operation):
                result=real_flock(descriptor,operation)
                if operation & server.fcntl.LOCK_EX:
                    acquisitions.append(descriptor)
                    if len(acquisitions)==1:
                        next(Path(folder).iterdir()).unlink()
                return result
            with mock.patch.object(server.fcntl,'flock',side_effect=reset_race):
                with _server_instance_lock(db,2,directory=folder):
                    self.assertEqual(len(acquisitions),2)
                    self.assertEqual(len(list(Path(folder).iterdir())),1)

    def test_reports_each_lock_scope_pid_path_and_deletion_result(self):
        import os
        db=self.database()
        with tempfile.TemporaryDirectory() as folder:
            for host in (0,2):
                with _server_instance_lock(db,host,directory=folder): pass
            paths=[str(path) for path in Path(folder).iterdir()]
            output=io.StringIO()
            with mock.patch.object(db,'list_hosts',return_value=[(2,)]), \
                    contextlib.redirect_stdout(output):
                self.assertEqual(db.reset_server_locks(directory=folder),[0,2])
            text=output.getvalue()
            for path in paths: self.assertIn(path,text)
            self.assertIn('für Host-ID 0',text)
            self.assertIn('für Host-ID 2',text)
            self.assertEqual(text.count('Datenbank: testdb; DB-Server: localhost:3306'),2)
            self.assertEqual(text.count(f'Gespeicherte PID: {os.getpid()}'),2)
            self.assertEqual(text.count('Ergebnis: Datei gelöscht'),2)
            self.assertIn('Laufzustand nicht geprüft',text)

    def test_option_dispatches_with_optional_host(self):
        for argv,host in ((['--resetlocks'],None),(['--hostid','2','--resetlocks'],2)):
            client=object.__new__(client_operations)
            client.args=ClientArgumentParser().parse(argv)
            client.database=mock.Mock()
            client.database.reset_server_locks.return_value=[2]
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(client.run(),0)
            client.database.reset_server_locks.assert_called_once_with(hostid=host)
            self.assertIn('1 lokale Lockdateien gelöscht',output.getvalue())
            self.assertIn('Server als gestoppt vorausgesetzt',output.getvalue())


if __name__ == '__main__':
    unittest.main()
