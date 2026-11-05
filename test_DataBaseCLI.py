"""Tests for DataBaseCLI.py."""

from unittest import mock
import DataBaseCore as core
# Importing a test module must not probe the configured production database.
with mock.patch.object(core.DataBaseConnectionCore, 'connect', side_effect=RuntimeError('offline test')):
    import version25 as app
import contextlib
import io
import os
import sqlite3
import unittest
import uuid
from datetime import datetime
from unittest.mock import MagicMock
from DataBaseCLI import DataBaseCLI
from client_operations import ClientArgumentParser
from client_operations import client_operations
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from doubles_output import doubles_output
from pathlib import Path
from types import SimpleNamespace
import DataBaseCore
import server_operations as server
import tempfile
import re
import version25
from DataBaseCore import DataBaseConnectionCore


def configure_schema_cursor(cursor, rows, hosts=None):
    hosts = [(0, "tmp0_job_work")] if hosts is None else hosts
    def fetchall():
        sql = cursor.execute.call_args.args[0]
        if "GROUP BY BINARY d.root_path" in sql:
            return []
        if "information_schema.columns" in sql:
            return rows
        if "SELECT hostid, tabellenname" in sql or "SELECT hostid, NULL AS tabellenname" in sql:
            return hosts
        if "information_schema.statistics" in sql:
            return [(table, col, pos) for table, shape in DataBaseCLI.EXPECTED_TABLE_SHAPES.items()
                    for pos, col in enumerate(shape["primary"], 1)]
        raise AssertionError(sql)
    cursor.fetchall.side_effect = fetchall

class TestCheckModifications(unittest.TestCase):
    @staticmethod
    def with_inode_baselines(rows):
        result = []
        for row in rows:
            try:
                baseline = datetime.fromtimestamp(os.stat(row[2]).st_mtime, timezone.utc).replace(tzinfo=None)
            except OSError:
                baseline = None
            result.append(row + (baseline,))
        return result

    def test_inode_time_is_first_check_and_records_directory_only_without_scandir(self):
        with tempfile.TemporaryDirectory() as folder:
            db = object.__new__(app.DatabaseConnection)
            connection = mock.MagicMock()
            connection.__enter__.return_value = connection
            cursor = connection.cursor.return_value
            cursor.fetchone.return_value = (1,)
            cursor.fetchall.return_value = [(7, 9, folder, None, '', datetime(2000, 1, 1))]
            expected = datetime.fromtimestamp(os.stat(folder).st_mtime, timezone.utc).replace(tzinfo=None)
            with mock.patch.object(db, 'connect', return_value=connection), \
                    mock.patch.object(db, 'list_live_excluded_absdirs', return_value=[]), \
                    mock.patch('DataBaseCore.os.scandir', side_effect=AssertionError('Must not scan contents')):
                self.assertEqual(db.check_modifications(0), 1)
            insert = next(call for call in cursor.execute.call_args_list if 'INSERT INTO tmp_modified' in call.args[0])
            self.assertIn('subfolders_excluded', insert.args[0])
            self.assertEqual(insert.args[1], (0, 7, 9, expected, True))
            self.assertFalse(any('SET time_inode_changed' in call.args[0]
                                 for call in cursor.execute.call_args_list))
            connection.commit.assert_called_once_with()

    def test_recreates_report_filters_files_and_updates_flags_at_end(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for name in ('changed', 'same', 'empty'):
                (root / name).mkdir()
            (root / 'changed' / 'file').write_bytes(b'x')
            os.utime(root / 'changed' / 'file', (200, 200))
            (root / 'changed' / 'ignored').write_bytes(b'ignore')
            (root / 'changed' / 'link').symlink_to(root / 'changed' / 'ignored')
            (root / 'changed' / 'child').mkdir()
            os.mkfifo(root / 'changed' / 'pipe')
            (root / 'same' / 'file').write_bytes(b'x')
            os.utime(root / 'same' / 'file', (100, 100))
            old = datetime(1970, 1, 1, 0, 1, 40)
            connection = mock.MagicMock()
            connection.__enter__.return_value = connection
            cursor = connection.cursor.return_value
            cursor.fetchone.return_value = (1,)
            cursor.fetchall.return_value = self.with_inode_baselines([
                (1, 9, str(root / 'changed'), old, 'ignored'),
                (2, 9, str(root / 'same'), old, ''),
                (3, 9, str(root / 'empty'), old, ''),
            ])
            db = DataBaseCLI(object.__new__(app.DatabaseConnection))
            with mock.patch.object(db.connection, 'connect', return_value=connection), \
                    mock.patch.object(db.connection, 'list_live_excluded_absdirs', return_value=[]):
                self.assertEqual(db.check_modifications(hostid=2), 2)
            calls = [call for call in cursor.execute.call_args_list
                     if "GET_LOCK" not in call.args[0] and "RELEASE_LOCK" not in call.args[0]]
            self.assertIn('CREATE TABLE IF NOT EXISTS tmp_modified', calls[0].args[0])
            self.assertIn('DELETE FROM tmp_modified WHERE batch_jobid=0', calls[1].args[0])
            self.assertEqual(calls[2].args[1], (2,))
            inserts = [call.args[1] for call in calls if 'INSERT INTO tmp_modified' in call.args[0]]
            self.assertEqual(inserts, [(2, 1, 9, datetime(1970, 1, 1, 0, 3, 20), False), (2, 3, 9, None, False)])
            self.assertIn('SET d.rescan_from_here = TRUE', calls[-1].args[0])
            self.assertIn('m.runid = d.runid', calls[-1].args[0])
            self.assertEqual(calls[-1].args[1], (2,))
            connection.commit.assert_called_once_with()

    def test_scan_failure_rolls_back_without_flag_update(self):
        db = DataBaseCLI(object.__new__(app.DatabaseConnection))
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (1,)
        cursor.fetchall.return_value = self.with_inode_baselines([(1, 9, '/missing', None, '')])
        with mock.patch.object(db.connection, 'connect', return_value=connection), \
                mock.patch.object(db.connection, 'list_live_excluded_absdirs', return_value=[]), \
                mock.patch('DataBaseCLI.os.scandir', side_effect=PermissionError), \
                mock.patch('DataBaseCore.os.stat', side_effect=PermissionError):
            with self.assertRaises(PermissionError):
                db.check_modifications()
        connection.rollback.assert_called_once_with()
        connection.commit.assert_not_called()
        self.assertFalse(any('SET d.rescan_from_here' in call.args[0]
                             for call in cursor.execute.call_args_list))


    def test_missing_nested_directories_mark_nearest_existing_parent_once(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'file').write_bytes(b'x')
            os.utime(root / 'file', (100, 100))
            # Also exercise an old directory that is now a regular file.
            (root / 'replaced').write_bytes(b'not a directory')
            os.utime(root / 'replaced', (50, 50))
            rows = [(1, 9, folder, datetime(1970, 1, 1, 0, 1, 40), ''),
                    (2, 9, str(root / 'missing'), None, ''),
                    (3, 9, str(root / 'missing' / 'nested'), None, ''),
                    (4, 9, str(root / 'replaced'), None, '')]
            db = object.__new__(app.DatabaseConnection)
            connection = mock.MagicMock()
            connection.__enter__.return_value = connection
            cursor = connection.cursor.return_value
            cursor.fetchone.return_value = (1,)
            cursor.fetchall.return_value = self.with_inode_baselines(rows)
            with mock.patch.object(db, 'connect', return_value=connection), \
                    mock.patch.object(db, 'list_live_excluded_absdirs', return_value=[]), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(db.check_modifications(0), 1)
            inserts = [call.args[1] for call in cursor.execute.call_args_list
                       if 'INSERT INTO tmp_modified' in call.args[0]]
            self.assertEqual(inserts, [(0, 1, 9, datetime(1970, 1, 1, 0, 1, 40), False)])
            connection.commit.assert_called_once_with()
            connection.rollback.assert_not_called()

    def test_missing_root_without_inventory_parent_reports_path(self):
        db = object.__new__(app.DatabaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (1,)
        cursor.fetchall.return_value = self.with_inode_baselines([(1, 9, '/gone-inventory-root', None, '')])
        with mock.patch.object(db, 'connect', return_value=connection), \
                mock.patch.object(db, 'list_live_excluded_absdirs', return_value=[]), \
                mock.patch('DataBaseCore.os.scandir', side_effect=FileNotFoundError):
            with self.assertRaisesRegex(RuntimeError, '/gone-inventory-root'):
                db.check_modifications(0)
        connection.rollback.assert_called_once_with()
        connection.commit.assert_not_called()

    def test_shared_report_waits_for_lock_and_releases_after_success(self):
        db = object.__new__(app.DatabaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.side_effect = [(0,), (1,), (1,)]
        cursor.fetchall.return_value = self.with_inode_baselines([])
        with mock.patch.object(db, 'connect', return_value=connection), \
                mock.patch.object(db, 'list_live_excluded_absdirs', return_value=[]), \
                mock.patch('DataBaseCore.time.sleep') as pause:
            self.assertEqual(db.check_modifications(2), 0)
        pause.assert_called_once_with(0.5)
        calls = cursor.execute.call_args_list
        self.assertIn('GET_LOCK', calls[0].args[0])
        self.assertIn('GET_LOCK', calls[1].args[0])
        self.assertIn('CREATE TABLE IF NOT EXISTS', calls[2].args[0])
        self.assertIn('RELEASE_LOCK', calls[-1].args[0])
        connection.commit.assert_called_once_with()

    def test_lock_error_leaves_existing_report_and_flags_untouched(self):
        db = object.__new__(app.DatabaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (None,)
        with mock.patch.object(db, 'connect', return_value=connection), \
                mock.patch.object(db, 'list_live_excluded_absdirs', return_value=[]):
            with self.assertRaises(RuntimeError):
                db.check_modifications(2)
        self.assertEqual(cursor.execute.call_count, 1)
        connection.rollback.assert_called_once_with()
        connection.commit.assert_not_called()


class TestJobConstraints(unittest.TestCase):
    def database(self):
        database = DataBaseCLI(object.__new__(app.DatabaseConnection))
        database.database = 'testdb'
        return database

    def test_existing_jobs_get_current_checks_in_one_alter(self):
        database = self.database()
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (1,)
        with mock.patch.object(database, 'connect', return_value=connection):
            database.ensure_jobs_constraints()
        alter = cursor.execute.call_args.args[0]
        self.assertIn('ALTER TABLE jobs', alter)
        for name in ('chk_job_task', 'chk_job_status'):
            self.assertIn('DROP CONSTRAINT IF EXISTS ' + name, alter)
            self.assertIn('ADD CONSTRAINT ' + name, alter)
        self.assertIn("'rescan'", alter)
        self.assertIn("'check-modifications'", alter)
        self.assertIn("'stale'", alter)
        self.assertEqual(cursor.execute.call_count, 2)
        connection.commit.assert_called_once_with()

    def test_invalid_existing_values_abort_without_row_changes(self):
        database = self.database()
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (1,)
        cursor.execute.side_effect = [None, RuntimeError('CHECK failed')]
        with mock.patch.object(database, 'connect', return_value=connection):
            with self.assertRaisesRegex(RuntimeError, 'CHECK failed'):
                database.ensure_jobs_constraints()
        connection.rollback.assert_called_once_with()
        connection.commit.assert_not_called()
        self.assertFalse(any(call.args[0].lstrip().startswith(('UPDATE', 'DELETE'))
                             for call in cursor.execute.call_args_list))

    def test_serverinit_without_jobs_table_does_not_alter_it(self):
        database = self.database()
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (0,)
        with mock.patch.object(database, 'connect', return_value=connection):
            database.ensure_jobs_constraints()
        self.assertEqual(cursor.execute.call_count, 1)

    def test_both_initializers_ensure_constraints(self):
        for command in ('initialize', 'serverinit'):
            with self.subTest(command=command):
                database = self.database()
                with mock.patch.object(database, 'init_database'), \
                        mock.patch.object(database, '_show_statement_timeout_hint'), \
                        mock.patch.object(database, 'ensure_jobs_constraints') as ensure, \
                        mock.patch.object(database, 'init_alldirs', return_value=False), \
                        mock.patch.object(database, '_write_local_host', return_value=0), \
                        mock.patch.object(database, 'check_schema', return_value=True), \
                        contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(getattr(database, command)(), 0)
                ensure.assert_called_once_with()


class TestDatabaseConnection(unittest.TestCase):
    def test_init_database_creates_current_host_schema_without_migration(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        server = mock.MagicMock()
        server.__enter__.return_value = server
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        with mock.patch.object(database, "connect_server", return_value=server), \
             mock.patch.object(database, "connect", return_value=connection), \
             contextlib.redirect_stdout(io.StringIO()):
            database.init_database()
            database.init_alldirs(reset=False)
        statements = [
            call.args[0] for call in connection.cursor.return_value.execute.call_args_list
        ]
        self.assertFalse(any(sql.lstrip().startswith("ALTER TABLE") for sql in statements))
        directories_ddl = next(sql for sql in statements if "CREATE TABLE IF NOT EXISTS directories" in sql)
        self.assertIn("rescan_from_here BOOLEAN NOT NULL DEFAULT FALSE", directories_ddl)
        self.assertIn("files_lastwrite DATETIME(6) NULL", directories_ddl)
        self.assertEqual(database.EXPECTED_TABLE_SHAPES["directories"]["columns"]["rescan_from_here"],
                         ("tinyint(1)", "NO", "0", None))
        jobs_ddl = next(sql for sql in statements if "CREATE TABLE IF NOT EXISTS jobs" in sql)
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS tmp_highpriority" in sql for sql in statements))
        self.assertFalse(any("CREATE TABLE IF NOT EXISTS tmp0_job_work" in sql for sql in statements))
        self.assertIn("hostid SMALLINT NOT NULL DEFAULT 0", jobs_ddl)
        self.assertIn("PRIMARY KEY (jobid)", jobs_ddl)
        self.assertIn("task IN ('scan', 'rescan', 'check-modifications', 'rescan-after-days', 'check', 'verify', 'binarycheck')", jobs_ddl)

    def test_list_recent_jobs_uses_fifteen_day_window_without_time_fields(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchall.return_value = [{"runid": 4}, {"runid": 9}]
        with mock.patch.object(database, "connect", return_value=connection):
            jobs = database.list_recent_jobs(days=15)
        self.assertEqual(jobs, [{"runid": 4}, {"runid": 9}])
        sql, params = cursor.execute.call_args.args
        self.assertIn("SELECT jobid, userhost, startdirname", sql)
        self.assertIn("timestart >= %s", sql)
        self.assertIn("timestart <= %s", sql)
        self.assertNotIn("timecreated", sql)
        self.assertNotIn("timeend", sql)
        self.assertEqual(len(params), 2)
        window = params[1] - params[0]
        self.assertGreaterEqual(window, app.timedelta(days=15))
        self.assertLess(window, app.timedelta(days=15, seconds=1))
        cursor.close.assert_called_once_with()

    def _schema_rows(self, root_collation="utf8mb4_bin", noperm_default=0, noperm_type="bigint unsigned"):
        rows = []
        for table, shape in DataBaseCLI.EXPECTED_TABLE_SHAPES.items():
            for column, (typ, nullable, default, collation) in shape["columns"].items():
                if table == "directories" and column == "root_path":
                    collation = root_collation
                if table == "directories" and column == "nopermdirs":
                    typ, default = noperm_type, noperm_default
                rows.append((table, column, typ, nullable, default, collation))
        return rows

    def test_check_schema_accepts_complete_expected_schema(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        configure_schema_cursor(connection.cursor.return_value, self._schema_rows())
        with mock.patch.object(database, "connect", return_value=connection), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(database.check_schema())

    def test_check_schema_accepts_mariadb_bigint_display_width(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        configure_schema_cursor(connection.cursor.return_value, self._schema_rows(
            noperm_type="bigint(20) unsigned"
        ))
        with mock.patch.object(database, "connect", return_value=connection), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(database.check_schema())

    def test_check_schema_reports_missing_tables_and_fields(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchall.return_value = []
        output = io.StringIO()
        with mock.patch.object(database, "connect", return_value=connection), \
             contextlib.redirect_stdout(output):
            self.assertFalse(database.check_schema())
        self.assertIn("Tabelle jobs fehlt", output.getvalue())
        self.assertIn("Tabelle directories fehlt", output.getvalue())

    def test_check_schema_rejects_wrong_root_collation_and_noperm_default(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        configure_schema_cursor(connection.cursor.return_value, self._schema_rows(
            root_collation="utf8mb4_unicode_ci", noperm_default=None
        ))
        output = io.StringIO()
        with mock.patch.object(database, "connect", return_value=connection), \
             contextlib.redirect_stdout(output):
            self.assertFalse(database.check_schema())
        self.assertIn("Collation utf8mb4_bin", output.getvalue())
        self.assertIn("nopermdirs: falscher Standardwert", output.getvalue())

    def test_check_schema_rejects_old_path_hash_width_without_migrating(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        rows = self._schema_rows()
        rows = [
            (table, column, "char(64)" if column == "path_hash" else kind,
             nullable, default, collation)
            for table, column, kind, nullable, default, collation in rows
        ]
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        configure_schema_cursor(connection.cursor.return_value, rows)
        output = io.StringIO()
        with mock.patch.object(database, "connect", return_value=connection), \
             contextlib.redirect_stdout(output):
            self.assertFalse(database.check_schema())
        self.assertIn("path_hash: erwartet CHAR(16)", output.getvalue())

    def test_init_alldirs_preserves_existing_table_without_migration(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchone.side_effect = [
            ("utf8mb4_unicode_ci",), *[(1,)] * 100
        ]
        with mock.patch.object(database, "connect", return_value=connection), \
             mock.patch("builtins.input", return_value="n"):
            self.assertFalse(database.init_alldirs())
        statements = [
            call.args[0] for call in connection.cursor.return_value.execute.call_args_list
        ]
        self.assertFalse(any(sql.startswith("DROP TABLE") for sql in statements))
        self.assertTrue(
            any(sql.strip().startswith("CREATE TABLE IF NOT EXISTS directories") for sql in statements)
        )
        self.assertTrue(any("COLLATE utf8mb4_bin" in sql for sql in statements))
        self.assertFalse(any("ALTER TABLE" in sql for sql in statements))

    def test_init_alldirs_drops_first_when_confirmed(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchone.side_effect = [
            ("utf8mb4_bin",), *[(1,)] * 100
        ]
        with mock.patch.object(database, "connect", return_value=connection), \
             mock.patch("builtins.input", return_value="y"):
            self.assertTrue(database.init_alldirs())
        statements = [
            call.args[0] for call in connection.cursor.return_value.execute.call_args_list
        ]
        self.assertTrue(statements[1].startswith("DROP TABLE IF EXISTS directories"))
        self.assertIn("CREATE TABLE IF NOT EXISTS directories", statements[2])

    def test_init_alldirs_creates_missing_table_without_question(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = 'testdb'
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (0,)
        with mock.patch.object(database, 'connect', return_value=connection), \
             mock.patch('builtins.input') as question, contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(database.init_alldirs())
        question.assert_not_called()
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertFalse(any(sql.startswith('DROP TABLE') for sql in statements))
        self.assertTrue(any('CREATE TABLE IF NOT EXISTS directories' in sql for sql in statements))
        connection.commit.assert_called_once()

    def test_init_alldirs_asks_exact_question_only_for_existing_table(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = 'testdb'
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchone.return_value = (1,)
        with mock.patch.object(database, 'connect', return_value=connection), \
             mock.patch('builtins.input', return_value='n') as question, \
             contextlib.redirect_stdout(io.StringIO()):
            database.init_alldirs()
        question.assert_called_once_with(
            'Wollen sie die Tabelle directories wirklich löschen und neu anlegen? [y/n] '
        )

    def test_init_alldirs_does_not_migrate_missing_hostid(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchone.side_effect = [
            ("utf8mb4_bin",), *[(1,)] * 12, (0,), *[(1,)] * 5
        ]
        with mock.patch.object(database, "connect", return_value=connection), \
             mock.patch("builtins.input", return_value="n"):
            database.init_alldirs()
        statements = [
            call.args[0] for call in connection.cursor.return_value.execute.call_args_list
        ]
        self.assertFalse(any(
            "ADD COLUMN hostid SMALLINT NOT NULL DEFAULT 0" in sql
            for sql in statements
        ))
        self.assertTrue(any(
            "hostid SMALLINT NOT NULL DEFAULT 0" in sql
            and "CREATE TABLE IF NOT EXISTS directories" in sql
            for sql in statements
        ))

    def test_reset_error_job_sets_insert_only_for_error_status(self):
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.rowcount = 1
        with mock.patch.object(
            app.database_connection, "connect", return_value=connection
        ):
            self.assertTrue(app.ApplicationContext.cli_database().reset_error_job(42))
        update_calls = [
            call.args for call in connection.cursor.return_value.execute.call_args_list
            if call.args and "UPDATE jobs" in call.args[0]
        ]
        sql, params = update_calls[0]
        self.assertIn("WHEN task = 'binarycheck' OR stage = 'coarse' THEN 'insert'", sql)
        self.assertIn("stage = CASE WHEN task = 'binarycheck' THEN 'coarse' ELSE stage END", sql)
        self.assertIn("WHERE jobid = %s AND status = 'error'", sql)
        self.assertEqual(params[2], 42)
        connection.commit.assert_called_once_with()

    def test_smart_reset_restricts_update_to_positive_selected_host(self):
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.rowcount = 0
        with mock.patch.object(app.database_connection, 'connect', return_value=connection):
            self.assertFalse(app.ApplicationContext.cli_database().reset_error_job(42, hostid=7))
        sql, params = connection.cursor.return_value.execute.call_args.args
        self.assertIn("WHERE jobid = %s AND status = 'error' AND hostid = %s", sql)
        self.assertEqual(params[2:], (42, 7))
        with self.assertRaises(ValueError):
            app.ApplicationContext.cli_database().reset_error_job(42, hostid=0)

    def test_get_job_uses_parameterized_job_id(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        expected = {"jobid": 19, "status": "pending"}
        connection.cursor.return_value.fetchone.return_value = expected
        with mock.patch.object(database, "connect", return_value=connection):
            result = database.get_job(19)
        self.assertIs(result, expected)
        connection.cursor.return_value.execute.assert_called_once_with(
            mock.ANY, (19,)
        )
        self.assertIn("WHERE jobid = %s", connection.cursor.return_value.execute.call_args.args[0])

    def test_delete_job_commits_and_reports_if_a_row_was_deleted(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.rowcount = 1
        with mock.patch.object(database, "connect", return_value=connection):
            self.assertTrue(database.delete_job(19))
        connection.cursor.return_value.execute.assert_any_call(
            "DELETE FROM jobs WHERE jobid = %s", (19,)
        )
        self.assertIn("WHERE jobid = %s", connection.cursor.return_value.execute.call_args.args[0])
        connection.commit.assert_called_once_with()

    def test_lists_directory_roots_with_counts(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        expected = [("/data", 4)]
        connection.cursor.return_value.fetchall.return_value = expected
        with mock.patch.object(database, "connect", return_value=connection):
            self.assertEqual(database.list_directory_roots(), expected)
        sql = connection.cursor.return_value.execute.call_args.args[0]
        self.assertIn("GROUP BY root_path", sql)
        self.assertNotIn("BINARY", sql)
        self.assertIn("COUNT(*)", sql)

    def test_delete_directory_root_uses_parameter_and_returns_remaining_count(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchone.return_value = (7,)
        with mock.patch.object(database, "connect", return_value=connection):
            self.assertEqual(database.delete_directory_root("/data"), 7)
        calls = connection.cursor.return_value.execute.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].args[1], ("/data",))
        self.assertIn("WHERE root_path = %s", calls[0].args[0])
        self.assertNotIn("BINARY", calls[0].args[0])
        self.assertIn("SELECT COUNT(*)", calls[1].args[0])
        connection.commit.assert_called_once_with()

    def test_drop_all_tables_drops_directories_before_jobs(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchall.return_value = [("tmp0_job_work",), ("tmp7_job_work",), ("jobs",)]
        with mock.patch("builtins.input", return_value="y") as question, \
             mock.patch.object(database, "connect", return_value=connection), \
             mock.patch.object(database, "init_database") as init, \
             mock.patch.object(database, "init_alldirs") as dirs, \
             mock.patch.object(database, "create_hosts_table") as hosts, \
             mock.patch.object(database, "create_double_jobs_table") as doubles, \
             mock.patch.object(database, "create_job_work_table") as queues, \
             contextlib.redirect_stdout(io.StringIO()):
            database.drop_all_tables()
        sql = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list]
        self.assertIn("DROP TABLE IF EXISTS `tmp0_job_work`", sql)
        self.assertIn("DROP TABLE IF EXISTS `tmp7_job_work`", sql)
        self.assertIn("DROP TABLE IF EXISTS hosts", sql)
        self.assertLess(sql.index("DROP TABLE IF EXISTS directories"), sql.index("DROP TABLE IF EXISTS jobs"))
        init.assert_called_once_with()
        dirs.assert_called_once_with(reset=False)
        hosts.assert_called_once_with()
        question.assert_called_once()
        self.assertNotIn("DROP TABLE IF EXISTS job_work", sql)
        queues.assert_not_called()

    def test_add_excluded_names_creates_table_and_inserts_rows(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        with mock.patch.object(database, "connect", return_value=connection):
            database.add_excluded_names([".git", "@eaDir"])
        calls = connection.cursor.return_value.execute.call_args_list
        self.assertIn("CREATE TABLE IF NOT EXISTS excluded_names", calls[0].args[0])
        connection.cursor.return_value.executemany.assert_called_once_with(
            "INSERT IGNORE INTO excluded_names (name) VALUES (%s)",
            [(".git",), ("@eaDir",)],
        )
        connection.commit.assert_called_once_with()

    def test_list_excluded_names_checks_table_before_reading(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (1,)
        cursor.fetchall.return_value = [(".git",), ("@eaDir",)]
        with mock.patch.object(database, "connect", return_value=connection):
            self.assertEqual(database.list_excluded_names(), [".git", "@eaDir"])
        self.assertEqual(cursor.execute.call_count, 2)
        self.assertIn("information_schema.tables", cursor.execute.call_args_list[0].args[0])
        self.assertIn("SELECT name FROM excluded_names", cursor.execute.call_args_list[1].args[0])

    def test_list_excluded_names_returns_none_when_table_is_missing(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchone.return_value = (0,)
        with mock.patch.object(database, "connect", return_value=connection):
            self.assertIsNone(database.list_excluded_names())
        connection.cursor.return_value.execute.assert_called_once()


@unittest.skipUnless(
    os.environ.get("DIRWALKER_DB_INTEGRATION") == "1",
    "Set DIRWALKER_DB_INTEGRATION=1 to run tests against the configured database.",
)
class TestHostidOneDatabaseIntegration(unittest.TestCase):
    def test_hostid_one_job_queue_and_directory_use_one_global_dirid(self):
        database = app.database_connection
        cli = app.DataBaseCLI(database)
        self.assertTrue(cli.check_schema(), "Initialize the current schema before integration tests.")
        hosts = database.list_hosts(1)
        self.assertTrue(hosts, "Integration test requires registered hostid=1 (--setmyhostid).")
        self.assertEqual(hosts[0][7], database.work_table_name(1))

        token = uuid.uuid4().hex
        path = f"/__dirwalker_hostid1_test__/{token}"
        test_userhost = f"hostid1-test-{token}"
        jobid = None
        directory_hash = app._path_hash(path)
        try:
            jobid = app.ApplicationContext.cli_database().insert_job(
                path, hostid=1, userhost=test_userhost
            )
            database.prepare_stage_work(jobid, "coarse", path, hostid=1)

            self.assertIsNone(database.claim_job(
                "insert", hostid=0, userhost=test_userhost
            ))
            claimed = database.claim_job(
                "insert", hostid=1, userhost=test_userhost
            )
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed["jobid"], jobid)
            self.assertEqual(claimed["hostid"], 1)
            self.assertEqual(claimed["userhost"], test_userhost)

            work = {
                "path_hash": directory_hash,
                "path": path,
                "parent_path": None,
                "child_count": 0,
            }
            database.complete_coarse_batch(
                jobid, path,
                [{"work": work, "store": True, "children": []}],
                hostid=1,
            )
            with database.connect() as connection:
                cursor = connection.cursor()
                try:
                    cursor.execute(
                        f"SELECT dirid, hostid, path FROM {database.TABLE_DIRECTORIES} "
                        "WHERE hostid = %s AND path_hash = %s",
                        (1, directory_hash),
                    )
                    directory = cursor.fetchone()
                    self.assertIsNotNone(directory)
                    self.assertEqual(directory[1:], (1, path))
                    cursor.execute(
                        f"SELECT COUNT(*), COUNT(DISTINCT dirid) "
                        f"FROM {database.TABLE_DIRECTORIES} WHERE dirid = %s",
                        (directory[0],),
                    )
                    self.assertEqual(cursor.fetchone(), (1, 1))
                    cursor.execute(
                        f"SELECT DISTINCT hostid FROM {database.work_table_name(1)} "
                        "WHERE jobid = %s",
                        (jobid,),
                    )
                    self.assertEqual(cursor.fetchall(), [(1,)])
                finally:
                    cursor.close()
        finally:
            with database.connect() as connection:
                cursor = connection.cursor()
                try:
                    cursor.execute(
                        f"DELETE FROM {database.TABLE_DIRECTORIES} "
                        "WHERE hostid = %s AND path_hash = %s",
                        (1, directory_hash),
                    )
                    connection.commit()
                finally:
                    cursor.close()
            if jobid is not None:
                cli.delete_job(jobid)


class TestExcludedDirectoriesDatabase(unittest.TestCase):
    def test_live_storage_preserves_remote_paths_and_normalizes_without_resolving(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        with mock.patch.object(database, "connect", return_value=connection), \
             mock.patch.object(app.os.path, "realpath", side_effect=AssertionError("client resolution")):
            database.add_live_excluded_absdirs(["/srv/data/../private/"], 1)
        rows = connection.cursor.return_value.executemany.call_args.args[1]
        self.assertEqual(rows, [(1, app._path_hash("/srv/private"), "/srv/private")])


class TestHostRegistry(unittest.TestCase):
    def database(self):
        database = object.__new__(app.DatabaseConnection)
        database.database = "testdb"
        database.database = "testdb"
        return database

    def connection(self):
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        return connection

    def test_create_hosts_table_contains_all_fields_and_primary_key(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        with mock.patch.object(database, "connect", return_value=connection):
            database.create_hosts_table()
        ddl = next(call.args[0] for call in connection.cursor.return_value.execute.call_args_list if "CREATE TABLE IF NOT EXISTS hosts" in call.args[0])
        for field in ("hostid", "`user`", "homepath", "homedirid", "`system`", "machine", "timecreated"):
            self.assertIn(field, ddl)
        self.assertIn("CREATE TABLE IF NOT EXISTS hosts", ddl)
        self.assertIn("PRIMARY KEY (hostid)", ddl)
        connection.commit.assert_called_once()

    def test_register_assigns_positive_id_and_fills_host_data(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        cursor = connection.cursor.return_value
        cursor.fetchone.side_effect = [(1,), (1,), None, (1,), (0,), (1,)]
        with mock.patch.object(database, "connect", return_value=connection), \
             mock.patch.object(database, "_local_host_values", return_value=("alex", "/home/alex", "Linux", "pc")), \
             mock.patch.object(database, "_host_home_dirid", return_value=42):
            self.assertEqual(database.set_my_hostid(), 1)
        sql, params = next(call.args for call in cursor.execute.call_args_list if call.args[0].startswith("INSERT INTO hosts"))
        self.assertEqual(params[:6], (1, "alex", "/home/alex", 42, "Linux", "pc"))
        self.assertIsInstance(params[6], datetime)
        self.assertNotIn("timecreated =", sql)
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS hosts" in sql for sql in statements))
        self.assertFalse(any("DROP TABLE" in sql for sql in statements))

    def test_repeat_registration_reuses_id(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        connection.cursor.return_value.fetchone.side_effect = [(1,), (1,), (7, "alex"), (0,), (1,)]
        with mock.patch.object(database, "connect", return_value=connection), \
             mock.patch.object(database, "_local_host_values", return_value=("alex", "/home/alex", "Linux", "pc")), \
             mock.patch("builtins.input", return_value="") as question, \
             mock.patch.object(database, "_host_home_dirid", return_value=None):
            self.assertEqual(database.set_my_hostid(), 7)
        question.assert_called_once()
        self.assertIn('[yN]', question.call_args.args[0])
        self.assertFalse(any("MAX(hostid)" in call.args[0] for call in connection.cursor.return_value.execute.call_args_list))

    def test_registration_cannot_write_zero_or_overflow(self):
        for candidate in (0, -1, 32768):
            database = app.DataBaseCLI(self.database())
            connection = self.connection()
            connection.cursor.return_value.fetchone.side_effect = [(1,), (1,), None, (candidate,), (1,)]
            with self.subTest(candidate=candidate), \
                 mock.patch.object(database, "connect", return_value=connection), \
                 self.assertRaises(ValueError):
                database.set_my_hostid()
            self.assertFalse(any(call.args[0].startswith("INSERT INTO hosts") for call in connection.cursor.return_value.execute.call_args_list))
            connection.rollback.assert_called_once()

    def test_serverinit_creates_missing_host_schema_and_upserts_zero_without_deleting(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        connection.cursor.return_value.fetchone.side_effect = [(1,), (1,), (1,)]
        with mock.patch.object(database, '_show_statement_timeout_hint'), \
             mock.patch.object(database, 'connect', return_value=connection), \
             mock.patch.object(database, '_host_home_dirid', return_value=None):
            with mock.patch.object(database, 'ensure_jobs_constraints'):
                self.assertEqual(database.serverinit(), 0)
        calls = connection.cursor.return_value.execute.call_args_list
        statements = [call.args[0] for call in calls]
        self.assertTrue(any('CREATE TABLE IF NOT EXISTS hosts' in sql for sql in statements))
        insert = next(call for call in calls if call.args[0].startswith('INSERT INTO hosts'))
        self.assertEqual(insert.args[1][0], 0)
        self.assertTrue(all(not sql.lstrip().startswith(('DELETE ', 'DROP ', 'TRUNCATE ')) for sql in statements))

    def test_home_directory_lookup_is_host_scoped_and_optional(self):
        database = app.DataBaseCLI(self.database())
        cursor = mock.Mock()
        cursor.fetchone.side_effect = [(1,), (55,)]
        self.assertEqual(database._host_home_dirid(cursor, 7, "/home/alex"), 55)
        self.assertEqual(cursor.execute.call_args.args[1], (7, app._path_hash("/home/alex"), "/home/alex"))
        cursor.reset_mock()
        cursor.fetchone.side_effect = [(0,)]
        self.assertIsNone(database._host_home_dirid(cursor, 7, "/home/alex"))
        self.assertEqual(cursor.execute.call_count, 1)

    def test_check_requires_reserved_zero(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        cursor = connection.cursor.return_value
        configure_schema_cursor(cursor, TestDatabaseConnection()._schema_rows(), hosts=[])
        output = io.StringIO()
        with mock.patch.object(database, "connect", return_value=connection), contextlib.redirect_stdout(output):
            self.assertFalse(database.check_schema())
        self.assertIn("hostid=0", output.getvalue())
        configure_schema_cursor(cursor, TestDatabaseConnection()._schema_rows())
        with mock.patch.object(database, "connect", return_value=connection), contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(database.check_schema())

    def test_check_reports_missing_hosts_table(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        configure_schema_cursor(connection.cursor.return_value, [row for row in TestDatabaseConnection()._schema_rows() if row[0] != "hosts"])
        output = io.StringIO()
        with mock.patch.object(database, "connect", return_value=connection), contextlib.redirect_stdout(output):
            self.assertFalse(database.check_schema())
        self.assertIn("Tabelle hosts fehlt", output.getvalue())


class TestHostWorkTables(unittest.TestCase):
    def database(self):
        database = object.__new__(app.DatabaseConnection)
        database.database = "testdb"
        return database

    def connection(self):
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        return connection

    def test_droptmp_drops_host_queues_and_both_result_tables(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        connection.cursor.return_value.fetchall.return_value = [
            ("tmp0_job_work",), ("tmp7_job_work",), ("tmp0008_job_work",),
            ("tmp_job_work",), ("tmp7_job_work_backup",), ("hosts",), ("jobs",),
            ("tmp_deepdoubles_result",), ("tmp_doubles_letchecked",),
            ("tmp_deepdoubles_candidates",), ("mp_deepdoubles_candidates",),
        ]
        with mock.patch.object(database, "connect", return_value=connection):
            self.assertEqual(set(database.droptmp()), {"tmp0_job_work", "tmp7_job_work", "tmp0008_job_work", "tmp_deepdoubles_result", "tmp_doubles_letchecked", "tmp_deepdoubles_candidates", "mp_deepdoubles_candidates"})
        drops = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list if call.args[0].startswith("DROP")]
        self.assertEqual(set(drops), {"DROP TABLE IF EXISTS `tmp0_job_work`", "DROP TABLE IF EXISTS `tmp7_job_work`", "DROP TABLE IF EXISTS `tmp0008_job_work`", "DROP TABLE IF EXISTS `tmp_deepdoubles_result`", "DROP TABLE IF EXISTS `tmp_doubles_letchecked`", "DROP TABLE IF EXISTS `tmp_deepdoubles_candidates`", "DROP TABLE IF EXISTS `mp_deepdoubles_candidates`"})

    def test_queue_creation_migrates_only_selected_host(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        connection.cursor.return_value.fetchone.return_value = (1,)
        with mock.patch.object(database, "connect", return_value=connection):
            self.assertEqual(database.create_job_work_table(7), "tmp7_job_work")
        calls = connection.cursor.return_value.execute.call_args_list
        self.assertTrue(any("CREATE TABLE IF NOT EXISTS tmp7_job_work" in call.args[0] for call in calls))
        copy = next(call for call in calls if "SELECT jobid, stage" in call.args[0])
        self.assertIn("INSERT IGNORE INTO tmp7_job_work", copy.args[0])
        self.assertIn("FROM job_work WHERE hostid = %s", copy.args[0])
        self.assertEqual(copy.args[1], (7,))
        connection.cursor.return_value.execute.assert_any_call("DELETE FROM job_work WHERE hostid = %s", (7,))

    def test_registered_table_name_is_written_to_hosts(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        connection.cursor.return_value.fetchone.side_effect = [(1,), (1,), (7, "alex"), (1,)]
        with mock.patch.object(database, "connect", return_value=connection), \
             mock.patch.object(database, "_local_host_values", return_value=("alex", "/home/alex", "Linux", "pc")), \
             mock.patch("builtins.input", return_value="n"), \
             mock.patch.object(database, "_host_home_dirid", return_value=None), \
             mock.patch.object(database, "create_job_work_table", return_value="tmp7_job_work") as create:
            self.assertEqual(database.set_my_hostid(), 7)
        create.assert_called_once_with(7, connection.cursor.return_value)
        insert = next(call for call in connection.cursor.return_value.execute.call_args_list if call.args[0].startswith("INSERT INTO hosts"))
        self.assertIn("tabellenname", insert.args[0])
        self.assertEqual(insert.args[1][-1], "tmp7_job_work")

    def test_check_rejects_wrong_server_table_and_missing_registered_queue(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        rows = TestDatabaseConnection()._schema_rows()
        for hosts in ([(0, "wrong")], [(0, "tmp0_job_work"), (7, "tmp7_job_work")]):
            with self.subTest(hosts=hosts):
                configure_schema_cursor(connection.cursor.return_value, rows, hosts)
                output = io.StringIO()
                with mock.patch.object(database, "connect", return_value=connection), contextlib.redirect_stdout(output):
                    self.assertFalse(database.check_schema())
                self.assertIn("tmp0_job_work" if len(hosts) == 1 else "tmp7_job_work fehlt", output.getvalue())

    def test_check_validates_every_regular_table(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        for table, shape in database.EXPECTED_TABLE_SHAPES.items():
            column = next(iter(shape["columns"]))
            rows = [(t, c, "varchar(1)" if (t, c) == (table, column) else typ, nullable, default, coll)
                    for t, c, typ, nullable, default, coll in TestDatabaseConnection()._schema_rows()]
            configure_schema_cursor(connection.cursor.return_value, rows)
            output = io.StringIO()
            with self.subTest(table=table), mock.patch.object(database, "connect", return_value=connection), contextlib.redirect_stdout(output):
                self.assertFalse(database.check_schema())
            self.assertIn(f"{table}.{column}", output.getvalue())

    def test_check_reports_empty_table_name_without_deriving_a_work_table(self):
        for value in (None, '', '   '):
            with self.subTest(value=value):
                database = app.DataBaseCLI(self.database())
                connection = self.connection()
                configure_schema_cursor(connection.cursor.return_value,
                                        TestDatabaseConnection()._schema_rows(),
                                        hosts=[(0,'tmp0_job_work'),(7,value)])
                output = io.StringIO()
                with mock.patch.object(database, 'connect', return_value=connection), \
                        contextlib.redirect_stdout(output):
                    self.assertFalse(database.check_schema())
                self.assertIn('hosts.hostid=7: tabellenname ist leer', output.getvalue())
                self.assertNotIn('tmp7_job_work fehlt', output.getvalue())

    def test_missing_table_name_column_is_an_error_without_null_fallback(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        rows = [row for row in TestDatabaseConnection()._schema_rows()
                if not (row[0] == 'hosts' and row[1] == 'tabellenname')]
        configure_schema_cursor(connection.cursor.return_value, rows)
        output = io.StringIO()
        with mock.patch.object(database, 'connect', return_value=connection), \
                contextlib.redirect_stdout(output):
            self.assertFalse(database.check_schema())
        self.assertIn('hosts.tabellenname fehlt', output.getvalue())
        self.assertFalse(any('NULL AS tabellenname' in call.args[0]
                             for call in connection.cursor.return_value.execute.call_args_list))

    def test_check_validates_unregistered_numeric_tmp_tables_too(self):
        database = app.DataBaseCLI(self.database())
        connection = self.connection()
        rows = TestDatabaseConnection()._schema_rows() + [("tmp77_job_work", "jobid", "text", "YES", None, None)]
        configure_schema_cursor(connection.cursor.return_value, rows)
        output = io.StringIO()
        with mock.patch.object(database, "connect", return_value=connection), contextlib.redirect_stdout(output):
            self.assertFalse(database.check_schema())
        self.assertIn("tmp77_job_work", output.getvalue())

    def test_serverinit_propagates_registration_failure(self):
        database = app.DataBaseCLI(self.database())
        with mock.patch.object(database, '_show_statement_timeout_hint'), \
             mock.patch.object(database, '_write_local_host', side_effect=RuntimeError('registration failed')), \
             mock.patch.object(database, 'ensure_jobs_constraints'), \
             self.assertRaisesRegex(RuntimeError, 'registration failed'):
            database.serverinit()


class TestDropAllQuestion(unittest.TestCase):
    def test_no_and_default_preserve_core_and_legacy_tables_with_one_question(self):
        for answer in ("", "n", "N"):
            with self.subTest(answer=answer):
                database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
                database.database = "testdb"
                connection = mock.MagicMock()
                connection.__enter__.return_value = connection
                connection.cursor.return_value.fetchall.return_value = [("tmp0_job_work",), ("tmp7_job_work",), ("job_work",)]
                with mock.patch("builtins.input", return_value=answer) as question, \
                     mock.patch.object(database, "connect", return_value=connection), \
                     mock.patch.object(database, "init_database") as init, \
                     mock.patch.object(database, "init_alldirs") as dirs, \
                     mock.patch.object(database, "create_hosts_table") as hosts, \
                     contextlib.redirect_stdout(io.StringIO()):
                    database.drop_all_tables()
                question.assert_called_once()
                sql = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list]
                for table in ("directories", "jobs", "hosts", "jobsrunid", "job_work", "tmp_deepdoubles_result"):
                    self.assertNotIn(f"DROP TABLE IF EXISTS {table}", sql)
                for table in ("`tmp0_job_work`", "`tmp7_job_work`", "`tmp_doubles_letchecked`", "excluded_names", "live_excluded_absdirs"):
                    self.assertIn(f"DROP TABLE IF EXISTS {table}", sql)
                init.assert_called_once_with()
                dirs.assert_not_called()
                hosts.assert_not_called()


class DeepDoubleTests(unittest.TestCase):
    def test_multiple_levels_and_cross_host_candidates(self):
        nodes = [(1, 3, 0, 'a', '/a'), (2, 4, 0, 'a', '/b'),
                 (3, 5, 0, 'b', '/c'), (4, 6, 0, 'b', '/d'),
                 (5, 0, 0, 'c', '/e'), (6, 0, 0, 'c', '/f'),
                 (7, 0, 1, 'a', '/otherhost')]
        rows = DataBaseCLI._deep_double_levels(nodes, {(0, 'b'), (0, 'c')})
        self.assertEqual(rows, [(0, 1, 0, 2, 2, '/a', '/b'),
                                (0, 1, 1, 7, 0, '/a', '/otherhost'),
                                (0, 2, 1, 7, 0, '/b', '/otherhost')])

    def test_cycles_shared_parent_null_hash_and_groups_of_three(self):
        nodes = [(1, 1, 0, 'a', 'a'), (2, 2, 0, 'a', 'b'),
                 (3, 1, 0, 'a', 'c'), (4, 0, 0, None, 'd')]
        rows = DataBaseCLI._deep_double_levels(nodes, set())
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][4], 0)
        self.assertEqual(rows[1][4], 0)

    def test_cross_host_parent_chains_with_overlapping_ids(self):
        nodes = [(1, 2, 10, 'leaf', '/a/leaf'),
                 (2, 3, 10, 'parent', '/a'),
                 (3, 0, 10, 'root', '/'),
                 (1, 2, 20, 'leaf', '/b/leaf'),
                 (2, 3, 20, 'parent', '/b'),
                 (3, 0, 20, 'root', '/')]
        rows = DataBaseCLI._deep_double_levels(nodes, set())
        leaf = next(row for row in rows if row[:4] == (10, 1, 20, 1))
        self.assertEqual(leaf[4], 2)
        # A mismatch on one host stops ascent without following the other host.
        nodes[-1] = (3, 0, 20, 'different-root', '/')
        rows = DataBaseCLI._deep_double_levels(nodes, set())
        leaf = next(row for row in rows if row[:4] == (10, 1, 20, 1))
        self.assertEqual(leaf[4], 1)

    def test_final_hash_exclusions_remain_host_specific(self):
        nodes = [(1, 0, 10, 'a', '/a'), (2, 0, 20, 'a', '/b'),
                 (3, 0, 30, 'a', '/c')]
        self.assertEqual(DataBaseCLI._deep_double_levels(nodes, {(10, 'a')}),
                         [(20, 2, 30, 3, 0, '/b', '/c')])

    def database(self, states):
        db = DataBaseCLI(MagicMock())
        connection = db.connection.connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value
        cursor.fetchall.side_effect = [states, [], [(1, 0, 0, 'a', '/a'), (2, 0, 0, 'a', '/b')]]
        return db, connection, cursor

    def test_missing_batch_does_not_build_candidates(self):
        db, connection, cursor = self.database([])
        with self.assertRaisesRegex(RuntimeError, 'Kein Binarycheck-Batch'):
            db.list_deep_double_levels()
        self.assertEqual(cursor.execute.call_count, 3)
        cursor.executemany.assert_not_called()
        connection.rollback.assert_called_once()
        cursor.close.assert_called_once()

    def test_partial_batches_use_available_positive_results(self):
        states = [[(1, 0, 'closed', 0, 0)], [(2, 0, 'closed', 1990, 1757)],
                  [(1, 0, 'active', 2, 1)], [(1, 0, 'error', 2, 1)],
                  [(1, 0, 'closed', 2, 2), (2, 1, 'paused', 2, 1)]]
        for state in states:
            with self.subTest(state=state):
                db, connection, cursor = self.database(state)
                with mock.patch.object(db, '_store_deep_double_results') as store:
                    rows = db.list_deep_double_levels()
                self.assertEqual(rows, [(0, 1, 0, 2, 0, '/a', '/b')])
                self.assertIn('Liste kann unvollständig sein', db.deep_double_analysis_summary)
                for jobid, hostid, status, total, completed in state:
                    self.assertIn(f'{completed}/{total}', db.deep_double_analysis_summary)
                self.assertEqual(store.call_args.args[0], min(row[0] for row in state))
                connection.rollback.assert_called_once()
                cursor.close.assert_called_once()

    def test_partial_node_query_excludes_nonpositive_and_other_batch_rows(self):
        db, connection, cursor = self.database([(2, 0, 'closed', 1990, 1757)])
        db.TABLE_DIRECTORIES = 'directories'
        db.TABLE_DOUBLE_JOBS = 'tmp_doubles_letchecked'
        with mock.patch.object(db, '_store_deep_double_results'):
            db.list_deep_double_levels()
        selection = next(call for call in cursor.execute.call_args_list
                         if 'SELECT d.dirid, d.parentdir' in call.args[0])
        with sqlite3.connect(':memory:') as sql:
            sql.execute('CREATE TABLE directories (dirid, parentdir, hostid, combined_sha256, path)')
            sql.execute('CREATE TABLE tmp_doubles_letchecked (jobid, hostid, left_dirid, right_dirid, status, is_requested, hostidB INTEGER DEFAULT 0)')
            sql.executemany('INSERT INTO directories VALUES (?,?,?,?,?)',
                            [(n, 0, 0, 'hash', '/'+str(n)) for n in range(1, 13)]
                            + [(21, 0, 1, 'hash', '/21'), (22, 0, 1, 'hash', '/22')])
            sql.executemany('INSERT INTO tmp_doubles_letchecked (jobid,hostid,left_dirid,right_dirid,status,is_requested) VALUES (?,?,?,?,?,?)', [
                (2, 0, 1, 2, 'equal', 1),
                (2, 0, 3, 4, 'unreadable', 1),
                (2, 0, 5, 6, 'pending', 1),
                (2, 0, 7, 8, 'different', 1),
                (1, 0, 9, 10, 'equal', 1),
                (2, 0, 11, 12, 'equal', 0),
                (3, 1, 21, 22, 'equal', 1),
            ])
            sql.execute('ALTER TABLE directories ADD COLUMN zero_content_only INTEGER DEFAULT 0')
            sql.execute('UPDATE directories SET zero_content_only=1 WHERE dirid=11')
            nodes = sql.execute(selection.args[0].replace('%s', '?'), selection.args[1]).fetchall()
        self.assertEqual({row[0] for row in nodes}, {1, 2, 12})

    def test_complete_batch_builds_candidates(self):
        db, connection, cursor = self.database([(1, 0, 'closed', 2, 2)])
        with mock.patch.object(db, '_store_deep_double_results') as store:
            self.assertEqual(db.list_deep_double_levels(), [(0, 1, 0, 2, 0, '/a', '/b')])
        store.assert_called_once_with(
            1, [(1, 0, 0, 'a', '/a'), (2, 0, 0, 'a', '/b')],
            [(0, 1, 0, 2, 0, '/a', '/b')],
        )
        cursor.executemany.assert_called_once()
        connection.commit.assert_not_called()
        connection.rollback.assert_called_once()

    def test_requested_job_selects_its_batch_and_stores_canonical_id(self):
        db, connection, cursor = self.database([(3, 0, 'closed', 2, 2), (4, 1, 'closed', 2, 2)])
        with mock.patch.object(db, '_store_deep_double_results') as store:
            db.list_deep_double_levels(4)
        self.assertEqual(cursor.execute.call_args_list[2].args[1], (4,))
        self.assertEqual(store.call_args.args[0], 3)


class TestDeepDoubleResults(unittest.TestCase):
    def database(self):
        db = DataBaseCLI(mock.MagicMock())
        db.TABLE_JOBS = 'jobs'
        db.TABLE_DIRECTORIES = 'directories'
        connection = db.connection.connect.return_value.__enter__.return_value
        connection.cursor.return_value.fetchone.return_value = (3,)
        return db, connection, connection.cursor.return_value

    def test_store_aggregates_both_hosts_and_replaces_only_one_batch(self):
        db, connection, cursor = self.database()
        nodes = [(9, 0, 0, 'hash', '/a'), (2, 0, 1, 'hash', '/b'),
                 (8, 0, 2, 'hash', '/c')]
        pairs = [(0, 9, 1, 2, 1, '/a', '/b'), (0, 9, 2, 8, 3, '/a', '/c')]
        db._store_deep_double_results(3, nodes, pairs)
        self.assertEqual(cursor.executemany.call_args.args[1],
                         [(3, 0, 9, 'hash', 3), (3, 1, 2, 'hash', 1), (3, 2, 8, 'hash', 3)])
        statements = [(call.args[0], call.args[1:] ) for call in cursor.execute.call_args_list]
        self.assertTrue(any('DELETE FROM tmp_deepdoubles_result WHERE jobid = %s' in sql
                            and params == ((3,),) for sql, params in statements))
        self.assertTrue(any('FOR UPDATE' in sql for sql, _ in statements))
        connection.commit.assert_called_once()

    def test_empty_rebuild_clears_old_batch_results(self):
        db, connection, cursor = self.database()
        db._store_deep_double_results(3, [], [])
        cursor.executemany.assert_not_called()
        self.assertTrue(any('DELETE FROM tmp_deepdoubles_result WHERE jobid = %s' in call.args[0]
                            for call in cursor.execute.call_args_list))
        connection.commit.assert_called_once()

    def test_failed_rebuild_rolls_back(self):
        db, connection, cursor = self.database()
        cursor.executemany.side_effect = RuntimeError('write failed')
        with self.assertRaises(RuntimeError):
            db._store_deep_double_results(3, [(1, 0, 0, 'h', '/a'), (2, 0, 0, 'h', '/b')],
                                         [(0, 1, 0, 2, 1, '/a', '/b')])
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()

    def test_report_sql_selects_all_positive_levels_across_hosts(self):
        db, connection, _cursor = self.database()
        with sqlite3.connect(':memory:') as sql:
            sql.execute('CREATE TABLE tmp_deepdoubles_result (jobid, hostid, dirid, combined_sha256, level)')
            sql.execute('CREATE TABLE directories (hostid, dirid, path, combined_sha256, subtree_structure, subtree_size_bytes, localfilecount, subdircount)')
            results = [(3, 0, 1, 'h', 0), (3, 0, 12, 'h', 2), (3, 1, 7, 'h', 1),
                       (3, 0, 20, 'other', 1), (3, 1, 21, 'other', 2),
                       (3, 0, 30, 'zero', 0), (99, 1, 2, 'h', 4)]
            sql.executemany('INSERT INTO tmp_deepdoubles_result VALUES (?,?,?,?,?)', results)
            sql.executemany('INSERT INTO directories VALUES (?,?,?,?,?,?,?,?)',
                            [(host, dirid, '/'+str(dirid), hash_, '0', 100, 1, 0)
                             for _, host, dirid, hash_, _ in results])
            sql.execute('ALTER TABLE directories ADD COLUMN zero_content_only INTEGER DEFAULT 0')
            sql.execute('UPDATE directories SET zero_content_only=1 WHERE dirid=12')
            sqlite_cursor = sql.cursor()
            class Cursor:
                def execute(self, statement, params):
                    sqlite_cursor.execute(statement.replace('%s', '?'), params)
                def fetchall(self):
                    return sqlite_cursor.fetchall()
                def close(self):
                    sqlite_cursor.close()
            connection.cursor.return_value = Cursor()
            with mock.patch.object(db, 'zero_content_directory_ids', return_value=set()), \
                 mock.patch.object(db, 'list_deep_double_levels') as rebuild, \
                 mock.patch.object(db, 'get_double_job_batch', return_value=[(3, 0), (4, 1)]):
                rows = db.list_deep_double_tree_rows(4)
            rebuild.assert_called_once_with(4)
            self.assertEqual([row[0] for row in rows], [7, 20, 21])
            self.assertEqual({row[0]: row[12] for row in rows}, {7: 1, 20: 0, 21: 1})
            self.assertTrue(all(row[0] == row[4] for row in rows))
            self.assertEqual(next(row[12] for row in rows if row[0] == 7), 1)


class TestDoubles(unittest.TestCase):
    def test_create_binarycheck_job_builds_comparison_rows(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.TABLE_JOBS = app.DatabaseConnection.TABLE_JOBS
        database.TABLE_DOUBLE_JOBS = app.DatabaseConnection.TABLE_DOUBLE_JOBS
        database.database = "testdb"
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.lastrowid = 88
        cursor.fetchone.return_value = (1,)
        cursor.fetchall.return_value = []
        rows = [
            (1, "/a", "0", 10, "a" * 64, 0),
            (2, "/b", "0", 10, "a" * 64, 0),
            (3, "/c", "0", 10, "a" * 64, 0),
        ]
        with mock.patch.object(
            database, "list_scoped_duplicate_roots", return_value=rows
        ), mock.patch.object(database, "list_directory_hostids", return_value=[0]), mock.patch.object(database, "connect", return_value=connection):
            self.assertEqual(database.create_binarycheck_job(), (88, 2))
        self.assertTrue(any(
            "CREATE TABLE IF NOT EXISTS `tmp_doubles_letchecked`" in call.args[0]
            for call in cursor.execute.call_args_list
        ))
        self.assertTrue(any(
            "binarycheck" in call.args[0]
            for call in cursor.execute.call_args_list
            if "INSERT INTO jobs" in call.args[0]
        ))
        self.assertEqual(
            cursor.executemany.call_args.args[1],
            [(88, 0, 0, 1, 2), (88, 0, 0, 1, 3)],
        )

    def test_double_job_progress_query_can_select_a_specific_job(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.TABLE_JOBS = app.DatabaseConnection.TABLE_JOBS
        database.TABLE_DOUBLE_JOBS = app.DatabaseConnection.TABLE_DOUBLE_JOBS
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.side_effect = [(34, "active"), (10, 4)]
        with mock.patch.object(database, "connect", return_value=connection):
            self.assertEqual(database.get_double_job_progress(34), (34, "active", 10, 4))
        job_sql, job_params = cursor.execute.call_args_list[0].args
        progress_sql, progress_params = cursor.execute.call_args_list[1].args
        self.assertIn("task = 'binarycheck'", job_sql)
        self.assertIn("jobid = %s", job_sql)
        self.assertEqual(job_params, (34,))
        self.assertIn("FROM `tmp_doubles_letchecked`", progress_sql)
        self.assertIn("is_requested = TRUE", progress_sql)
        self.assertIn("'equal', 'different', 'skipped_empty'", progress_sql)
        self.assertEqual(progress_params, (34,))

    def test_double_job_progress_returns_none_without_binarycheck_before_table_query(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.TABLE_JOBS = app.DatabaseConnection.TABLE_JOBS
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchone.return_value = None
        with mock.patch.object(database, "connect", return_value=connection):
            self.assertIsNone(database.get_double_job_progress())
        connection.cursor.return_value.execute.assert_called_once()
        self.assertIn(
            "task = 'binarycheck'",
            connection.cursor.return_value.execute.call_args.args[0],
        )

    def test_duplicate_query_keeps_ancestors_and_omits_nested_duplicates(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        with sqlite3.connect(":memory:") as sql:
            sql.execute("""CREATE TABLE directories (
                dirid INTEGER, parentdir INTEGER, hostid INTEGER, path TEXT,
                combined_sha256 TEXT, subtree_structure TEXT,
                subtree_size_bytes INTEGER, localfilecount INTEGER, subdircount INTEGER
            )""")
            # Duplicate children lie below duplicate roots, through unique parents.
            rows = [
                (1, 0, 0, '/a', 'root', 'tree', 20, 0, 1),
                (2, 0, 0, '/b', 'root', 'tree', 20, 0, 1),
                (3, 1, 0, '/a/mid', 'unique-a', 'mid', 10, 0, 1),
                (4, 2, 0, '/b/mid', 'unique-b', 'mid', 10, 0, 1),
                (5, 3, 0, '/a/mid/child', 'child', 'leaf', 10, 1, 0),
                (6, 4, 0, '/b/mid/child', 'child', 'leaf', 10, 1, 0),
                (7, 0, 0, '/empty1', 'empty', 'leaf', 0, 0, 0),
                (8, 0, 0, '/empty2', 'empty', 'leaf', 0, 0, 0),
                (9, 0, 1, '/other-host', 'child', 'leaf', 10, 1, 0),
                (10, 0, 0, '/standalone1', 'standalone', 'leaf', 10, 1, 0),
                (11, 0, 0, '/standalone2', 'standalone', 'leaf', 10, 1, 0),
                (12, 0, 0, '/zeros1', 'zeros', 'nested', 0, 1, 1),
                (13, 12, 0, '/zeros1/child', 'zero-leaf', 'leaf', 0, 1, 0),
                (14, 0, 0, '/zeros2', 'zeros', 'nested', 0, 1, 1),
                (15, 14, 0, '/zeros2/child', 'zero-leaf', 'leaf', 0, 1, 0),
            ]
            sql.executemany("INSERT INTO directories VALUES (?,?,?,?,?,?,?,?,?)", rows)
            sql.execute('ALTER TABLE directories ADD COLUMN zero_content_only INTEGER DEFAULT 0')
            sql.execute('UPDATE directories SET zero_content_only=1 WHERE dirid IN (7,8,12,13,14,15)')
            connection = mock.MagicMock()
            connection.__enter__.return_value = connection
            connection.cursor.return_value = sql.cursor()
            with mock.patch.object(database, 'zero_content_directory_ids', return_value=
                    app.DataBaseConnection._zero_content_tree_ids([
                        (r[0],r[1],r[2],r[7],r[8],0,r[7] if r[6] == 0 else 0,0,
                         0 if r[7] == 0 else r[6]) for r in rows])), \
                 mock.patch.object(database, "connect", return_value=connection):
                result = database.list_duplicate_subtree_roots()
            self.assertEqual({row[0] for row in result}, {1, 2, 9, 10, 11})


class TestServerOperations(unittest.TestCase):
    def test_restart_stopped_job_creates_new_row_with_task_status(self):
        database = app.DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = "testdb"
        database.database = "testdb"
        for task, saved_stage, expected_status in (
            ("scan", "coarse", "insert"),
            ("check", "detailed", "pending"),
            ("verify", "detailed", "pending"),
        ):
            with self.subTest(task=task):
                connection = mock.MagicMock()
                connection.__enter__.return_value = connection
                cursor = connection.cursor.return_value
                cursor.fetchone.return_value = {
                    "startdirname": "/data",
                    "task": task,
                    "status": "stopped",
                    "stage": saved_stage,
                    "skipdirs": "cache,tmp",
                }
                cursor.lastrowid = 91
                with mock.patch.object(database, "connect", return_value=connection):
                    result = database.restart_stopped_job(7)
                self.assertEqual(
                    result,
                    {
                        "jobid": 91, "task": task, "status": expected_status,
                        "stage": saved_stage,
                    },
                )
                statements = [call.args[0] for call in cursor.execute.call_args_list]
                self.assertTrue(any(statement.lstrip().startswith("INSERT INTO") for statement in statements))
                copied_work = next(
                    statement for statement in statements
                    if "SELECT %s, stage, hostid, path_hash" in statement
                )
                self.assertIn("child_count", copied_work)
                insert_statement = next(
                    statement for statement in statements
                    if statement.lstrip().startswith("INSERT INTO")
                )
                self.assertIn("skipdirs", insert_statement)
                self.assertFalse(any(statement.lstrip().startswith("UPDATE") for statement in statements))


class TestShowTables(unittest.TestCase):
    def database(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = 'filestatus'
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        return database, connection, connection.cursor.return_value

    def test_lists_tables_in_configured_database(self):
        database, connection, cursor = self.database()
        cursor.fetchall.return_value = [('directories',), ('jobs',)]
        with mock.patch.object(database, 'connect', return_value=connection):
            self.assertEqual(database.list_tables(), ['directories', 'jobs'])
        self.assertEqual(cursor.execute.call_args.args[1], ('filestatus',))
        cursor.close.assert_called_once()

    def test_unknown_table_never_becomes_sql(self):
        database, connection, cursor = self.database()
        cursor.fetchone.return_value = None
        name = 'not_a_table`; DROP TABLE jobs; --'
        with mock.patch.object(database, 'connect', return_value=connection), self.assertRaises(ValueError):
            database.table_preview(name)
        cursor.execute.assert_called_once()
        self.assertEqual(cursor.execute.call_args.args[1], ('filestatus', name))
        cursor.close.assert_called_once()

    def test_catalog_identifier_is_quoted(self):
        database, connection, cursor = self.database()
        cursor.fetchone.side_effect = [('odd`name',), (2,)]
        cursor.fetchall.return_value = [(1,), (2,)]
        with mock.patch.object(database, 'connect', return_value=connection):
            self.assertEqual(database.table_preview('odd`name'), ([(1,), (2,)], 2))
        self.assertEqual(cursor.execute.call_args_list[-1].args[0], 'SELECT * FROM `odd``name` LIMIT 40')

    def test_preview_limits_real_rows_without_limiting_count(self):
        database, connection, _cursor = self.database()
        with sqlite3.connect(':memory:') as sql:
            sql.execute('CREATE TABLE sample (id INTEGER, value TEXT)')
            sql.executemany('INSERT INTO sample VALUES (?,?)', [(n, str(n)) for n in range(100)])
            inner = sql.cursor()
            class Cursor:
                def execute(self, statement, params=()):
                    if 'information_schema.tables' in statement:
                        inner.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", (params[1],))
                    else:
                        inner.execute(statement, params)
                def fetchone(self): return inner.fetchone()
                def fetchall(self): return inner.fetchall()
                def close(self): inner.close()
            connection.cursor.return_value = Cursor()
            with mock.patch.object(database, 'connect', return_value=connection):
                rows, total = database.table_preview('sample')
            self.assertEqual(len(rows), 40)
            self.assertEqual(total, 100)


class TestStaleJobs(unittest.TestCase):
    def test_new_jobs_schema_accepts_stale(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = 'testdb'
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        with mock.patch.object(database, 'connect', return_value=connection), \
             mock.patch.object(database, 'connect_server', return_value=connection), \
             contextlib.redirect_stdout(io.StringIO()):
            database.init_database()
        ddl = next(call.args[0] for call in connection.cursor.return_value.execute.call_args_list
                   if 'CREATE TABLE IF NOT EXISTS jobs (' in call.args[0])
        self.assertIn("'stale'", ddl.split('CONSTRAINT chk_job_status CHECK (')[1].split(')')[0])

    def test_serverinit_preserves_jobs_queues_results_and_history(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        with mock.patch.object(database, '_show_statement_timeout_hint') as timeout, \
             mock.patch.object(database, '_write_local_host', return_value=0) as host, \
             mock.patch.object(database, '_mark_active_jobs_stale') as stale, \
             mock.patch.object(database, '_reset_binarycheck_results') as results, \
             mock.patch.object(database, '_reset_host_work_tables') as queues, \
             mock.patch.object(database, '_clear_inventory_jobs') as jobs:
            with mock.patch.object(database, 'ensure_jobs_constraints'):
                self.assertEqual(database.serverinit(), 0)
        timeout.assert_called_once_with()
        host.assert_called_once_with(server_host=True)
        for destructive in (stale, results, queues, jobs):
            destructive.assert_not_called()

    def test_serverinit_keeps_active_jobs_even_with_old_status_constraint(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        with mock.patch.object(database, '_show_statement_timeout_hint'), \
             mock.patch.object(database, '_mark_active_jobs_stale', side_effect=RuntimeError('old CHECK')) as stale, \
             mock.patch.object(database, '_write_local_host', return_value=0):
            with mock.patch.object(database, 'ensure_jobs_constraints'):
                self.assertEqual(database.serverinit(), 0)
        stale.assert_not_called()

    def test_only_active_rows_change_across_all_hosts_preserving_run_metadata(self):
        sql = sqlite3.connect(':memory:')
        self.addCleanup(sql.close)
        sql.executescript('''
            CREATE TABLE jobs (jobid INTEGER, hostid INTEGER, status TEXT,
                runid INTEGER, stage TEXT, timeupdated TEXT, last_activity TEXT);
            CREATE TABLE jobsrunid (runid INTEGER, status TEXT);
            INSERT INTO jobsrunid VALUES (99, 'active');
            CREATE TABLE tmp0_job_work (jobid INTEGER, state TEXT);
            INSERT INTO tmp0_job_work VALUES (1, 'active');
        ''')
        statuses = ['active', 'active', 'active', 'paused', 'stopped', 'closed',
                    'error', 'pending', 'insert', 'stale']
        sql.executemany('INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?)',
                        [(i, i % 3, status, i + 99, 'detailed', 'before', 'last activity')
                         for i, status in enumerate(statuses, 1)])
        sql.commit()
        class Cursor:
            def __init__(self):
                self.inner = sql.cursor()
            def execute(self, statement, params):
                self.inner.execute(statement.replace('%s', '?'), params)
            @property
            def rowcount(self):
                return self.inner.rowcount
            def close(self):
                self.inner.close()
        class Connection:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def cursor(self):
                return Cursor()
            def commit(self):
                sql.commit()
            def rollback(self):
                sql.rollback()
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        with mock.patch.object(database, 'connect', return_value=Connection()):
            self.assertEqual(database._mark_active_jobs_stale(), 3)
            self.assertEqual(database._mark_active_jobs_stale(), 0)
        rows = sql.execute('SELECT jobid, status, runid, stage, last_activity FROM jobs ORDER BY jobid').fetchall()
        for i, status, runid, stage, activity in rows:
            self.assertEqual(status, 'stale' if i <= 3 else statuses[i - 1])
            self.assertEqual((runid, stage, activity), (i + 99, 'detailed', 'last activity'))
        self.assertEqual(sql.execute('SELECT * FROM jobsrunid').fetchall(), [(99, 'active')])
        self.assertEqual(sql.execute('SELECT * FROM tmp0_job_work').fetchall(), [(1, 'active')])

    def test_stale_update_rolls_back_and_closes_cursor_on_failure(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.execute.side_effect = RuntimeError('CHECK rejects stale')
        with mock.patch.object(database, 'connect', return_value=connection), self.assertRaises(RuntimeError):
            database._mark_active_jobs_stale()
        connection.commit.assert_not_called()
        connection.rollback.assert_called_once()
        connection.cursor.return_value.close.assert_called_once()


class TestServerInitWorkReset(unittest.TestCase):
    def database(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = 'testdb'
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        return database, connection, connection.cursor.return_value

    def test_all_numeric_work_queues_are_dropped_and_recreated_empty(self):
        database, connection, cursor = self.database()
        cursor.fetchall.return_value = [(name,) for name in (
            'tmp0_job_work', 'tmp7_job_work', 'tmp0008_job_work', 'tmp77_job_work',
            'job_work', 'tmp_highpriority', 'tmp_deepdoubles_result', 'tmpx_job_work')]
        with mock.patch.object(database, 'connect', return_value=connection):
            self.assertEqual(database._reset_host_work_tables(),
                             ['tmp0_job_work', 'tmp7_job_work', 'tmp8_job_work', 'tmp77_job_work'])
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        drops = [sql for sql in statements if sql.startswith('DROP')]
        self.assertEqual(set(drops), {f'DROP TABLE IF EXISTS `{name}`' for name in (
            'tmp0_job_work', 'tmp7_job_work', 'tmp0008_job_work', 'tmp77_job_work')})
        for hostid in (0, 7, 8, 77):
            self.assertTrue(any(f'CREATE TABLE IF NOT EXISTS tmp{hostid}_job_work (' in sql
                                for sql in statements))
        self.assertFalse(any('INSERT IGNORE' in sql or 'DELETE FROM job_work' in sql for sql in statements))
        cursor.fetchone.assert_not_called()
        self.assertLess(max(i for i, sql in enumerate(statements) if sql.startswith('DROP')),
                        min(i for i, sql in enumerate(statements) if 'CREATE TABLE' in sql))
        connection.commit.assert_called_once()
        cursor.close.assert_called_once()

    def test_host_zero_queue_is_created_when_no_queues_exist(self):
        database, connection, cursor = self.database()
        cursor.fetchall.return_value = []
        with mock.patch.object(database, 'connect', return_value=connection):
            self.assertEqual(database._reset_host_work_tables(), ['tmp0_job_work'])
        self.assertTrue(any('CREATE TABLE IF NOT EXISTS tmp0_job_work' in call.args[0]
                            for call in cursor.execute.call_args_list))

    def test_invalid_host_queue_fails_before_dropping_any_queue(self):
        database, connection, cursor = self.database()
        cursor.fetchall.return_value = [('tmp32768_job_work',)]
        with mock.patch.object(database, 'connect', return_value=connection), self.assertRaises(ValueError):
            database._reset_host_work_tables()
        self.assertFalse(any(call.args[0].startswith('DROP') for call in cursor.execute.call_args_list))
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()

    def test_priority_table_is_dropped_before_being_recreated(self):
        database, connection, cursor = self.database()
        with mock.patch.object(database, 'connect', return_value=connection):
            database._reset_binarycheck_results()
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        drop = statements.index('DROP TABLE IF EXISTS tmp_highpriority')
        create = next(i for i, sql in enumerate(statements)
                      if 'CREATE TABLE IF NOT EXISTS tmp_highpriority' in sql)
        self.assertLess(drop, create)
        connection.commit.assert_called_once()


class TestDuplicateHostConfirmation(unittest.TestCase):
    def register(self, answers, old_user='alex', old_hostid=7):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.database = 'testdb'
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        def fetchone():
            sql = cursor.execute.call_args.args[0]
            if 'GET_LOCK' in sql or 'RELEASE_LOCK' in sql:
                return (1,)
            if 'information_schema.columns' in sql:
                return (1,)
            if 'SELECT hostid, `user`' in sql:
                return (old_hostid, old_user)
            if 'MAX(hostid)' in sql:
                return (8,)
            raise AssertionError(sql)
        cursor.fetchone.side_effect = fetchone
        with mock.patch.object(database, 'connect', return_value=connection), \
             mock.patch.object(database, '_local_host_values', return_value=('alex', '/home/alex', 'Linux', 'pc')), \
             mock.patch.object(database, '_host_home_dirid', return_value=None), \
             mock.patch.object(database, 'create_job_work_table', side_effect=lambda hostid, cursor: f'tmp{hostid}_job_work'), \
             mock.patch('builtins.input', side_effect=answers) as question:
            result = database.set_my_hostid()
        return result, cursor, question

    def test_yes_registers_an_additional_id_on_same_computer(self):
        result, cursor, question = self.register(['y'])
        self.assertEqual(result, 8)
        insert = next(call for call in cursor.execute.call_args_list if call.args[0].startswith('INSERT INTO hosts'))
        self.assertEqual(insert.args[1][:3], (8, 'alex', '/home/alex'))
        self.assertEqual(question.call_count, 1)
        for value in ('Linux', 'pc', '/home/alex', '[yN]'):
            self.assertIn(value, question.call_args.args[0])

    def test_default_and_no_reuse_existing_id(self):
        for answer in ('', 'n', 'N'):
            result, cursor, question = self.register([answer])
            self.assertEqual(result, 7)
            self.assertFalse(any('MAX(hostid)' in call.args[0] for call in cursor.execute.call_args_list))

    def test_invalid_answer_is_asked_again(self):
        result, cursor, question = self.register(['maybe', 'Y'])
        self.assertEqual(result, 8)
        self.assertEqual(question.call_count, 2)

    def test_declining_never_overwrites_other_user_or_uses_host_zero(self):
        for old_user, old_hostid in (('other-user', 7), ('alex', 0)):
            result, cursor, question = self.register(['n'], old_user, old_hostid)
            self.assertIsNone(result)
            self.assertFalse(any(call.args[0].startswith('INSERT INTO hosts') for call in cursor.execute.call_args_list))

    def test_end_of_input_defaults_to_no_new_id(self):
        result, cursor, question = self.register([EOFError()])
        self.assertEqual(result, 7)


class TestServerinitStatementTimeoutHint(unittest.TestCase):
    def database(self, values):
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchone.return_value = values
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.connect = mock.Mock(return_value=connection)
        return database, connection

    def test_matching_values_do_not_show_configuration_instructions(self):
        from decimal import Decimal
        database, connection = self.database((Decimal('360.000000'), 360.0))
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            database._show_statement_timeout_hint()
        self.assertEqual(output.getvalue(), '')
        connection.cursor.return_value.execute.assert_called_once_with(
            'SELECT @@GLOBAL.max_statement_time, @@SESSION.max_statement_time')
        connection.cursor.return_value.close.assert_called_once()

    def test_different_global_or_session_value_shows_actual_values_and_instructions(self):
        for values in ((0, 0), (300, 300), (360, 300), (300, 360), (600, 600)):
            database, connection = self.database(values)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                database._show_statement_timeout_hint()
            text = output.getvalue()
            self.assertIn(f'global={values[0]} s, Sitzung={values[1]} s', text)
            self.assertIn('/var/packages/MariaDB10/etc/my.cnf', text)
            self.assertIn('SET GLOBAL max_statement_time = 360;', text)
            self.assertIn('Administrator', text)
            self.assertEqual(connection.cursor.return_value.execute.call_count, 1)

    def test_read_failure_prevents_destructive_serverinit_operations(self):
        database, connection = self.database((360, 360))
        connection.cursor.return_value.execute.side_effect = RuntimeError('read failed')
        with mock.patch.object(database, '_mark_active_jobs_stale') as stale, \
             mock.patch.object(database, '_write_local_host') as hosts, \
             self.assertRaisesRegex(RuntimeError, 'read failed'):
            database.serverinit()
        stale.assert_not_called()
        hosts.assert_not_called()
        connection.cursor.return_value.close.assert_called_once()


class TestProgressHostSelection(unittest.TestCase):
    def setUp(self):
        import sqlite3
        self.sql = sqlite3.connect(':memory:')
        self.addCleanup(self.sql.close)
        self.sql.executescript('''
            CREATE TABLE jobs(jobid, task, hostid, status);
            CREATE TABLE tmp_doubles_letchecked(jobid, hostid, status, is_requested);
            INSERT INTO jobs VALUES (34, 'binarycheck', 1, 'active'),
                                    (35, 'binarycheck', 0, 'closed'),
                                    (36, 'binarycheck', 2, 'pending'),
                                    (37, 'scan', 1, 'active');
            INSERT INTO tmp_doubles_letchecked VALUES
                (34,1,'equal',1),(34,1,'pending',1),(34,2,'equal',1),
                (34,1,'equal',0),(35,0,'different',1);
        ''')
        sql = self.sql
        class Cursor:
            def __init__(self): self.cursor = sql.cursor()
            def execute(self, statement, params): self.cursor.execute(statement.replace('%s', '?'), params)
            def fetchone(self): return self.cursor.fetchone()
            def close(self): self.cursor.close()
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.side_effect = Cursor
        self.database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        self.database.connect = mock.Mock(return_value=connection)

    def test_latest_job_is_selected_for_requested_host(self):
        self.assertEqual(self.database.get_double_job_progress(hostid=1), (34, 'active', 2, 1))
        self.assertEqual(self.database.get_double_job_progress(hostid=0), (35, 'closed', 1, 1))
        self.assertEqual(self.database.get_double_job_progress(), (36, 'pending', 0, 0))

    def test_job_and_host_must_match(self):
        self.assertIsNone(self.database.get_double_job_progress(34, hostid=2))
        self.assertIsNone(self.database.get_double_job_progress(hostid=3))
        self.assertEqual(self.database.get_double_job_progress(34, hostid=1), (34, 'active', 2, 1))

    def test_invalid_host_fails_before_connection(self):
        for hostid in (-1, 32768):
            with self.assertRaises(ValueError):
                self.database.get_double_job_progress(hostid=hostid)
        self.database.connect.assert_not_called()


class TestStrictInitLifecycle(unittest.TestCase):
    def test_inventory_replacement_clears_jobs_but_declining_preserves_them(self):
        for replaced in (False, True):
            database = DataBaseCLI(object.__new__(app.DataBaseConnection))
            order = mock.Mock()
            with mock.patch.object(database, 'init_database') as schema, \
                 mock.patch.object(database, '_show_statement_timeout_hint') as timeout, \
                 mock.patch.object(database, 'init_alldirs', return_value=replaced) as dirs, \
                 mock.patch.object(database, '_clear_inventory_jobs') as clear, \
                 mock.patch.object(database, '_write_local_host', return_value=0) as host, \
                 mock.patch.object(database, 'check_schema', return_value=True) as check:
                for name, method in [('schema', schema), ('timeout', timeout), ('dirs', dirs),
                                     ('clear', clear), ('host', host), ('check', check)]:
                    order.attach_mock(method, name)
                with mock.patch.object(database, 'ensure_jobs_constraints'):
                    self.assertEqual(database.initialize(), 0)
            expected = [mock.call.schema(), mock.call.timeout(), mock.call.dirs()]
            if replaced:
                expected.append(mock.call.clear())
            expected.extend([mock.call.host(server_host=True), mock.call.check()])
            self.assertEqual(order.mock_calls, expected)

    def test_timeout_read_failure_prevents_inventory_and_job_deletion(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        with mock.patch.object(database, 'init_database'), \
             mock.patch.object(database, '_show_statement_timeout_hint', side_effect=RuntimeError('timeout read failed')), \
             mock.patch.object(database, 'init_alldirs') as dirs, \
             mock.patch.object(database, '_clear_inventory_jobs') as clear, \
             self.assertRaisesRegex(RuntimeError, 'timeout read failed'):
            database.initialize()
        dirs.assert_not_called()
        clear.assert_not_called()

    def test_full_schema_check_failure_is_reported_by_init(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        with mock.patch.object(database, 'init_database'), \
             mock.patch.object(database, '_show_statement_timeout_hint'), \
             mock.patch.object(database, 'init_alldirs', return_value=False), \
             mock.patch.object(database, '_write_local_host', return_value=0), \
             mock.patch.object(database, 'check_schema', return_value=False):
            with mock.patch.object(database, 'ensure_jobs_constraints'):
                self.assertEqual(database.initialize(), 1)

    def test_deleted_inventory_invalidates_jobs_history_queues_and_references_atomically(self):
        sql = sqlite3.connect(':memory:')
        self.addCleanup(sql.close)
        tables = ['jobs', 'jobsrunid', 'tmp0_job_work', 'tmp1_job_work', 'job_work',
                  'tmp_highpriority', 'tmp_deepdoubles_result', 'tmp_doubles_letchecked']
        for name in tables:
            sql.execute(f'CREATE TABLE {name} (id INTEGER)')
            sql.execute(f'INSERT INTO {name} VALUES (42)')
        sql.execute('CREATE TABLE hosts (hostid INTEGER, homedirid INTEGER)')
        sql.executemany('INSERT INTO hosts VALUES (?, ?)', [(0, 123), (1, 456)])
        sql.execute('CREATE TABLE unrelated (value TEXT)')
        sql.execute("INSERT INTO unrelated VALUES ('keep')")
        sql.commit()
        class Cursor:
            def __init__(self): self.cursor = sql.cursor()
            def execute(self, statement, params=()):
                if 'information_schema.tables' in statement:
                    statement, params = "SELECT name FROM sqlite_master WHERE type='table'", ()
                self.cursor.execute(statement.replace('%s', '?'), params)
            def fetchall(self): return self.cursor.fetchall()
            def close(self): self.cursor.close()
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.side_effect = Cursor
        connection.commit.side_effect = sql.commit
        connection.rollback.side_effect = sql.rollback
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.connection.database = 'testdb'
        with mock.patch.object(database, 'connect', return_value=connection):
            database._clear_inventory_jobs()
        for name in tables:
            self.assertEqual(sql.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0], 0)
        self.assertEqual(sql.execute('SELECT * FROM hosts ORDER BY hostid').fetchall(), [(0, None), (1, None)])
        self.assertEqual(sql.execute('SELECT * FROM unrelated').fetchall(), [('keep',)])
        connection.commit.assert_called_once()


class TestInventoryResetRollback(unittest.TestCase):
    def test_failed_history_delete_rolls_back_earlier_queue_deletes(self):
        sql = sqlite3.connect(':memory:')
        self.addCleanup(sql.close)
        sql.executescript('CREATE TABLE tmp1_job_work(id); INSERT INTO tmp1_job_work VALUES(42); '
                          'CREATE TABLE jobsrunid(id); INSERT INTO jobsrunid VALUES(42); '
                          'CREATE TABLE jobs(id); INSERT INTO jobs VALUES(42);')
        sql.commit()
        class Cursor:
            def __init__(self): self.cursor = sql.cursor()
            def execute(self, statement, params=()):
                if 'information_schema.tables' in statement:
                    statement, params = "SELECT name FROM sqlite_master WHERE type='table'", ()
                if statement == 'DELETE FROM `jobsrunid`':
                    raise RuntimeError('history delete failed')
                self.cursor.execute(statement, params)
            def fetchall(self): return self.cursor.fetchall()
            def close(self): self.cursor.close()
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.side_effect = Cursor
        connection.commit.side_effect = sql.commit
        connection.rollback.side_effect = sql.rollback
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.connection.database = 'testdb'
        with mock.patch.object(database, 'connect', return_value=connection), \
             self.assertRaisesRegex(RuntimeError, 'history delete failed'):
            database._clear_inventory_jobs()
        for table in ('tmp1_job_work', 'jobsrunid', 'jobs'):
            self.assertEqual(sql.execute(f'SELECT * FROM {table}').fetchall(), [(42,)])
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()


class PriorityTests_DataBaseCLI(unittest.TestCase):
    def worker(self):
        db = mock.Mock()
        db.zero_content_directory_ids.return_value = set()
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        db.connect.return_value = connection
        worker = app.Binarycheck(db)
        worker._priority_runid = 100
        worker._next_priority_check = 0
        worker._priority_ids = set()
        return worker, db, connection, connection.cursor.return_value

    def test_insert_deduplicates_and_rolls_back_unknown_host_ids(self):
        worker, db, connection, cursor = self.worker()
        cli = DataBaseCLI(db)
        cursor.fetchall.return_value = [(7,), (8,)]
        self.assertEqual(cli.insert_priority(1, [7, 8, 7]), 2)
        self.assertEqual(cursor.executemany.call_args.args[1], [(1, 7), (1, 8)])
        connection.commit.assert_called_once()
        cursor.executemany.reset_mock()
        cursor.fetchall.return_value = [(7,)]
        with self.assertRaises(ValueError):
            cli.insert_priority(1, [7, 8])
        cursor.executemany.assert_not_called()
        connection.rollback.assert_called_once()

    def test_serverinit_creates_shared_table(self):
        worker, db, connection, cursor = self.worker()
        cli = DataBaseCLI(db)
        with mock.patch.object(cli, 'create_double_jobs_table'):
            cli._reset_binarycheck_results()
        self.assertTrue(any('CREATE TABLE IF NOT EXISTS tmp_highpriority' in call.args[0]
                            for call in cursor.execute.call_args_list))


class BinarycheckHostTests_DataBaseCLI(unittest.TestCase):
    def test_each_host_gets_its_own_job_and_comparison_rows(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.list_excluded_names = mock.Mock(return_value=[])
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        type(cursor).lastrowid = mock.PropertyMock(side_effect=[88, 89])
        rows = [(1, '/a', '0', 10, 'hash', 0), (2, '/b', '0', 10, 'hash', 0),
                (3, '/c', '0', 10, 'otherhash', 1), (4, '/d', '0', 10, 'otherhash', 1)]
        with mock.patch.object(database, 'list_scoped_duplicate_roots', return_value=rows) as roots, \
                mock.patch.object(database, 'list_directory_hostids', return_value=[0, 1]), \
                mock.patch.object(database, 'connect', return_value=connection), \
                mock.patch.object(database, 'create_double_jobs_table'):
            self.assertEqual(database.create_binarycheck_job(), [(88, 1), (89, 1)])
        self.assertEqual(roots.call_args_list, [mock.call((0,)), mock.call((1,)), mock.call((0, 1))])
        inserts = [call for call in cursor.execute.call_args_list if 'INSERT INTO jobs' in call.args[0]]
        self.assertEqual([call.args[1][1] for call in inserts], [0, 1])
        self.assertEqual([call.args[1] for call in cursor.executemany.call_args_list],
                         [[(88, 0, 0, 1, 2)], [(89, 1, 1, 3, 4)]])

    def test_missing_host_cannot_silently_default_to_zero(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        database.list_excluded_names = mock.Mock(return_value=[])
        with mock.patch.object(database, 'list_directory_hostids', return_value=[0]), \
                mock.patch.object(database, 'list_scoped_duplicate_roots', return_value=[(1, '/a', '0', 10, 'hash')]), \
                mock.patch.object(database, 'connect') as connect:
            with self.assertRaisesRegex(RuntimeError, 'Host-ID fehlt'):
                database.create_binarycheck_job()
        connect.assert_not_called()


class DeleteLiveExcludedTests_DataBaseCLI(unittest.TestCase):
    def test_delete_matches_hosts_and_literal_directory_boundaries(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.rowcount = 7
        with mock.patch.object(database, 'connect', return_value=connection), \
                mock.patch.object(database, '_live_excluded_read', side_effect=[[(i,) for i in range(1, 8)], []]), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(database.live_excluded_directories(hostid=2, delete=True), 7)
        sql, params = cursor.execute.call_args.args
        self.assertTrue(sql.startswith('DELETE d FROM directories d WHERE EXISTS'))
        self.assertIn('e.hostid = d.hostid', sql)
        self.assertIn("CONCAT(TRIM(TRAILING '/' FROM e.path), '/')", sql)
        self.assertIn('BINARY d.path', sql)
        self.assertNotIn('LIKE', sql)
        self.assertEqual(params, (2, 1, 2, 3, 4, 5, 6, 7))
        connection.commit.assert_called_once()

    def test_lost_commit_retries_same_primary_keys_with_fresh_connection(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        first, second = mock.MagicMock(), mock.MagicMock()
        first.__enter__.return_value = first
        second.__enter__.return_value = second
        first.cursor.return_value.rowcount = 2
        second.cursor.return_value.rowcount = 0
        first.commit.side_effect = app.Error(msg='commit response lost', errno=2055)
        with mock.patch.object(database, 'connect', side_effect=[first, second]) as connect, \
                mock.patch.object(database, '_live_excluded_read', side_effect=[[(10,), (11,)], []]), \
                mock.patch('DataBaseCLI.time.sleep'), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(database.live_excluded_directories(hostid=1, delete=True), 2)
        self.assertEqual(connect.call_count, 2)
        self.assertEqual(first.cursor.return_value.execute.call_args,
                         second.cursor.return_value.execute.call_args)
        self.assertEqual(second.cursor.return_value.execute.call_args.args[1], (1, 10, 11))

    def test_batches_advance_primary_key_and_commit_separately(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.rowcount = 2
        with mock.patch.object(database, 'connect', return_value=connection), \
                mock.patch.object(database, '_live_excluded_read', side_effect=[[(1,), (2,)], [(3,), (4,)], []]) as read, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(database.live_excluded_directories(hostid=1, delete=True), 4)
        self.assertEqual(connection.commit.call_count, 2)
        self.assertEqual([call.args[1] for call in read.call_args_list], [(1, 0), (1, 2), (1, 4)])
        self.assertTrue(all('LIMIT 3000' in call.args[0] for call in read.call_args_list))


class DirectorySummaryTests_DataBaseCLI(unittest.TestCase):
    def report(self, entries):
        operation = object.__new__(client_operations)
        operation.database = mock.Mock()
        operation.database.directory_summary_data.return_value = [
            dict(hostid=host, runid=run, jobid=run + 100, directory_count=count, run_timestamp=stamp)
            for host, run, count, stamp in entries
        ]
        with contextlib.redirect_stdout(io.StringIO()) as output:
            operation.show_directory_summary_data()
        return output.getvalue()

    def test_query_counts_by_host_and_run_with_timestamp(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchall.return_value = []
        with mock.patch.object(database, 'connect', return_value=connection):
            self.assertEqual(database.directory_summary_data(), [])
        query = cursor.execute.call_args.args[0]
        self.assertIn('r.timecreated AS run_timestamp', query)
        self.assertIn('AS incomplete_count', query)
        self.assertIn('AS missing_hash_count', query)
        self.assertIn('d.combined_sha256 IS NULL', query)
        self.assertNotIn('tmp_doubles_letchecked', query)
        self.assertIn('d.localfilesize IS NULL', query)
        self.assertNotIn('emptyfilecount IS NULL', query)
        self.assertIn('r.runid = d.runid', query)
        self.assertIn('GROUP BY d.runid, d.hostid, r.timecreated', query)
        self.assertTrue(query.startswith('SELECT '))
        cursor.close.assert_called_once()


class DuplicateTimeoutTests(unittest.TestCase):
    def test_preparation_triples_timeout_and_restores_on_failure(self):
        database = DataBaseCLI(mock.Mock())
        with mock.patch.object(database, '_create_binarycheck_job', side_effect=RuntimeError('query')) as prepare:
            prepare.side_effect = lambda: self.assertEqual(database._timeout_multiplier, 3)
            database.create_binarycheck_job()
            self.assertEqual(database._timeout_multiplier, 1)
            prepare.side_effect = RuntimeError('query')
            with self.assertRaises(RuntimeError):
                database.create_binarycheck_job()
            self.assertEqual(database._timeout_multiplier, 1)

    def test_connection_and_sql_limits_are_multiplied(self):
        connection = mock.Mock()
        database = DataBaseCLI(connection)
        database._timeout_multiplier = 3
        database.connect()
        connection.connect.assert_called_once_with(timeout_multiplier=3)
        connection.connect.return_value.cursor.return_value.execute.assert_called_once_with(
            'SET SESSION max_statement_time = @@SESSION.max_statement_time * %s', (3,))


class CrossHostCandidateTests(unittest.TestCase):
    def test_localserver_leads_even_when_remote_dirid_is_smaller(self):
        db = DataBaseCLI(object.__new__(app.DataBaseConnection))
        db.list_excluded_names = mock.Mock(return_value=[])
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        type(cursor).lastrowid = mock.PropertyMock(side_effect=[6, 7])
        cursor.fetchone.return_value = ('local-machine',)
        rows = [(10, '/same/path', '0-1', 10, 'hash', 0),
                (20, '/same/path', '0-1', 10, 'hash', 1)]
        with mock.patch.object(db, 'list_scoped_duplicate_roots', return_value=rows), \
                mock.patch.object(db, 'list_directory_hostids', return_value=[0, 1]), \
                mock.patch.object(db, 'connect', return_value=connection), \
                mock.patch.object(db, 'create_double_jobs_table'):
            self.assertEqual(db.create_binarycheck_job(), (6, 1))
        self.assertEqual(cursor.executemany.call_args.args[1], [(6, 1, 0, 20, 10)])
        insert = [call for call in cursor.execute.call_args_list if 'INSERT INTO jobs' in call.args[0]][-1]
        self.assertEqual(insert.args[1][:2], ('local-machine', 1))


class PerHostJobTests(unittest.TestCase):
    def prepare(self, hosts, candidates):
        db = DataBaseCLI(object.__new__(app.DataBaseConnection))
        db.list_excluded_names = mock.Mock(return_value=[])
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        type(cursor).lastrowid = mock.PropertyMock(side_effect=range(10, 10 + len(hosts)))
        cursor.fetchone.return_value = ('registered-machine',)
        with mock.patch.object(db, 'list_scoped_duplicate_roots', return_value=candidates), \
                mock.patch.object(db, 'list_directory_hostids', return_value=hosts), \
                mock.patch.object(db, 'connect', return_value=connection):
            result = db.create_binarycheck_job()
        inserts = [call.args[1][1] for call in cursor.execute.call_args_list if 'INSERT INTO jobs' in call.args[0]]
        pairs = [call.args[1] for call in cursor.executemany.call_args_list]
        return result, inserts, pairs

    def test_no_empty_jobs_when_hosts_have_no_candidates(self):
        result, inserts, pairs = self.prepare([0, 1, 7], [])
        self.assertEqual(result, (None, 0))
        self.assertEqual(inserts, [])
        self.assertEqual(pairs, [])

    def test_one_job_per_host_with_local_and_multiple_remote_candidate_sets(self):
        candidates = [(id_, '/path' + str(id_), '0-1', 10, 'samehash', host)
                      for host, id_ in [(0, 10), (0, 11), (1, 20), (1, 21), (2, 30)]]
        result, inserts, pairs = self.prepare([0, 1, 2], candidates)
        self.assertEqual(inserts, [0, 1, 2])
        self.assertEqual(result, [(10, 1), (11, 3), (12, 1)])
        self.assertEqual(pairs[0], [(10, 0, 0, 10, 11)])
        self.assertEqual(pairs[1], [(11, 1, 0, 20, 10), (11, 1, 1, 20, 21), (11, 1, 2, 20, 30)])


class ScopedCandidateQueries(unittest.TestCase):
    def test_query_pages_are_scoped_and_bounded(self):
        db = DataBaseCLI(object.__new__(app.DataBaseConnection))
        db.list_excluded_names = mock.Mock(return_value=[])
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        batch = [(i, '/p'+str(i), '0', 1, 'hash', 7) for i in range(1, 1001)]
        cursor.fetchall.side_effect = [batch, []]
        with mock.patch.object(db, 'connect', return_value=connection):
            self.assertEqual(len(db._read_duplicate_candidates(7, ['hash'])), 1000)
        calls = cursor.execute.call_args_list
        self.assertEqual([call.args[1] for call in calls], [(7, 0, 'hash'), (7, 1000, 'hash')])
        self.assertTrue(all('LIMIT 1000' in call.args[0] and 'WHERE hostid = %s' in call.args[0] for call in calls))
        self.assertFalse(any('RECURSIVE' in call.args[0] or 'GROUP BY' in call.args[0] for call in calls))

    def test_all_single_hosts_precede_every_host_pair(self):
        db = DataBaseCLI(object.__new__(app.DataBaseConnection))
        db.list_excluded_names = mock.Mock(return_value=[])
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        with mock.patch.object(db, 'list_directory_hostids', return_value=[2, 0, 1]), \
                mock.patch.object(db, 'list_scoped_duplicate_roots', return_value=[]) as find, \
                mock.patch.object(db, 'connect', return_value=connection), contextlib.redirect_stdout(io.StringIO()):
            db.create_binarycheck_job()
        self.assertEqual(find.call_args_list, [mock.call((0,)), mock.call((1,)), mock.call((2,)),
                                              mock.call((0,1)), mock.call((0,2)), mock.call((1,2))])

    def test_pair_queries_only_matching_hashes_and_requires_both_hosts(self):
        db = DataBaseCLI(object.__new__(app.DataBaseConnection))
        db.list_excluded_names = mock.Mock(return_value=[])
        db._candidate_host_rows = {0: [(1, '/a', '0', 1, 'match', 0), (2, '/b', '0', 1, 'only-left', 0)]}
        with mock.patch.object(db, '_read_duplicate_candidates', return_value=[
                (3, '/c', '0', 1, 'match', 1), (4, '/d', 'other', 1, 'only-left', 1)]) as read:
            result = db.list_scoped_duplicate_roots((0,1))
        self.assertEqual({row[0] for row in result}, {1,3})
        read.assert_called_once_with(1, ['match', 'only-left'])


class TreeSnapshotTests_DataBaseCLI(unittest.TestCase):
    def test_in_memory_analysis_uses_reads_and_does_not_rebuild_tables(self):
        db = DataBaseCLI(object.__new__(app.DataBaseConnection))
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchall.side_effect = [
            [(38,0,'active',448,10), (39,1,'closed',1,1)],
            [(0,'root'), (1,'root')],
            [(1,0,1,'root','/Mac'), (2,1,1,'child','/Mac/a'),
             (10,0,0,'root','/NAS'), (11,10,0,'child','/NAS/a')],
            [(2,'/Mac/a','child','0',10,1,0,1), (11,'/NAS/a','child','0',10,1,0,0)],
        ]
        with mock.patch.object(db, 'connect', return_value=connection), \
                mock.patch.object(db, 'get_double_job_batch', return_value=[]), \
                mock.patch.object(db, 'list_verified_double_pairs', return_value=[]), \
                mock.patch.object(db, '_store_deep_double_results') as store:
            rows = db.list_doubles_tree_snapshot_rows(39)
        self.assertEqual({row[12] for row in rows}, {0,1})
        store.assert_not_called()
        cursor.executemany.assert_not_called()
        statements = [call.args[0].strip().split()[0] for call in cursor.execute.call_args_list]
        self.assertTrue(all(word in ('SELECT','SET','START') for word in statements))
        connection.commit.assert_not_called()

    def test_confirmed_cross_host_root_is_visible_without_nested_cache(self):
        db = DataBaseCLI(object.__new__(app.DataBaseConnection))
        root = (20,'/Mac/25HS','hash','1-0',10,'/NAS/25HS',10,10,1,1,1,1,1,0)
        with mock.patch.object(db, 'list_deep_double_levels', return_value=[]), \
                mock.patch.object(db, 'get_double_job_batch', return_value=[(38,0),(39,1)]), \
                mock.patch.object(db, 'list_verified_double_pairs', side_effect=[[],[root]]):
            self.assertEqual(db.list_doubles_tree_snapshot_rows(39), [root])


class TestAllJobsTreeSelection(unittest.TestCase):
    def test_all_jobs_analysis_has_no_creation_time_or_runid_filter_and_no_writes(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchall.side_effect = [[(1,0,'closed',1,1),(39,1,'closed',1,1)], [], []]
        with mock.patch.object(database, 'connect', return_value=connection), \
                mock.patch.object(database, '_store_deep_double_results') as save:
            self.assertEqual(database.list_deep_double_levels(persist=False, all_jobs=True), [])
        sql, params = cursor.execute.call_args_list[2].args
        self.assertNotIn('j.timecreated =', sql)
        self.assertNotIn('runid', sql)
        self.assertEqual(params, ())
        self.assertEqual(cursor.execute.call_args_list[3].args[1], (1,39))
        save.assert_not_called()
        cursor.executemany.assert_not_called()

    def test_all_jobs_list_has_no_latest_job_limit(self):
        database = DataBaseCLI(object.__new__(app.DataBaseConnection))
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchall.return_value = [(1,0),(38,0),(39,1)]
        with mock.patch.object(database, 'connect', return_value=connection):
            self.assertEqual(database.get_double_job_batch(None), [(1,0),(38,0),(39,1)])
        self.assertNotIn('LIMIT', cursor.execute.call_args.args[0])


# exclusion_purge regression coverage.


class TestExclusionPurge(unittest.TestCase):
    def setUp(self):
        self.sql = sqlite3.connect(':memory:')
        self.addCleanup(self.sql.close)
        self.sql.create_function('CONCAT', -1, lambda *parts: ''.join(map(str, parts)))
        self.sql.create_function('LOCATE', 2, lambda needle, value: value.find(needle) + 1)
        self.sql.create_function('CHAR_LENGTH', 1, len)
        self.sql.create_function('LEFTSTR', 2, lambda value, size: value[:size])
        self.sql.executescript('''
            CREATE TABLE directories (dirid INTEGER PRIMARY KEY, hostid INTEGER, path TEXT);
            CREATE TABLE excluded_names (name TEXT);
            CREATE TABLE live_excluded_absdirs (hostid INTEGER, path TEXT);
        ''')
        self.sql.executemany('INSERT INTO excluded_names VALUES (?)',
                             [('@eaDir',), ('100%_cache',), ('',)])
        self.sql.executemany('INSERT INTO live_excluded_absdirs VALUES (?,?)',
                             [(1, '/private/cache/'), (1, '/literal%_path'), (2, '/')])
        paths = [(1, '/photos/@EADIR'), (1, '/photos/@eaDir/nested'),
                 (1, '/photos/@eaDir-other'), (1, '/photos/100%_cache/nested'),
                 (1, '/photos/100x_cache'), (1, '/private/cache'),
                 (1, '/private/cache/child'), (1, '/private/cache-other'),
                 (3, '/private/cache'), (1, '/Private/cache'),
                 (1, '/literal%_path/child'), (1, '/literalXXpath'),
                 (2, '/anything'), (3, '/photos/@eadir/nested')]
        self.sql.executemany('INSERT INTO directories (hostid,path) VALUES (?,?)', paths)
        self.sql.commit()
        self.statements = []
        sql, statements = self.sql, self.statements

        class Cursor:
            def __init__(self): self.inner = sql.cursor()
            def execute(self, query, params=()):
                statements.append((query, params))
                if 'information_schema.tables' in query:
                    query = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?"
                    params = (params[-1],)
                query = query.replace('DELETE d FROM directories d', 'DELETE FROM directories AS d')
                query = query.replace("TRIM(BOTH '/' FROM d.path)", "TRIM(d.path, '/')")
                query = query.replace("TRIM(TRAILING '/' FROM e.path)", "RTRIM(e.path, '/')")
                query = query.replace('BINARY ', '').replace('LEFT(', 'LEFTSTR(').replace('%s', '?')
                self.inner.execute(query, params)
            def fetchone(self): return self.inner.fetchone()
            def fetchall(self): return self.inner.fetchall()
            @property
            def rowcount(self): return self.inner.rowcount
            def close(self): self.inner.close()

        class Connection:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def cursor(self): return Cursor()
            def commit(self): sql.commit()
            def rollback(self): sql.rollback()

        self.db = DataBaseCLI(object.__new__(app.DatabaseConnection))
        self.db.database = 'testdb'
        patch = mock.patch.object(self.db, 'connect', return_value=Connection())
        patch.start()
        self.addCleanup(patch.stop)

    def remaining(self):
        return self.sql.execute('SELECT hostid,path FROM directories ORDER BY dirid').fetchall()

    def test_all_hosts_literal_names_paths_descendants_and_count(self):
        self.assertEqual(self.db.purge_directories_from_exclusion_lists(), 8)
        self.assertEqual(self.remaining(), [(1, '/photos/@eaDir-other'),
                         (1, '/photos/100x_cache'), (1, '/private/cache-other'),
                         (3, '/private/cache'), (1, '/Private/cache'), (1, '/literalXXpath')])
        deletions = [(query, params) for query, params in self.statements if query.startswith('DELETE')]
        self.assertEqual([params[0] for query, params in deletions], [1, 2, 3])
        self.assertTrue(all('AND d.dirid IN (' in query for query, params in deletions))
        self.assertTrue(all('AND d.hostid = %s' in query for query, params in deletions))
        self.assertEqual(self.db.purge_directories_from_exclusion_lists(), 0)

    def test_optional_host_limits_both_lists(self):
        self.assertEqual(self.db.purge_directories_from_exclusion_lists(1), 6)
        self.assertIn((2, '/anything'), self.remaining())
        self.assertIn((3, '/photos/@eadir/nested'), self.remaining())

    def test_absent_lists_are_safe_and_independent(self):
        self.sql.execute('DROP TABLE excluded_names')
        self.assertEqual(self.db.purge_directories_from_exclusion_lists(), 4)
        self.sql.execute('DROP TABLE live_excluded_absdirs')
        self.assertEqual(self.db.purge_directories_from_exclusion_lists(), 0)

    def test_purge_triples_timeouts_and_restores_after_success_or_failure(self):
        db = DataBaseCLI(mock.Mock())
        for previous in (1, 2):
            db._timeout_multiplier = previous
            with mock.patch.object(db, '_purge_directories_from_exclusion_lists') as purge:
                def succeed(hostid):
                    self.assertEqual(db._timeout_multiplier, previous * 3)
                    self.assertEqual(hostid, 2)
                    return 42
                purge.side_effect = succeed
                self.assertEqual(db.purge_directories_from_exclusion_lists(2), 42)
                self.assertEqual(db._timeout_multiplier, previous)
                purge.side_effect = RuntimeError('query timed out')
                with self.assertRaises(RuntimeError):
                    db.purge_directories_from_exclusion_lists(2)
                self.assertEqual(db._timeout_multiplier, previous)

    def test_purge_connection_and_session_limits_are_tripled(self):
        provider = mock.Mock()
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        provider.connect.return_value = connection
        connection.cursor.return_value.fetchone.return_value = (1,)
        connection.cursor.return_value.rowcount = 7
        connection.cursor.return_value.fetchall.side_effect = [[(1,)], []]
        db = DataBaseCLI(provider)
        db.database = 'testdb'
        self.assertEqual(db.purge_directories_from_exclusion_lists(2), 7)
        provider.connect.assert_called_once_with(timeout_multiplier=3)
        connection.cursor.return_value.execute.assert_any_call(
            'SET SESSION max_statement_time = @@SESSION.max_statement_time * %s', (3,))
        self.assertEqual(db._timeout_multiplier, 1)

    def test_no_inventory_hosts_performs_no_deletes(self):
        self.sql.execute('DELETE FROM directories')
        self.sql.commit()
        self.assertEqual(self.db.purge_directories_from_exclusion_lists(), 0)
        self.assertFalse(any(query.startswith('DELETE') for query, params in self.statements))

    def test_sequential_commits_and_per_host_counts(self):
        provider = mock.Mock()
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        provider.connect.return_value = connection
        provider.list_directory_hostids.return_value = [0, 2, 7]
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (1,)
        cursor.fetchall.side_effect = [[(1,)], [], [(2,)], [], [(3,)], []]
        type(cursor).rowcount = mock.PropertyMock(side_effect=[4, 0, 3])
        events = []
        cursor.execute.side_effect = lambda query, params=(): events.append(('sql', params)) if query.startswith('DELETE') else None
        connection.commit.side_effect = lambda: events.append(('commit',))
        db = DataBaseCLI(provider)
        db.database = 'testdb'
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(db.purge_directories_from_exclusion_lists(), 7)
        provider.list_directory_hostids.assert_called_once_with(cursor=cursor, include_jobs=False)
        self.assertEqual(events, [('sql', (0, 1)), ('commit',), ('commit',),
                                 ('sql', (2, 2)), ('commit',), ('commit',),
                                 ('sql', (7, 3)), ('commit',), ('commit',)])
        self.assertIn('Host 0: 4', output.getvalue())
        self.assertIn('Host 2: 0', output.getvalue())
        self.assertIn('Host 7: 3', output.getvalue())

    def test_large_sparse_host_is_scanned_in_bounded_id_windows(self):
        self.sql.execute('DELETE FROM directories')
        self.sql.executemany('INSERT INTO directories (dirid,hostid,path) VALUES (?,1,?)',
                             [(index, '/photos/@eadir/item' + str(index) if index % 10 == 0
                               else '/photos/keep' + str(index)) for index in range(1, 1202)])
        self.sql.commit()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(self.db.purge_directories_from_exclusion_lists(), 120)
        deletes = [(query, params) for query, params in self.statements if query.startswith('DELETE')]
        self.assertEqual([len(params) - 1 for query, params in deletes], [500, 500, 201])
        self.assertEqual([params[1] for query, params in deletes], [1, 501, 1001])
        reads = [(query, params) for query, params in self.statements if query.startswith('SELECT dirid')]
        self.assertEqual([params[1] for query, params in reads], [0, 500, 1000, 1201])
        self.assertTrue(all('EXISTS' not in query and params[-1] == 500 for query, params in reads))
        self.assertEqual(self.sql.execute('SELECT COUNT(*) FROM directories').fetchone()[0], 1081)
        self.assertIn('bis DIRID 1201 geprüft; 120 gelöscht', output.getvalue())

    def test_uncertain_commit_is_not_retried_and_original_error_survives_rollback(self):
        db = DataBaseCLI(object.__new__(app.DatabaseConnection))
        db.database = 'testdb'
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (1,)
        cursor.fetchall.side_effect = [[(1,)], [(2,)]]
        cursor.rowcount = 1
        error = app.Error(msg='Lost connection during commit', errno=2055)
        connection.commit.side_effect = [None, error]
        connection.rollback.side_effect = RuntimeError('rollback connection also lost')
        with mock.patch.object(db, 'connect', return_value=connection), \
                contextlib.redirect_stdout(io.StringIO()), self.assertLogs(level='WARNING'):
            with self.assertRaises(app.Error) as raised:
                db.purge_directories_from_exclusion_lists(2)
        self.assertIs(raised.exception, error)
        deletes = [call for call in cursor.execute.call_args_list if call.args[0].startswith('DELETE')]
        self.assertEqual([call.args[1] for call in deletes], [(2, 1), (2, 2)])
        self.assertEqual(connection.commit.call_count, 2)
        self.assertEqual(db._timeout_multiplier, 1)

    def test_dispatch_prints_deleted_count(self):
        for prefix, expected in (([], None), (['--hostid', '0'], 0)):
            args = ClientArgumentParser().parse(prefix + ['--dirs-purge-from-exclusion-lists'])
            client = object.__new__(client_operations)
            client.args = args
            client.database = mock.Mock()
            client.database.purge_directories_from_exclusion_lists.return_value = 42
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(client.run(), 0)
            client.database.purge_directories_from_exclusion_lists.assert_called_once_with(hostid=expected)
            self.assertIn('42 ausgeschlossene Directory-Datensätze gelöscht.', output.getvalue())


# unused_jobs regression coverage.


class TestUnusedJobs(unittest.TestCase):
    def test_runid_and_finished_links_runnable_jobs_and_sources_are_protected(self):
        sql=sqlite3.connect(':memory:')
        self.addCleanup(sql.close)
        sql.executescript('''
            CREATE TABLE jobs (jobid INTEGER,hostid INTEGER,task TEXT,status TEXT,
                startdirname TEXT,timeend TEXT,source_jobid INTEGER);
            CREATE TABLE directories (runid INTEGER,finished INTEGER);
            CREATE TABLE jobsrunid (runid INTEGER,jobid INTEGER);
        ''')
        jobs=[(1,'closed',None),(2,'closed',None),(3,'closed',None),(4,'active',None),
              (5,'paused',None),(6,'error',None),(7,'stopped',None),(8,'closed',None),
              (9,'pending',8),(10,'insert',None)]
        sql.executemany("INSERT INTO jobs VALUES (?,0,'scan',?,'/root',NULL,?)",jobs)
        sql.executemany('INSERT INTO jobsrunid VALUES (?,?)',[(100,1),(101,1),(200,2),(300,3)])
        sql.execute('INSERT INTO directories VALUES (101,200)')
        db=object.__new__(app.DatabaseConnection)
        self.assertEqual([row['jobid'] for row in db.list_unused_jobs(cursor=sql.cursor())],[3,6,7])

    def test_confirmation_defaults_yes_but_no_and_eof_cancel(self):
        for answer,delete in (('',True),('y',True),('Y',True),('n',False),('other',False),(EOFError,False)):
            with self.subTest(answer=answer):
                client=object.__new__(client_operations)
                client.args=ClientArgumentParser().parse(['--hostid','2','--jobs-remove-unused'])
                client.database=mock.Mock()
                client.database.list_unused_jobs.return_value=[dict(jobid=3,hostid=2,task='scan',status='closed',startdirname='/root')]
                client.database.remove_unused_jobs.return_value=1
                patch=mock.patch('builtins.input',side_effect=EOFError) if answer is EOFError else mock.patch('builtins.input',return_value=answer)
                with patch as prompt, contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(client.run(),0)
                self.assertIn('[Yn]',prompt.call_args.args[0])
                client.database.list_unused_jobs.assert_called_once_with(hostid=2)
                if delete: client.database.remove_unused_jobs.assert_called_once_with([3])
                else: client.database.remove_unused_jobs.assert_not_called()

    def test_rechecks_only_approved_ids_and_keeps_newly_used_jobs(self):
        db=DataBaseCLI(object.__new__(app.DatabaseConnection)); db.database='testdb'
        connection=mock.MagicMock(); connection.__enter__.return_value=connection
        cursor=connection.cursor.return_value
        cursor.fetchone.return_value=(1,)
        cursor.rowcount=1
        with mock.patch.object(db,'connect',return_value=connection), \
                mock.patch.object(db,'list_unused_jobs',return_value=[dict(jobid=3),dict(jobid=99)]) as check, \
                mock.patch.object(db,'_work_tables',return_value=['tmp2_job_work']):
            self.assertEqual(db.remove_unused_jobs([3,4]),1)
        check.assert_called_once_with(cursor=cursor,for_update=True)
        deletes=[call for call in cursor.execute.call_args_list if call.args[0].startswith('DELETE')]
        self.assertEqual(len(deletes),3)
        self.assertTrue(all(call.args[1]==(3,) for call in deletes))
        connection.commit.assert_called_once_with()


# Binarycheck freshness regression coverage.


class ComparisonFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.database = DataBaseCLI(mock.Mock())
        self.cursor = mock.Mock()
        self.scan = datetime(2026, 11, 1)
        self.start = self.scan + timedelta(days=1)
        self.finish = self.start + timedelta(minutes=1)

    def history(self, host=0, remote=0, left=1, right=2, status='equal',
                job_status='closed', scan=None, hashed=None):
        return (host, remote, left, right, status, self.finish, job_status,
                self.start, self.scan if scan is None else scan, hashed,
                self.scan, hashed)

    def filter(self, rows, comparisons=None, confirm=None, before=None):
        self.cursor.fetchall.return_value = rows
        with contextlib.redirect_stdout(io.StringIO()):
            return self.database._filter_binarycheck_repeats(
                self.cursor, {0: {(0, 1, 2)}} if comparisons is None else comparisons,
                confirm, before)

    def test_fresh_completed_pair_skipped_and_old_results_backfill_timestamp(self):
        self.assertEqual(self.filter([self.history()]), {})
        updates = [call for call in self.cursor.execute.call_args_list
                   if call.args[0].startswith('UPDATE')]
        self.assertEqual(len(updates), 2)
        self.assertIn('scanned_at=%s', updates[0].args[0])
        self.assertEqual(updates[0].args[1], (self.finish, 0, 1, self.scan, self.finish))

    def test_existing_fresh_hash_needs_no_backfill(self):
        self.assertEqual(self.filter([self.history(hashed=self.finish)]), {})
        self.assertEqual(self.cursor.execute.call_count, 1)

    def test_new_scan_after_comparison_started_requires_new_comparison(self):
        result = self.filter([self.history(scan=self.start + timedelta(seconds=1))])
        self.assertEqual(result, {0: {(0, 1, 2)}})

    def test_new_partner_is_not_skipped_by_other_completed_pair(self):
        result = self.filter([self.history(hashed=self.finish)], {0: {(0, 1, 3)}})
        self.assertEqual(result, {0: {(0, 1, 3)}})

    def test_reverse_cross_host_pair_is_recognized(self):
        result = self.filter([self.history(host=0, remote=1, left=1, right=20)],
                             {1: {(0, 20, 1)}})
        self.assertEqual(result, {})

    def test_queued_pair_wins_over_completed_and_cannot_be_repeated(self):
        confirm = mock.Mock(return_value=True)
        result = self.filter([self.history(), self.history(status='pending', job_status='active')],
                             confirm=confirm)
        self.assertEqual(result, {})
        confirm.assert_not_called()

    def test_failed_unreadable_pair_can_be_retried(self):
        self.assertEqual(self.filter([self.history(status='unreadable', job_status='error')]),
                         {0: {(0, 1, 2)}})

    def test_global_then_per_host_consent_and_commit_before_prompt(self):
        events = []
        answers = iter([True, False, True])
        def confirm(host, count):
            events.append((host, count))
            return next(answers)
        result = self.filter([self.history(status='different'),
                              self.history(host=1, remote=1, left=10, right=11)],
                             {0: {(0, 1, 2)}, 1: {(1, 10, 11)}}, confirm,
                             lambda: events.append('commit'))
        self.assertEqual(events, ['commit', (None, 2), (0, 1), (1, 1)])
        self.assertEqual(result, {1: {(1, 10, 11)}})

    def test_global_no_skips_host_prompts(self):
        confirm = mock.Mock(return_value=False)
        self.assertEqual(self.filter([self.history()], confirm=confirm), {})
        confirm.assert_called_once_with(None, 1)


if __name__ == '__main__':
    unittest.main()
