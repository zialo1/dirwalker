"""Runtime scan, rescan and Binarycheck tests for version25.py.

Run with python -m unittest test_version25 or pytest. Unit tests use mocked
database handles or SQLite inventories; process tests exercise spawn and signals.
"""

from unittest import mock
import DataBaseCore as core
# Importing a test module must not probe the configured production database.
with mock.patch.object(core.DataBaseConnectionCore, 'connect', side_effect=RuntimeError('offline test')):
    import version25 as app
import contextlib
from datetime import datetime
import io
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from DataBaseCLI import DataBaseCLI
from client_operations import ClientArgumentParser
from client_operations import client_operations
from types import SimpleNamespace
import DataBaseCore
import server_operations as server
import socket
import threading
from datetime import timedelta
import version25
from DataBaseCore import DataBaseConnectionCore


class TestMarkedRescan(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.sql = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
        self.addCleanup(self.sql.close)
        columns = app.DatabaseConnection.EXPECTED_TABLE_SHAPES['directories']['columns']
        definitions = []
        for name, shape in columns.items():
            kind = ('INTEGER PRIMARY KEY' if name == 'dirid' else
                    'TIMESTAMP' if name == 'files_lastwrite' else
                    'INTEGER' if 'int' in shape[0] else 'TEXT')
            definitions.append(name + ' ' + kind)
        self.sql.execute('CREATE TABLE directories (' + ','.join(definitions) + ')')
        self.sql.execute('CREATE TABLE jobsrunid (runid INTEGER, jobid INTEGER)')
        self.sql.execute('CREATE TABLE jobs (jobid INTEGER PRIMARY KEY, userhost TEXT, hostid INTEGER, '
                         'startdirname TEXT, task TEXT, status TEXT, stage TEXT, skipdirs TEXT, '
                         'scanall INTEGER, timecreated TIMESTAMP, timeupdated TIMESTAMP, last_activity TIMESTAMP)')
        self.sql.execute("INSERT INTO jobs (jobid,hostid,skipdirs) VALUES (1,2,'ignored')")
        self.sql.execute('INSERT INTO jobsrunid VALUES (9,1)')
        self.sql.execute('CREATE TABLE excluded_names (name TEXT)')
        self.sql.execute('CREATE TABLE hosts (hostid INTEGER PRIMARY KEY, machine TEXT)')
        self.sql.executemany('INSERT INTO hosts VALUES (?,?)', [(0, 'nas'), (2, 'laptop'), (3, 'desktop')])
        sql = self.sql

        class Cursor:
            def __init__(self):
                self.inner = sql.cursor()
            def execute(self, query, params=()):
                if 'information_schema.tables' in query:
                    query = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?"
                    params = (params[-1],)
                query = query.replace('ORDER BY BINARY name', 'ORDER BY name')
                return self.inner.execute(query.replace('%s', '?'), params)
            def executemany(self, query, params):
                return self.inner.executemany(query.replace('%s', '?'), params)
            def fetchone(self):
                return self.inner.fetchone()
            def fetchall(self):
                return self.inner.fetchall()
            @property
            def lastrowid(self):
                return self.inner.lastrowid
            def close(self):
                self.inner.close()

        class Connection:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def cursor(self):
                return Cursor()
            def commit(self):
                sql.commit()
            def rollback(self):
                sql.rollback()

        self.db = DataBaseCLI(object.__new__(app.DatabaseConnection))
        self.db.connection.database = "testdb"
        self.patch = mock.patch.object(self.db.connection, 'connect', return_value=Connection())
        self.patch.start()
        self.addCleanup(self.patch.stop)
        patch = mock.patch.object(self.db.connection, 'list_live_excluded_absdirs', return_value=[])
        patch.start()
        self.addCleanup(patch.stop)

    def add(self, relative, parent, marked=False, host=2):
        path = self.root / relative
        path.mkdir(exist_ok=True)
        (path / 'file').write_bytes(b'x')
        os.utime(path / 'file', (100, 100))
        self.sql.execute('INSERT INTO directories '
                         '(path, parentdir, root_path, hostid, runid, finished, files_lastwrite, '
                         'rescan_from_here, file_list_sha256, files_sha256, subdirs_sha256, '
                         'combined_sha256, subdirectory_hash, subtree_structure, zero_content_only) '
                         "VALUES (?,?,?,?,9,9,?,?,'list','files','subdirs','combined','subtree','structure',0)",
                         (str(path), parent, str(self.root), host,
                          datetime(1970, 1, 1, 0, 1, 40), marked))
        dirid = self.sql.execute('SELECT last_insert_rowid()').fetchone()[0]
        self.sql.execute('UPDATE directories SET localfilesize=1, subdircount=0 WHERE dirid=?', (dirid,))
        if parent:
            self.sql.execute('UPDATE directories SET subdircount=COALESCE(subdircount,0)+1 WHERE dirid=?', (parent,))
        return dirid

    def hashes(self, dirid):
        return self.sql.execute('SELECT files_sha256, combined_sha256, rescan_from_here '
                                'FROM directories WHERE dirid=?', (dirid,)).fetchone()

    def jobs(self):
        return self.sql.execute('SELECT startdirname, stage, scanall FROM jobs WHERE jobid != 1').fetchall()

    def test_topmost_marks_unchanged_tree_keeps_all_hashes(self):
        root = self.add('', 0, True)
        child = self.add('child', root, True)
        # Excluded later timestamp, special entries and symlinks cannot invalidate hashes.
        (self.root / 'ignored').write_bytes(b'ignore')
        (self.root / 'link').symlink_to(self.root / 'ignored')
        os.mkfifo(self.root / 'pipe')
        self.sql.commit()
        self.assertEqual(self.db.directories_rescan_marked(2), (1, 0))
        self.assertEqual(self.hashes(root), ('files', 'combined', 0))
        self.assertEqual(self.hashes(child), ('files', 'combined', 0))
        self.assertEqual(self.jobs(), [])

    def test_changed_descendant_clears_ancestors_but_preserves_sibling(self):
        root = self.add('', 0, True)
        child = self.add('child', root, True)
        sibling = self.add('sibling', root)
        os.utime(self.root / 'child' / 'file', (200, 200))
        self.sql.commit()
        self.assertEqual(self.db.directories_rescan_marked(2), (1, 1))
        self.assertEqual(self.hashes(child), (None, None, 0))
        self.assertEqual(self.hashes(root), ('files', None, 0))
        self.assertEqual(self.hashes(sibling), ('files', 'combined', 0))
        value = self.sql.execute('SELECT files_lastwrite, finished FROM directories WHERE dirid=?', (child,)).fetchone()
        self.assertEqual(value, (datetime(1970, 1, 1, 0, 3, 20), 0))
        self.assertEqual(self.jobs(), [(str(self.root), 'detailed', 1)])

    def test_new_and_removed_directories_and_unmarked_parent(self):
        root = self.add('', 0)
        branch = self.add('branch', root, True)
        stale = self.add('branch/stale', branch)
        (self.root / 'branch' / 'stale' / 'file').unlink()
        (self.root / 'branch' / 'stale').rmdir()
        new = self.root / 'branch' / 'new'
        new.mkdir()
        (new / 'file').write_bytes(b'new')
        self.sql.commit()
        self.assertEqual(self.db.directories_rescan_marked(2), (1, 1))
        self.assertIsNone(self.hashes(stale))
        row = self.sql.execute('SELECT parentdir, files_lastwrite, files_sha256 FROM directories WHERE path=?',
                               (str(new),)).fetchone()
        self.assertEqual(row[0], branch)
        self.assertIsNotNone(row[1])
        self.assertIsNone(row[2])
        self.assertEqual(self.hashes(root), ('files', None, 0))

    def test_disjoint_marked_roots_share_one_detailed_job(self):
        root = self.add('', 0)
        first = self.add('branch', root, True)
        second = self.add('branch-extra', root, True)
        os.utime(self.root / 'branch' / 'file', (200, 200))
        os.utime(self.root / 'branch-extra' / 'file', (300, 300))
        self.sql.commit()
        self.assertEqual(self.db.directories_rescan_marked(2), (2, 1))
        self.assertEqual(self.hashes(first), (None, None, 0))
        self.assertEqual(self.hashes(second), (None, None, 0))
        self.assertEqual(len(self.jobs()), 1)

    def test_later_failure_rolls_back_timestamp_and_preserves_all_marks(self):
        root = self.add('', 0, True)
        child = self.add('child', root, True)
        os.utime(self.root / 'file', (200, 200))
        self.sql.commit()
        scandir = os.scandir
        def fail_child(path):
            if path == str(self.root / 'child'):
                raise OSError('disk I/O failed')
            return scandir(path)
        with mock.patch('DataBaseCLI.os.scandir', side_effect=fail_child):
            with self.assertRaises(OSError):
                self.db.directories_rescan_marked(2)
        self.assertEqual(self.hashes(root), ('files', 'combined', 1))
        self.assertEqual(self.hashes(child), ('files', 'combined', 1))
        value = self.sql.execute('SELECT files_lastwrite FROM directories WHERE dirid=?', (root,)).fetchone()[0]
        self.assertEqual(value, datetime(1970, 1, 1, 0, 1, 40))
        self.assertEqual(self.jobs(), [])

    def test_failure_keeps_marks_hashes_and_queue(self):
        root = self.add('', 0, True)
        self.sql.commit()
        with mock.patch('DataBaseCore.os.scandir', side_effect=PermissionError), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.db.directories_rescan_marked(2), (1, 0))
        self.assertEqual(self.hashes(root), ('files', 'combined', 1))
        self.assertEqual(self.jobs(), [])

    def test_unreadable_branch_preserves_descendants_and_other_branches_continue(self):
        root = self.add('', 0, True)
        denied = self.add('denied', root)
        descendant = self.add('denied/descendant', denied, True)
        sibling = self.add('sibling', root, True)
        os.utime(self.root / 'sibling' / 'file', (200, 200))
        self.sql.commit()
        scandir = os.scandir
        def deny_branch(path):
            if path == str(self.root / 'denied'):
                raise PermissionError(13, 'Permission denied', path)
            return scandir(path)
        output = io.StringIO()
        with mock.patch('DataBaseCore.os.scandir', side_effect=deny_branch), \
                contextlib.redirect_stdout(output):
            self.assertEqual(self.db.directories_rescan_marked(2), (1, 1))
        self.assertEqual(self.hashes(denied), ('files', 'combined', 1))
        self.assertEqual(self.hashes(descendant), ('files', 'combined', 1))
        self.assertEqual(self.hashes(sibling), (None, None, 0))
        self.assertEqual(self.hashes(root), ('files', None, 1))
        self.assertIn('nicht lesbar', output.getvalue())

    def test_third_permission_failure_prunes_descendants_and_retains_boundary(self):
        root = self.add('', 0, True)
        denied = self.add('denied', root)
        child = self.add('denied/child', denied, True)
        grandchild = self.add('denied/child/grandchild', child)
        sibling = self.add('sibling', root)
        self.sql.commit()
        scandir = os.scandir
        def deny(path):
            if path == str(self.root / 'denied'):
                raise PermissionError(path)
            return scandir(path)
        for failures in (1, 2, 3):
            with mock.patch('DataBaseCore.os.scandir', side_effect=deny), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.db.directories_rescan_marked(2)
            count = self.sql.execute('SELECT read_failures FROM directories WHERE dirid=?', (denied,)).fetchone()[0]
            self.assertEqual(count, failures)
            self.assertIsNotNone(self.hashes(denied))
            self.assertEqual(self.hashes(denied)[2], 1)
            self.assertEqual(self.hashes(sibling), ('files', 'combined', 0))
            if failures < 3:
                self.assertIsNotNone(self.hashes(child))
                self.assertIsNotNone(self.hashes(grandchild))
            else:
                self.assertIsNone(self.hashes(child))
                self.assertIsNone(self.hashes(grandchild))
                self.assertIsNone(self.hashes(denied)[0])
                self.assertIsNone(self.hashes(root)[1])
        self.assertTrue((self.root / 'denied/child/grandchild/file').exists())

    def test_successful_rescan_resets_permission_failure_counter(self):
        root = self.add('', 0, True)
        self.sql.commit()
        with mock.patch('DataBaseCore.os.scandir', side_effect=PermissionError), \
                contextlib.redirect_stdout(io.StringIO()):
            self.db.directories_rescan_marked(2)
        self.assertEqual(self.sql.execute('SELECT read_failures FROM directories WHERE dirid=?', (root,)).fetchone()[0], 1)
        self.db.directories_rescan_marked(2)
        self.assertEqual(self.sql.execute('SELECT read_failures FROM directories WHERE dirid=?', (root,)).fetchone()[0], 0)
        self.assertEqual(self.hashes(root), ('files', 'combined', 0))

    def test_current_global_exclusions_skip_old_marked_recycle_and_git_paths(self):
        root = self.add('', 0)
        recycle = self.add('#recycle', root, True)
        git = self.add('#recycle/.git', recycle, True)
        self.sql.executemany('INSERT INTO excluded_names VALUES (?)', [('#recycle',), ('.git',)])
        self.sql.commit()
        with mock.patch('DataBaseCore.os.scandir', side_effect=AssertionError('Excluded path opened')):
            self.assertEqual(self.db.directories_rescan_marked(2), (1, 0))
        self.assertEqual(self.hashes(recycle), ('files', 'combined', 1))
        self.assertEqual(self.hashes(git), ('files', 'combined', 1))
        self.assertEqual(self.jobs(), [])

    def test_other_hosts_and_no_marks(self):
        root = self.add('', 0, True, host=3)
        self.sql.commit()
        self.assertEqual(self.db.directories_rescan_marked(2), (0, 0))
        self.assertEqual(self.hashes(root), ('files', 'combined', 1))

    def test_enqueue_only_writes_job_and_keeps_marks_and_hashes(self):
        root = self.add('', 0, True)
        self.sql.commit()
        with mock.patch('os.scandir', side_effect=AssertionError('Client must not scan')):
            jobid = self.db.enqueue_directories_rescan_marked(2)
        row = self.sql.execute('SELECT task, status, stage, hostid FROM jobs WHERE jobid=?',
                               (jobid,)).fetchone()
        self.assertEqual(row, ('rescan', 'insert', 'coarse', 2))
        self.assertEqual(self.hashes(root), ('files', 'combined', 1))

    def test_server_child_dispatches_marked_worker_without_client_module(self):
        import server_operations as server
        runtime = mock.Mock()
        runtime._live_excluded_absdirs = {}
        runtime.database_connection.directories_rescan_marked.return_value = (1, 1)
        with mock.patch.object(server, '_runtime', return_value=runtime), \
                mock.patch.object(server.multiprocessing, 'parent_process', return_value=None), \
                mock.patch.object(server.signal, 'signal'), contextlib.redirect_stdout(io.StringIO()):
            server.server_operations._child_entry(
                'rescan', '<marked-directories>', 17,
                task='rescan', hostid=2)
        runtime.database_connection.directories_rescan_marked.assert_called_once_with(hostid=2)
        runtime.coarse_scan.assert_not_called()
        runtime.detailed_scan.assert_not_called()
        runtime.database_connection.stop_wake_monitor.assert_called_once_with()

    def test_process_job_closes_rescan_without_normal_coarse_transition(self):
        job = dict(jobid=17, hostid=2, task='rescan',
                   startdirname='<marked-directories>')
        for outcome, status in (('ok', 'closed'), ('paused', 'paused'), ('error', 'error')):
            with self.subTest(outcome=outcome):
                server = mock.Mock()
                server.run_spawn.return_value = outcome
                server.worker_error = 'worker failed'
                with mock.patch.object(app, 'set_job_status') as set_status, \
                        mock.patch('os.scandir', side_effect=AssertionError('Synthetic root')), \
                        contextlib.redirect_stdout(io.StringIO()):
                    app.process_job(job, 'coarse', server)
                server.run_spawn.assert_called_once_with(
                    'rescan', '<marked-directories>', 17,
                    task='rescan', hostid=2)
                self.assertEqual(set_status.call_args.args[:2], (17, status))
                self.assertEqual(set_status.call_args.kwargs['hostid'], 2)

    def test_check_modifications_enqueues_all_registered_hosts_without_scanning(self):
        root = self.add('', 0, True)
        self.sql.commit()
        with mock.patch('os.scandir', side_effect=AssertionError('Client must not scan')):
            created = self.db.enqueue_check_modifications()
        self.assertEqual([host for host, job in created], [0, 2, 3])
        jobs = self.sql.execute('SELECT hostid,userhost,task,status,stage FROM jobs WHERE jobid != 1 '
                                'ORDER BY hostid').fetchall()
        self.assertEqual(jobs, [(host, machine, 'check-modifications', 'insert', 'coarse')
                                for host, machine in ((0, 'nas'), (2, 'laptop'), (3, 'desktop'))])
        self.assertEqual(self.hashes(root), ('files', 'combined', 1))

    def test_check_modifications_explicit_host_and_missing_host(self):
        self.sql.commit()
        created = self.db.enqueue_check_modifications(2)
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0][0], 2)
        with self.assertRaises(ValueError):
            self.db.enqueue_check_modifications(99)
        self.assertEqual(len(self.jobs()), 1)

    def test_check_modifications_without_registered_hosts(self):
        self.sql.execute('DELETE FROM hosts')
        self.sql.commit()
        self.assertEqual(self.db.enqueue_check_modifications(), [])
        self.assertEqual(self.jobs(), [])

    def test_check_dispatch_omitted_host_differs_from_explicit_zero(self):
        for argv, expected in ((['--check-modifications'], None),
                               (['--hostid', '0', '--check-modifications'], 0)):
            with self.subTest(argv=argv):
                client = object.__new__(client_operations)
                client.args = ClientArgumentParser().parse(argv)
                client.database = mock.Mock()
                client.database.enqueue_check_modifications.return_value = [(0, 17)]
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(client.run(), 0)
                client.database.enqueue_check_modifications.assert_called_once_with(hostid=expected)
                client.database.check_modifications.assert_not_called()

    def test_server_child_runs_modification_check(self):
        import server_operations as server
        runtime = mock.Mock()
        runtime._live_excluded_absdirs = {}
        runtime.database_connection.check_modifications.return_value = 3
        with mock.patch.object(server, '_runtime', return_value=runtime), \
                mock.patch.object(server.multiprocessing, 'parent_process', return_value=None), \
                mock.patch.object(server.signal, 'signal'), contextlib.redirect_stdout(io.StringIO()):
            server.server_operations._child_entry(
                'check-modifications', '<check-modifications>', 17,
                task='check-modifications', hostid=2)
        runtime.database_connection.check_modifications.assert_called_once_with(hostid=2)
        runtime.coarse_scan.assert_not_called()
        runtime.detailed_scan.assert_not_called()
        runtime.database_connection.stop_wake_monitor.assert_called_once_with()

    def test_process_check_job_closes_and_recovers_network_failure(self):
        job = dict(jobid=17, hostid=2, task='check-modifications',
                   startdirname='<check-modifications>')
        for outcome in ('ok', 'network_retry', 'stopped'):
            with self.subTest(outcome=outcome):
                server = mock.Mock()
                server.run_spawn.return_value = outcome
                with mock.patch.object(app, 'set_job_status') as status, \
                        mock.patch('os.scandir', side_effect=AssertionError('Synthetic root')), \
                        contextlib.redirect_stdout(io.StringIO()):
                    app.process_job(job, 'coarse', server)
                server.run_spawn.assert_called_once_with(
                    'check-modifications', '<check-modifications>', 17,
                    task='check-modifications', hostid=2)
                if outcome == 'ok':
                    status.assert_called_once_with(17, 'closed', hostid=2)
                elif outcome == 'network_retry':
                    status.assert_not_called()
                    server.recover_network_job.assert_called_once_with(17, 2)
                else:
                    status.assert_not_called()

    def test_worker_exception_details_reach_job_error(self):
        import server_operations as server_module
        from types import SimpleNamespace
        runtime = mock.Mock()
        runtime._live_excluded_absdirs = {}
        error = PermissionError(13, 'Permission denied', '/private/photos')
        runtime.database_connection.check_modifications.side_effect = error
        report = SimpleNamespace(value=b'')
        with mock.patch.object(server_module, '_runtime', return_value=runtime), \
                mock.patch.object(server_module.multiprocessing, 'parent_process', return_value=None), \
                mock.patch.object(server_module.signal, 'signal'), \
                mock.patch.object(server_module.traceback, 'print_exc'):
            with self.assertRaises(PermissionError):
                server_module.server_operations._child_entry(
                    'check-modifications', '<check-modifications>', 40,
                    task='check-modifications', hostid=0, error_report=report)
        message = report.value.decode('utf-8')
        self.assertIn('PermissionError', message)
        self.assertIn('/private/photos', message)
        coordinator = mock.Mock()
        coordinator.run_spawn.return_value = 'error'
        coordinator.worker_error = message
        job = dict(jobid=40, hostid=0, task='check-modifications')
        with mock.patch.object(app, 'set_job_status') as status, \
                contextlib.redirect_stdout(io.StringIO()):
            app.process_job(job, 'coarse', coordinator)
        status.assert_called_once_with(40, 'error', message, hostid=0)
        coordinator.log_job_error.assert_called_once_with(40, message)

    def test_rescan_clears_parent_hash_when_a_child_is_still_unfinished(self):
        root = self.add('', 0, True)
        child = self.add('child', root)
        self.sql.execute('UPDATE directories SET files_sha256=NULL,combined_sha256=NULL,localfilesize=NULL WHERE dirid=?', (child,))
        self.sql.commit()
        self.db.directories_rescan_marked(2)
        self.assertIsNone(self.hashes(root)[1])
        self.assertIsNone(self.hashes(child)[1])

    def test_rebuild_only_hashes_nodes_with_complete_children(self):
        import hashlib
        empty = hashlib.sha256().hexdigest()
        for incomplete in (True, False):
            with self.subTest(incomplete=incomplete):
                database = object.__new__(app.DatabaseConnection)
                reader, writer = mock.MagicMock(), mock.MagicMock()
                reader.cursor.return_value.fetchone.return_value = (1,)
                reader.cursor.return_value.fetchmany.side_effect = [[
                    (2, 1, 1, 1, 'child', None if incomplete else 0,
                     None if incomplete else empty, 0, 0),
                    (1, 0, 0, 0, 'root', 0, empty, 1, 1)], []]
                with mock.patch.object(database, 'connect', side_effect=[reader, writer]), \
                        mock.patch.object(database, 'refresh_zero_content_flags'):
                    database.rebuild_subtree_statistics('/root', hostid=2)
                updates = writer.cursor.return_value.execute.call_args_list
                self.assertEqual(len(updates), 2)
                if incomplete:
                    self.assertTrue(all('combined_sha256 = NULL' in call.args[0] for call in updates))
                else:
                    self.assertTrue(all('combined_sha256 = %s' in call.args[0] for call in updates))
                    self.assertTrue(all(call.args[1][3] is not None for call in updates))

    def test_marked_rescan_removes_consumed_legacy_report_rows_only(self):
        root=self.add('',0,True)
        self.sql.execute('CREATE TABLE tmp_modified (batch_jobid INTEGER,hostid INTEGER,dirid INTEGER)')
        self.sql.executemany('INSERT INTO tmp_modified VALUES (?,?,?)',[(0,2,root),(99,2,root),(0,3,root)])
        self.sql.commit()
        self.db.directories_rescan_marked(2)
        self.assertEqual(self.sql.execute('SELECT * FROM tmp_modified ORDER BY batch_jobid,hostid').fetchall(),
                         [(0,3,root),(99,2,root)])

    def test_parser_and_dispatch(self):
        args = ClientArgumentParser().parse(['--hostid', '2', '--directories-rescan-marked'])
        client = object.__new__(client_operations)
        client.args = args
        client.database = mock.Mock()
        client.database.enqueue_directories_rescan_marked.return_value = 17
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(client.run(), 0)
        client.database.enqueue_directories_rescan_marked.assert_called_once_with(hostid=2)
        client.database.directories_rescan_marked.assert_not_called()


# Scan and Binarycheck regression coverage.


class PriorityTests(unittest.TestCase):
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

    def test_poll_interval_and_host_job_scope(self):
        worker, db, connection, cursor = self.worker()
        cursor.fetchall.side_effect = [[('dirid',), ('hostid',), ('jobid',)], [(8,)]]
        with mock.patch.object(app.time, 'monotonic', side_effect=[10, 129, 130]):
            worker._refresh_priority(4, 1)
            self.assertEqual(worker._priority_ids, {8})
            worker._refresh_priority(4, 1)
            self.assertEqual(db.connect.call_count, 1)
            cursor.fetchall.side_effect = [[('dirid',)], []]
            worker._refresh_priority(4, 1)
        self.assertEqual(db.connect.call_count, 2)
        self.assertEqual(worker._priority_ids, set())
        self.assertEqual(cursor.execute.call_args_list[1].args,
                         ('SELECT dirid FROM `tmp_highpriority` WHERE `hostid` = %s AND `jobid` = %s', (1, 4)))
        self.assertTrue(all(call.args[0].startswith(('SHOW', 'SELECT'))
                            for call in cursor.execute.call_args_list))

    def test_missing_or_dropped_table_clears_hints(self):
        for fail_on_select in (False, True):
            worker, db, connection, cursor = self.worker()
            worker._priority_ids = {8}
            error = app.Error()
            error.errno = 1146
            cursor.execute.side_effect = [None, error] if fail_on_select else error
            cursor.fetchall.return_value = [('dirid',)]
            worker._refresh_priority(4, 1)
            self.assertEqual(worker._priority_ids, set())
            cursor.close.assert_called_once()

    def test_other_database_errors_propagate(self):
        worker, db, connection, cursor = self.worker()
        cursor.execute.side_effect = app.Error()
        with self.assertRaises(app.Error):
            worker._refresh_priority(4, 1)

    def test_priority_first_normal_fallback_and_existing_persistence(self):
        for priority, expected in ((set(), [7, 9]), ({99}, [7, 9])):
            worker, db, connection, cursor = self.worker()
            cursor.fetchone.return_value = (99, 1)
            cursor.fetchall.side_effect = [[
                (7, 8, '/a', '/b', 'x', 'y', 1, 0, 1, 0),
                (9, 10, '/c', '/d', 'x', 'y', 1, 0, 1, 0)], []]
            def refresh(*args):
                worker._priority_ids = priority
            with mock.patch.object(worker, '_refresh_priority', side_effect=refresh):
                worker.loop(4, hostid=1)
            self.assertEqual([call.args[3] for call in db._retry_on_lock_error.call_args_list], expected)
            self.assertTrue(all(call.args[0] == db._save_binarycheck_result_once
                                for call in db._retry_on_lock_error.call_args_list))

    def test_delegation_uses_one_process_and_removes_owned_pairs(self):
        from collections import deque
        worker, db, connection, cursor = self.worker()
        worker._priority_ids = {9}
        worker._priority_ipc = mock.Mock()
        rows = deque([(7, 8), (9, 10)])
        with mock.patch.object(worker, '_start_priority') as start:
            remaining = worker._delegate_priority(rows, 4, 1)
            self.assertEqual(list(remaining), [(7, 8)])
            self.assertFalse(worker._priority_idle)
            start.assert_called_once_with(4, 1)
            worker._delegate_priority(deque([(9, 11)]), 4, 1)
            start.assert_called_once()
        worker._priority_ipc.send.assert_called_once_with(('work', [(9, 10)]))

    def test_batch_triggers(self):
        worker, *_ = self.worker()
        with mock.patch.object(app.time, 'monotonic', return_value=599):
            self.assertFalse(worker._batch_due([1] * 19, 0))
            self.assertTrue(worker._batch_due([1] * 20, 0))
            self.assertTrue(worker._batch_due([1], 0, exhausted=True))
            self.assertFalse(worker._batch_due([], 0, exhausted=True))
        with mock.patch.object(app.time, 'monotonic', return_value=600):
            self.assertTrue(worker._batch_due([1], 0))

    def test_parent_acknowledges_waits_and_resets_in_order(self):
        worker, *_ = self.worker()
        ipc = worker._priority_ipc = mock.Mock()
        worker._priority_process = mock.Mock(exitcode=None)
        ipc.poll.side_effect = [True, False]
        ipc.recv.return_value = ('write_request', 3)
        events = []
        ipc.send.side_effect = lambda message: events.append(message)
        with mock.patch.object(app.time, 'monotonic', return_value=100), \
             mock.patch.object(app.time, 'sleep', side_effect=lambda seconds: events.append(('sleep', seconds))):
            worker._service_priority_ipc()
        self.assertEqual(events, [('waiting', 3, 110), ('sleep', 10), ('reset', 3)])

    def test_child_polls_every_five_seconds_and_writes_only_after_ack(self):
        worker, db, *_ = self.worker()
        ipc, lock = mock.Mock(), mock.MagicMock()
        ipc.poll.side_effect = [False, True]
        ipc.recv.return_value = ('waiting', 1, 110)
        with mock.patch.object(app.time, 'monotonic', return_value=100), \
             mock.patch.object(app.time, 'sleep') as sleep:
            worker._flush_priority_batch([(4, 1, 7, 8)], ipc, lock)
        self.assertEqual(sleep.call_args_list, [mock.call(5), mock.call(5)])
        self.assertEqual(ipc.send.call_args_list,
                         [mock.call(('write_request', 1)), mock.call(('committed', 1))])
        db._retry_on_lock_error.assert_called_once_with(worker._save_priority_batch_once, ((4, 1, 7, 8),))
        lock.__enter__.assert_called_once()

    def test_expired_ack_requests_a_new_window_without_writing(self):
        worker, db, *_ = self.worker()
        ipc, lock = mock.Mock(), mock.MagicMock()
        ipc.poll.return_value = True
        ipc.recv.side_effect = [('waiting', 1, 99), ('reset', 1), ('waiting', 2, 110)]
        with mock.patch.object(app.time, 'monotonic', return_value=100), \
             mock.patch.object(app.time, 'sleep'):
            worker._flush_priority_batch([(4,)], ipc, lock)
        self.assertEqual(ipc.send.call_args_list, [mock.call(('write_request', 1)),
                         mock.call(('write_request', 2)), mock.call(('committed', 2))])
        db._retry_on_lock_error.assert_called_once()

    def test_batch_is_one_transaction_and_rolls_back_on_failure(self):
        worker, db, connection, cursor = self.worker()
        batch = [(4, 1, 7, 8), (4, 1, 9, 10)]
        worker._save_priority_batch_once(batch)
        self.assertEqual(db._save_binarycheck_result_once.call_args_list,
                         [mock.call(*row, _connection=connection, _zero_content_ids=set()) for row in batch])
        connection.commit.assert_called_once()
        connection.commit.reset_mock()
        db._save_binarycheck_result_once.side_effect = [None, RuntimeError('failed')]
        with self.assertRaises(RuntimeError):
            worker._save_priority_batch_once(batch)
        connection.commit.assert_not_called()
        connection.rollback.assert_called_once()

    def test_shared_connection_does_not_commit_per_result(self):
        worker, db, connection, cursor = self.worker()
        actual = object.__new__(app.DataBaseConnection)
        actual._save_binarycheck_result_once(4, 1, 7, 8, 99, 'different',
                                           _connection=connection)
        connection.commit.assert_not_called()
        cursor.close.assert_called_once()

    def test_priority_comparisons_flush_at_twenty_and_at_end(self):
        worker, db, connection, cursor = self.worker()
        cursor.fetchone.return_value = (99, 1)
        cursor.fetchall.side_effect = [[
            (i, i + 1, '/a', '/b', 'x', 'y', 1, 0, 1, 0)
            for i in range(1, 44, 2)], []]
        selected = [(i, i + 1) for i in range(1, 44, 2)]
        with mock.patch.object(worker, '_flush_priority_batch') as flush:
            worker._run_job(4, 1, selected, mock.Mock(), mock.Mock())
        self.assertEqual([len(call.args[0]) for call in flush.call_args_list], [20, 2])
        db._retry_on_lock_error.assert_not_called()

    def test_cleanup_terminates_and_joins_child(self):
        worker, *_ = self.worker()
        process = worker._priority_process = mock.Mock()
        process.is_alive.side_effect = [True, False]
        ipc = worker._priority_ipc = mock.Mock()
        with mock.patch.object(worker, '_log_priority_lifecycle') as log:
            worker._cleanup_priority()
        log.assert_called_once_with('closed', exitcode=process.exitcode)
        process.terminate.assert_called_once()
        process.join.assert_called_once_with(timeout=5)
        process.close.assert_called_once()
        ipc.close.assert_called_once()

    def test_priority_failure_reaches_parent(self):
        worker, *_ = self.worker()
        ipc = worker._priority_ipc = mock.Mock()
        ipc.poll.return_value = True
        ipc.recv.return_value = ('error', 'commit failed')
        with self.assertRaisesRegex(RuntimeError, 'commit failed'):
            worker._service_priority_ipc()


class _SQLiteCursor:
    def __init__(self, owner):
        self.owner = owner
        self.cursor = owner.raw.cursor()
        self.columns = False

    def execute(self, sql, params=()):
        self.columns = sql.startswith('SHOW COLUMNS')
        if self.columns:
            return
        if sql.lstrip().startswith('UPDATE `tmp_doubles_letchecked`'):
            import os
            self.owner.raw.execute('INSERT INTO audit VALUES (?, ?, ?)',
                                   (os.getpid(), self.owner.txid, params[-2]))
        if sql.lstrip().startswith('UPDATE directories d') and 'd.hashed_at' in sql:
            jobid, first, second, hostid, dirid = params
            sql = ('UPDATE directories SET hashed_at=MAX(COALESCE(hashed_at, ?), ?) '
                   'WHERE hostid=? AND dirid=? AND scanned_at IS NOT NULL '
                   'AND EXISTS (SELECT 1 FROM jobs j WHERE j.jobid=? '
                   "AND j.status='active' AND j.timestart >= directories.scanned_at)")
            params = (first, second, hostid, dirid, jobid)
        self.cursor.execute(sql.replace('%s', '?'), params)

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return [('hostid',), ('dirid',)] if self.columns else self.cursor.fetchall()

    def close(self):
        self.cursor.close()


class _SQLiteConnection:
    def __init__(self, path):
        import sqlite3
        import uuid
        self.raw = sqlite3.connect(path, timeout=2)
        self.txid = uuid.uuid4().hex

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.raw.rollback()
        self.raw.close()

    def cursor(self):
        return _SQLiteCursor(self)

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.raw.rollback()


class _SpawnDatabase(app.DataBaseConnection):
    def __init__(self, path):
        self.path = path

    def connect(self):
        return _SQLiteConnection(self.path)

    def _acquire_runid(self, cursor):
        # SQLite adapter for exercising the production registration transaction.
        cursor.execute('SELECT COALESCE(MAX(runid), 0) + 1 FROM jobsrunid')
        return None, cursor.fetchone()[0]


class SpawnPriorityTests(unittest.TestCase):
    def test_real_spawn_ipc_batches_and_normal_worker(self):
        import multiprocessing
        import os
        from pathlib import Path
        import sqlite3
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            path = str(Path(folder) / 'priority.sqlite')
            with sqlite3.connect(path) as connection:
                connection.executescript('''
                    CREATE TABLE jobs (jobid INTEGER, hostid INTEGER, runid INTEGER, status TEXT, task TEXT, skipdirs TEXT, timestart TEXT DEFAULT '2026-10-29');
                    CREATE TABLE live_excluded_absdirs (hostid INTEGER, path TEXT);
                    INSERT INTO jobs (jobid,hostid,runid,status,task,skipdirs) VALUES (4, 1, 99, 'active', 'binarycheck', '');
                    CREATE TABLE jobsrunid (runid INTEGER PRIMARY KEY, jobid INTEGER,
                        task TEXT, status TEXT, stage TEXT, timecreated TEXT,
                        pid INTEGER, process_host TEXT, process_start TEXT, remotehost INTEGER);
                    INSERT INTO jobsrunid VALUES (99, 4, 'binarycheck', 'active', 'coarse', '2026-10-29', NULL, NULL, NULL, NULL);
                    CREATE TABLE directories (dirid INTEGER, hostid INTEGER, path TEXT,
                        subtree_structure TEXT, localfilecount INTEGER, subdircount INTEGER,
                        parentdir INTEGER DEFAULT 0, nopermdirs INTEGER DEFAULT 0,
                        emptyfilecount INTEGER DEFAULT 0, otherentrycount INTEGER DEFAULT 0,
                        localfilesize INTEGER DEFAULT 0, zero_content_only INTEGER DEFAULT 0,
                        scanned_at TEXT DEFAULT '2026-10-28', hashed_at TEXT);
                    CREATE TABLE tmp_doubles_letchecked (jobid INTEGER, hostid INTEGER,
                        left_dirid INTEGER, right_dirid INTEGER, is_requested INTEGER, status TEXT,
                        verification_method TEXT, source_left_dirid INTEGER, source_right_dirid INTEGER,
                        finished_at TEXT, error_message TEXT, runid INTEGER, hostidB INTEGER DEFAULT 1);
                    CREATE TABLE tmp_highpriority (hostid INTEGER, dirid INTEGER);
                    CREATE TABLE audit (pid INTEGER, txid TEXT, leftid INTEGER);
                ''')
                for i in range(1, 46, 2):
                    connection.executemany('INSERT INTO directories (dirid,hostid,path,subtree_structure,localfilecount,subdircount) VALUES (?, 1, ?, ?, 1, 0)',
                                           [(i, '/a', 'x'), (i + 1, '/b', 'y')])
                    connection.execute('INSERT INTO tmp_doubles_letchecked VALUES (4, 1, ?, ?, 1, ?, NULL, NULL, NULL, NULL, NULL, 99, 1)',
                                       (i, i + 1, 'pending'))
                    if i < 45:
                        connection.execute('INSERT INTO tmp_highpriority VALUES (1, ?)', (i,))
            worker = app.Binarycheck(_SpawnDatabase(path))
            worker.WRITE_WINDOW = 0.1
            context = multiprocessing.get_context('spawn')
            starts = []
            def start(jobid, hostid):
                if worker._priority_started:
                    return
                parent_ipc, child_ipc = context.Pipe(duplex=True)
                worker._write_lock = context.Lock()
                child = app.Binarycheck(worker.database)
                child.ACK_POLL_INTERVAL = 0.01
                process = context.Process(target=child.highpriorityprovess,
                                          args=(jobid, hostid, child_ipc, worker._write_lock))
                process.start()
                child_ipc.close()
                worker._priority_runid = worker.database.register_binarycheck_priority_run(jobid, hostid, process.pid)
                parent_ipc.send(('runid', worker._priority_runid))
                with self.assertRaisesRegex(RuntimeError, 'läuft bereits'):
                    worker.database.ensure_no_priority_worker(hostid)
                starts.append(process.pid)
                worker._priority_started = True
                worker._priority_process = process
                worker._priority_ipc = parent_ipc
            with mock.patch.object(worker, '_start_priority', side_effect=start), \
                 mock.patch.object(worker, '_log_priority_lifecycle'):
                worker.loop(4, 1)
            self.assertEqual(len(starts), 1)
            self.assertIsNone(worker._priority_process)
            with sqlite3.connect(path) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM tmp_doubles_letchecked WHERE status='different'").fetchone()[0], 23)
                child_batches = connection.execute('SELECT COUNT(*) FROM audit WHERE pid = ? GROUP BY txid ORDER BY COUNT(*)', (starts[0],)).fetchall()
                self.assertEqual(child_batches, [(2,), (20,)])
                self.assertEqual(connection.execute('SELECT COUNT(*) FROM audit WHERE pid = ?', (os.getpid(),)).fetchone()[0], 1)
                self.assertEqual(connection.execute('SELECT COUNT(*) FROM tmp_highpriority').fetchone()[0], 22)
                self.assertEqual(connection.execute('SELECT runid FROM jobs WHERE jobid=4').fetchone(), (99,))
                self.assertEqual(connection.execute('SELECT runid, stage FROM jobsrunid ORDER BY runid').fetchall(),
                                 [(99, 'coarse'), (100, 'highpriority')])
                self.assertEqual(connection.execute('SELECT DISTINCT runid FROM tmp_doubles_letchecked WHERE left_dirid < 45').fetchall(), [(100,)])
                self.assertEqual(connection.execute('SELECT runid FROM tmp_doubles_letchecked WHERE left_dirid=45').fetchone(), (99,))
                metadata = connection.execute('SELECT pid, process_host, process_start FROM jobsrunid WHERE runid=100').fetchone()
                self.assertEqual(metadata[0], starts[0])
                self.assertEqual(metadata[1], app.socket.gethostname())
                self.assertTrue(metadata[2])

    def test_production_launcher_starts_only_once(self):
        worker = app.Binarycheck(mock.Mock())
        context = mock.Mock()
        parent_ipc, child_ipc = mock.Mock(), mock.Mock()
        context.Pipe.return_value = (parent_ipc, child_ipc)
        with mock.patch.object(app.multiprocessing, 'get_context', return_value=context), \
             mock.patch.object(app.signal, 'signal', return_value=app.signal.SIG_DFL), \
             mock.patch.object(worker, '_log_priority_lifecycle') as log:
            worker._start_priority(4, 1)
            worker._start_priority(4, 1)
        worker.database.register_binarycheck_priority_run.assert_called_once_with(4, 1, context.Process.return_value.pid)
        log.assert_called_once_with('spawned')
        context.Process.assert_called_once()
        context.Process.return_value.start.assert_called_once()
        self.assertEqual(context.Process.call_args.kwargs['target'].__name__, 'highpriorityprovess')
        child_ipc.close.assert_called_once()


class PriorityRunidAndLoggingTests(unittest.TestCase):
    def database(self):
        db = object.__new__(app.DataBaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        return db, connection, connection.cursor.return_value

    def test_registration_allocates_history_entry_and_releases_global_lock(self):
        db, connection, cursor = self.database()
        cursor.fetchone.side_effect = [('binarycheck',), (1,)]
        with mock.patch.object(db, 'connect', return_value=connection), \
             mock.patch.object(db, '_acquire_runid', return_value=('allocator', 100)) as allocate:
            self.assertEqual(db.register_binarycheck_priority_run(4, 1, os.getpid()), 100)
        allocate.assert_called_once_with(cursor)
        insert = next(call for call in cursor.execute.call_args_list if call.args[0].startswith('INSERT'))
        self.assertIn('jobsrunid', insert.args[0])
        self.assertEqual(insert.args[1][:5], (100, 4, 'binarycheck', 'active', 'highpriority'))
        self.assertFalse(any(call.args[0].startswith('UPDATE') for call in cursor.execute.call_args_list))
        self.assertEqual(cursor.execute.call_args.args, ('SELECT RELEASE_LOCK(%s)', ('allocator',)))
        connection.commit.assert_called_once()
        cursor.close.assert_called_once()

    def test_wrong_host_or_inactive_job_cannot_register(self):
        db, connection, cursor = self.database()
        cursor.fetchone.side_effect = [None, (1,)]
        with mock.patch.object(db, 'connect', return_value=connection), \
             mock.patch.object(db, '_acquire_runid', return_value=('allocator', 100)), \
             self.assertRaises(RuntimeError):
            db.register_binarycheck_priority_run(4, 2, os.getpid())
        connection.commit.assert_not_called()
        connection.rollback.assert_called_once()
        self.assertFalse(any(call.args[0].startswith('INSERT') for call in cursor.execute.call_args_list))
        self.assertEqual(cursor.execute.call_args.args, ('SELECT RELEASE_LOCK(%s)', ('allocator',)))

    def test_lifecycle_events_reach_console_and_syslog(self):
        import contextlib
        import io
        import logging
        worker = app.Binarycheck(mock.Mock())
        worker._priority_jobid, worker._priority_hostid, worker._priority_runid = 4, 1, 100
        worker._priority_process = mock.Mock(pid=12345)
        for event, exitcode in (('spawned', None), ('closed', 0), ('closed', -15)):
            output = io.StringIO()
            messages = []
            with mock.patch.object(app, 'SysLogHandler') as factory, contextlib.redirect_stdout(output):
                handler = factory.return_value
                handler.level = logging.INFO
                handler.handle.side_effect = lambda record: messages.append(record.getMessage())
                worker._log_priority_lifecycle(event, exitcode)
            self.assertEqual(messages, [output.getvalue().strip()])
            for text in (event, 'jobid=4', 'hostid=1', 'runid=100', 'pid=12345'):
                self.assertIn(text, messages[0])
            if exitcode is not None:
                self.assertIn(f'exitcode={exitcode}', messages[0])
            self.assertEqual(factory.call_args.kwargs['address'], ('localhost', 5101))
            handler.close.assert_called_once()

    def test_sigusr1_enables_each_finished_directory_and_stays_enabled(self):
        import contextlib
        import io
        worker = app.Binarycheck(mock.Mock())
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            worker._report_priority_finished(4, 1, 100, 7, 8, 'equal')
            self.assertEqual(output.getvalue(), '')
            worker._enable_priority_verbose(app.signal.SIGUSR1, None)
            worker._report_priority_finished(4, 1, 100, 7, 8, 'equal')
            worker._enable_priority_verbose(app.signal.SIGUSR1, None)
            worker._report_priority_finished(4, 1, 100, 9, 10, 'different')
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 4)
        for line, dirid in zip(lines, (7, 8, 9, 10)):
            self.assertIn(f'dirid={dirid},', line)
            self.assertIn('runid=100', line)

    def test_subprocess_installs_and_restores_sigusr1_handler(self):
        worker = app.Binarycheck(mock.Mock())
        ipc = mock.Mock()
        ipc.recv.return_value = ('finish',)
        with mock.patch.object(app.signal, 'signal', return_value=app.signal.SIG_DFL) as install:
            worker.highpriorityprovess(4, 1, ipc, mock.Mock())
        self.assertEqual(install.call_args_list,
                         [mock.call(app.signal.SIGUSR1, worker._enable_priority_verbose),
                          mock.call(app.signal.SIGUSR1, app.signal.SIG_DFL)])
        ipc.close.assert_called_once()

    def test_priority_runid_is_reused_for_further_assignments(self):
        worker = app.Binarycheck(mock.Mock())
        worker._priority_runid = 100
        ipc = mock.Mock()
        ipc.recv.side_effect = [('work', [(7, 8)]), ('work', [(9, 10)]), ('finish',)]
        with mock.patch.object(app.signal, 'signal'), mock.patch.object(worker, '_run_job') as run:
            worker.highpriorityprovess(4, 1, ipc, mock.Mock())
        self.assertEqual(run.call_count, 2)
        self.assertEqual(worker._priority_runid, 100)
        worker.database.register_binarycheck_priority_run.assert_not_called()


class _SignalPriorityWorker(app.Binarycheck):
    def _enable_priority_verbose(self, signum, frame):
        super()._enable_priority_verbose(signum, frame)
        self.signal_ipc.send(('verbose_enabled',))


def _signal_priority_entry(database, ipc, lock, output):
    import contextlib
    worker = _SignalPriorityWorker(database)
    worker._priority_runid = 100
    worker.signal_ipc = ipc
    with open(output, 'w') as stream, contextlib.redirect_stdout(stream):
        previous = app.signal.signal(app.signal.SIGUSR1, worker._enable_priority_verbose)
        try:
            ipc.send(('ready',))
            worker.highpriorityprovess(4, 1, ipc, lock)
        finally:
            app.signal.signal(app.signal.SIGUSR1, previous)


class RealPrioritySignalTests(unittest.TestCase):
    def test_real_sigusr1_enables_verbose_output_in_spawned_process(self):
        import multiprocessing
        import os
        from pathlib import Path
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            output = str(Path(folder) / 'verbose.txt')
            context = multiprocessing.get_context('spawn')
            parent, child = context.Pipe()
            lock = context.Lock()
            process = context.Process(target=_signal_priority_entry,
                                      args=(_SpawnDatabase('unused'), child, lock, output))
            process.start()
            child.close()
            try:
                self.assertTrue(parent.poll(5), 'subprocess did not initialize')
                self.assertEqual(parent.recv(), ('ready',))
                os.kill(process.pid, app.signal.SIGUSR1)
                self.assertTrue(parent.poll(5), 'SIGUSR1 was not handled')
                self.assertEqual(parent.recv(), ('verbose_enabled',))
                parent.send(('finish',))
                process.join(timeout=5)
                self.assertEqual(process.exitcode, 0)
            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                process.close()
                parent.close()


class TestPriorityProcessIdentity(unittest.TestCase):
    def database(self, rows):
        db = object.__new__(app.DataBaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchall.return_value = rows
        return db, connection, connection.cursor.return_value

    def test_live_same_process_prevents_startup(self):
        db, connection, cursor = self.database([(100, 4, 1234, 'birth')])
        with mock.patch.object(db, 'connect', return_value=connection), \
             mock.patch.object(app, '_process_start_token', return_value='birth'), \
             self.assertRaisesRegex(RuntimeError, 'PID 1234, Runid 100'):
            db.ensure_no_priority_worker(7)
        self.assertEqual(cursor.execute.call_args.args[1], (app.socket.gethostname(), 7))
        self.assertIn('j.jobid IS NULL', cursor.execute.call_args.args[0])
        cursor.close.assert_called_once()

    def test_dead_or_reused_pids_do_not_prevent_startup(self):
        for current in (None, 'different birth'):
            db, connection, cursor = self.database([(100, 4, 1234, 'birth')])
            with mock.patch.object(db, 'connect', return_value=connection), \
                 mock.patch.object(app, '_process_start_token', return_value=current):
                db.ensure_no_priority_worker(0)

    def test_pid_with_missing_start_metadata_is_not_silently_ignored(self):
        db, connection, cursor = self.database([(100, 4, 1234, None)])
        with mock.patch.object(db, 'connect', return_value=connection), \
             mock.patch.object(app, '_process_start_token', return_value='birth'), self.assertRaises(RuntimeError):
            db.ensure_no_priority_worker(0)

    def test_simultaneous_registration_is_rechecked_under_allocator_lock(self):
        db, connection, cursor = self.database([(100, 4, 1234, 'birth')])
        with mock.patch.object(db, 'connect', return_value=connection), \
             mock.patch.object(app, '_process_start_token', return_value='birth'), \
             mock.patch.object(db, '_acquire_runid', return_value=('allocator', 101)), \
             self.assertRaisesRegex(RuntimeError, 'läuft bereits'):
            db.register_binarycheck_priority_run(4, 0, 5678)
        self.assertFalse(any(call.args[0].startswith('INSERT') for call in cursor.execute.call_args_list))
        connection.rollback.assert_called_once()
        self.assertEqual(cursor.execute.call_args.args, ('SELECT RELEASE_LOCK(%s)', ('allocator',)))

    def test_occupied_preflight_does_not_spawn_or_allocate(self):
        db = mock.Mock()
        db.ensure_no_priority_worker.side_effect = RuntimeError('occupied')
        worker = app.Binarycheck(db)
        with mock.patch.object(app.multiprocessing, 'get_context') as context, self.assertRaises(RuntimeError):
            worker._start_priority(4, 0)
        context.assert_not_called()
        db.register_binarycheck_priority_run.assert_not_called()

    def test_registration_failure_reaps_the_unassigned_child(self):
        db = mock.Mock()
        db.register_binarycheck_priority_run.side_effect = RuntimeError('occupied')
        worker = app.Binarycheck(db)
        context = mock.Mock()
        parent, child = mock.Mock(), mock.Mock()
        context.Pipe.return_value = (parent, child)
        process = context.Process.return_value
        process.is_alive.side_effect = [True, False]
        with mock.patch.object(app.multiprocessing, 'get_context', return_value=context), \
             mock.patch.object(app.signal, 'signal', return_value=app.signal.SIG_DFL), \
             mock.patch.object(worker, '_log_priority_lifecycle'), self.assertRaises(RuntimeError):
            worker._start_priority(4, 0)
        process.terminate.assert_called_once()
        process.join.assert_called_once_with(timeout=5)
        parent.close.assert_called_once()
        parent.send.assert_not_called()


class VanishedCoarseDirectoryTests(unittest.TestCase):
    def setUp(self):
        # These tests exercise filesystem churn; hash readiness has separate coverage.
        patch = mock.patch.object(app.database_connection, 'ensure_combined_hash_readiness')
        patch.start()
        self.addCleanup(patch.stop)

    def test_vanished_child_is_removed_and_next_directory_is_scanned(self):
        import tempfile
        from pathlib import Path
        for use_glob in (False, True):
            with self.subTest(glob=use_glob), tempfile.TemporaryDirectory() as root:
                root = os.path.realpath(root)
                missing, present = os.path.join(root, 'gone-cache'), os.path.join(root, 'present')
                Path(present).mkdir()
                works = [dict(path=path, parent_path=root, path_hash=app._path_hash(path))
                         for path in (missing, present)]
                with mock.patch.object(app.database_connection, 'prepare_stage_work'), \
                     mock.patch.object(app.database_connection, 'claim_work_batch', side_effect=[works, []]), \
                     mock.patch.object(app.database_connection, 'complete_coarse_batch') as complete:
                    app.coarse_scan(root, 16, hostid=1, coarse_glob=use_glob)
                results = complete.call_args.args[2]
                self.assertEqual(len(results), 2)
                self.assertFalse(results[0]['store'])
                self.assertTrue(results[0]['remove_subtree'])
                self.assertIsNone(results[0]['permission_parent'])
                self.assertTrue(results[1]['store'])

    def test_root_disappearing_after_preparation_is_still_an_error(self):
        import tempfile
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, 'root')
            os.mkdir(root)
            work = dict(path=root, parent_path=None, path_hash=app._path_hash(root))
            with mock.patch.object(app.database_connection, 'prepare_stage_work', side_effect=lambda *a, **kw: os.rmdir(root)), \
                 mock.patch.object(app.database_connection, 'claim_work_batch', return_value=[work]), \
                 mock.patch.object(app.database_connection, 'complete_coarse_batch') as complete, \
                 self.assertRaises(FileNotFoundError):
                app.coarse_scan(root, 16, hostid=1)
            complete.assert_not_called()

    def test_vanished_directory_entry_does_not_abort_iteration(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            root = os.path.realpath(root)
            vanished, present = mock.Mock(), mock.Mock()
            vanished.path, present.path = os.path.join(root, 'gone'), os.path.join(root, 'child')
            vanished.is_dir.side_effect = FileNotFoundError('gone')
            present.is_dir.return_value = True
            scan = mock.MagicMock()
            scan.__enter__.return_value = iter([vanished, present])
            work = dict(path=root, parent_path=None, path_hash=app._path_hash(root))
            with mock.patch.object(app.database_connection, 'prepare_stage_work'), \
                 mock.patch.object(app.database_connection, 'claim_work_batch', side_effect=[[work], []]), \
                 mock.patch.object(app.database_connection, 'complete_coarse_batch') as complete, \
                 mock.patch.object(app.os, 'scandir', return_value=scan):
                app.coarse_scan(root, 16, hostid=1)
            self.assertEqual(complete.call_args.args[2][0]['children'], [present.path])

    def test_child_replaced_by_file_is_not_stored_as_directory(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as root:
            root = os.path.realpath(root)
            path = os.path.join(root, 'was-directory')
            Path(path).write_text('now a file')
            work = dict(path=path, parent_path=root, path_hash=app._path_hash(path))
            with mock.patch.object(app.database_connection, 'prepare_stage_work'), \
                 mock.patch.object(app.database_connection, 'claim_work_batch', side_effect=[[work], []]), \
                 mock.patch.object(app.database_connection, 'complete_coarse_batch') as complete:
                app.coarse_scan(root, 16, hostid=1)
            result = complete.call_args.args[2][0]
            self.assertFalse(result['store'])
            self.assertTrue(result['remove_subtree'])


class ZeroContentBinarycheckTests(unittest.TestCase):
    worker = PriorityTests.worker

    def test_existing_queued_fileless_tree_is_skipped_without_filesystem_comparison(self):
        worker, db, connection, cursor = self.worker()
        db.zero_content_directory_ids.return_value = {(1,7), (1,8)}
        cursor.fetchone.return_value = (99,1)
        cursor.fetchall.side_effect = [[(7,8,'/a','/b','nested','nested',0,2,0,2)], []]
        with mock.patch.object(worker, '_refresh_priority'), \
             mock.patch.object(app, '_directory_contents_equal') as compare:
            worker.loop(4, hostid=1)
        compare.assert_not_called()
        self.assertEqual(db._retry_on_lock_error.call_args.args[6], 'skipped_empty')

    def test_empty_discovered_subtree_pairs_are_not_inserted_as_equal_results(self):
        database = object.__new__(app.DataBaseConnection)
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value
        cursor.fetchall.return_value = [(1,'/a/empty'),(2,'/b/empty'),
                                       (3,'/a/file'),(4,'/b/file')]
        database._save_binarycheck_result_once(4,1,7,8,99,'equal','direct',None,None,
            (('/a/empty','/b/empty'),('/a/file','/b/file')),
            _connection=connection, _zero_content_ids={(1,1),(1,2)})
        inserted = cursor.executemany.call_args.args[1]
        self.assertEqual([(row[3],row[4]) for row in inserted], [(3,4)])
        self.assertTrue(all(row[1] == row[2] == 1 for row in inserted))


class ZeroFileReadabilityTests(unittest.TestCase):
    def test_real_zero_file_is_readable_but_nonzero_and_symlink_are_not_zero_proof(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as root:
            empty, full, link = (Path(root) / name for name in ('empty','full','link'))
            empty.touch()
            full.write_bytes(b'x')
            link.symlink_to(empty)
            self.assertTrue(app._zero_file_is_readable(empty))
            self.assertFalse(app._zero_file_is_readable(full))
            self.assertFalse(app._zero_file_is_readable(link))
            with mock.patch.object(app.os, 'open', side_effect=PermissionError('denied')):
                self.assertFalse(app._zero_file_is_readable(empty))

    def test_incomplete_detailed_scan_does_not_store_proof_of_zero_files(self):
        import tempfile
        from pathlib import Path
        for denied in (False, True):
            with tempfile.TemporaryDirectory() as root:
                Path(root, 'zero').touch()
                work = dict(path=root, parent_path=None, path_hash=app._path_hash(root))
                with mock.patch.object(app.database_connection, 'prepare_stage_work', return_value=True), \
                     mock.patch.object(app.database_connection, 'claim_work_batch', side_effect=[[work], []]), \
                     mock.patch.object(app.database_connection, 'complete_detailed_batch') as complete, \
                     mock.patch.object(app.database_connection, 'rebuild_subtree_statistics'), \
                     mock.patch.object(app, '_zero_file_is_readable', return_value=not denied):
                    app.detailed_scan(root, 16, hostid=1)
                result = complete.call_args.args[2][0]
                self.assertEqual(result['stats']['emptyfilecount'], None if denied else 1)


class ScanConnectionRecoveryTests(unittest.TestCase):
    def test_batch_reopens_transaction_after_disconnect_and_cleanup_failure(self):
        import dirwalker
        for module in (app, dirwalker):
            for stage in ('coarse', 'detailed'):
                with self.subTest(module=module.__name__, stage=stage):
                    db = object.__new__(module.DataBaseConnection)
                    connections = [mock.MagicMock(), mock.MagicMock()]
                    for connection in connections:
                        connection.__enter__.return_value = connection
                        connection.cursor.return_value.fetchone.return_value = (1, 1)
                    lost = module.Error('connection reset')
                    lost.errno = 2055
                    connections[0].rollback.side_effect = RuntimeError('broken pipe')
                    connections[0].cursor.return_value.close.side_effect = RuntimeError('cursor lost')
                    with mock.patch.object(db, 'connect', side_effect=connections), \
                            mock.patch.object(db, '_compact_done_subtrees', side_effect=[lost, None]), \
                            mock.patch.object(module.time, 'sleep') as sleep:
                        getattr(db, 'complete_' + stage + '_batch')(18, '/root', [], hostid=1)
                    connections[0].commit.assert_not_called()
                    connections[1].commit.assert_called_once()
                    sleep.assert_called_once_with(30)

    def test_lost_commit_response_is_not_replayed(self):
        for stage in ('coarse', 'detailed'):
            with self.subTest(stage=stage):
                db = object.__new__(app.DataBaseConnection)
                connection = mock.MagicMock()
                connection.__enter__.return_value = connection
                connection.cursor.return_value.fetchone.return_value = (1, 1)
                lost = app.Error('commit response lost')
                lost.errno = 2013
                connection.commit.side_effect = lost
                with mock.patch.object(db, 'connect', return_value=connection) as connect, \
                        mock.patch.object(db, '_compact_done_subtrees'), \
                        mock.patch.object(app.time, 'sleep') as sleep:
                    with self.assertRaises(app.Error) as raised:
                        getattr(db, 'complete_' + stage + '_batch')(18, '/root', [], hostid=1)
                self.assertIs(raised.exception, lost)
                connect.assert_called_once()
                sleep.assert_not_called()


class VanishedDetailedDirectoryTests(unittest.TestCase):
    def test_missing_or_replaced_child_is_incomplete_and_scan_continues(self):
        import tempfile
        from pathlib import Path
        import dirwalker
        for module in (app, dirwalker):
            for replaced in (False, True):
                with self.subTest(module=module.__name__, replaced=replaced), tempfile.TemporaryDirectory() as root:
                    root = os.path.realpath(root)
                    gone, present = Path(root, 'gone'), Path(root, 'present')
                    if replaced:
                        gone.touch()
                    present.mkdir()
                    Path(present, 'file').write_bytes(b'abc')
                    works = [dict(path=str(path), parent_path=root,
                                  path_hash=module._path_hash(str(path)))
                             for path in (gone, present)]
                    with mock.patch.object(module.database_connection, 'prepare_stage_work', return_value=True), \
                            mock.patch.object(module.database_connection, 'claim_work_batch', side_effect=[works, []]), \
                            mock.patch.object(module.database_connection, 'complete_detailed_batch') as complete, \
                            mock.patch.object(module.database_connection, 'rebuild_subtree_statistics') as rebuild:
                        module.detailed_scan(root, 16, hostid=1)
                    results = complete.call_args.args[2]
                    self.assertEqual(len(results), 2)
                    self.assertIsNone(results[0]['stats']['emptyfilecount'])
                    self.assertIsNone(results[0]['permission_parent'])
                    self.assertEqual(results[1]['stats']['localfilecount'], 1)
                    self.assertEqual(results[1]['stats']['localfilesize'], 3)
                    rebuild.assert_called_once_with(root, hostid=1)

    def test_missing_root_remains_a_job_error(self):
        import tempfile
        with tempfile.TemporaryDirectory() as parent:
            root = os.path.join(parent, 'missing')
            work = dict(path=root, parent_path=None, path_hash=app._path_hash(root))
            with mock.patch.object(app.database_connection, 'prepare_stage_work', return_value=True), \
                    mock.patch.object(app.database_connection, 'claim_work_batch', return_value=[work]), \
                    mock.patch.object(app.database_connection, 'complete_detailed_batch') as complete, \
                    self.assertRaises(FileNotFoundError):
                app.detailed_scan(root, 16, hostid=1)
            complete.assert_not_called()


class VanishedDirectoryThresholdTests(unittest.TestCase):
    def test_threshold_counts_across_batches_and_stops_at_twenty_four(self):
        works = [dict(path='/root/missing' + str(i), parent_path='/root', path_hash=str(i))
                 for i in range(25)]
        with mock.patch.object(app.database_connection, 'prepare_stage_work', return_value=True), \
                mock.patch.object(app.database_connection, 'claim_work_batch', side_effect=[works[:7], works[7:]]), \
                mock.patch.object(app.database_connection, 'complete_detailed_batch') as complete, \
                mock.patch.object(app.database_connection, 'rebuild_subtree_statistics') as rebuild, \
                mock.patch.object(app.os, 'scandir', side_effect=FileNotFoundError) as scan:
            self.assertEqual(app.detailed_scan('/root', 16, hostid=1), 24)
        self.assertEqual(scan.call_count, 24)
        complete.assert_called_once()
        rebuild.assert_not_called()

    def test_reschedule_clears_only_job_host_checkpoints_and_enables_scanall(self):
        db = object.__new__(app.DataBaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.fetchone.return_value = (16,)
        with mock.patch.object(db, 'connect', return_value=connection):
            self.assertTrue(db.reschedule_full_coarse(16, 1))
        calls = connection.cursor.return_value.execute.call_args_list
        self.assertIn('FOR UPDATE', calls[0].args[0])
        self.assertIn('DELETE FROM tmp1_job_work', calls[1].args[0])
        self.assertEqual(calls[1].args[1], (16, 1))
        self.assertIn("stage = 'coarse'", calls[2].args[0])
        self.assertIn('scanall = 1', calls[2].args[0])
        connection.commit.assert_called_once()

    def test_parent_reschedules_instead_of_closing_job(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            server = mock.Mock()
            server.run_spawn.return_value = 'rescan_coarse'
            with mock.patch.object(app.database_connection, 'reschedule_full_coarse', return_value=True) as reschedule, \
                    mock.patch.object(app, 'set_job_status') as status:
                app.process_job(dict(jobid=16, hostid=1, startdirname=root), 'detailed', server)
            reschedule.assert_called_once_with(16, 1)
            status.assert_not_called()
            server.log_coarse_rescheduled.assert_called_once_with(16, 1, 24)


class RuntimeNetworkRecoveryTests(unittest.TestCase):
    def database(self):
        db = object.__new__(app.DataBaseConnection)
        db.host, db.port, db.database = 'test', 3306, 'test'
        db.user, db.password = 'test', 'test'
        return db

    def test_parent_network_outcome_does_not_close_or_fail_job(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            operation = mock.Mock()
            operation.run_spawn.return_value = 'network_retry'
            with mock.patch.object(app, 'set_job_status') as status:
                app.process_job(dict(jobid=16, hostid=1, startdirname=root), 'detailed', operation)
            status.assert_not_called()
            operation.recover_network_job.assert_called_once_with(16, 1)

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


class RuntimePerHostJobTests(unittest.TestCase):
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

    def test_same_host_work_still_runs_after_remote_phase(self):
        db = object.__new__(app.DataBaseConnection)
        with mock.patch.object(db, 'run_remote_binarycheck', return_value=True) as remote, \
                mock.patch.object(app, 'Binarycheck') as normal:
            db.run_binarycheck_job(7, hostid=1)
        remote.assert_called_once_with(7, hostid=1)
        normal.return_value.loop.assert_called_once_with(7, hostid=1)


class ScanReportingTests(unittest.TestCase):
    def test_coarse_reports_committed_inserts_and_rate(self):
        database = mock.Mock()
        with tempfile.TemporaryDirectory() as root:
            database.claim_work_batch.side_effect = [[dict(path=root, parent_path=None)], []]
            database.complete_coarse_batch.return_value = 1
            with mock.patch.object(app, 'database_connection', database), \
                    mock.patch.object(app.time, 'monotonic', side_effect=[100, 102]), \
                    contextlib.redirect_stdout(io.StringIO()) as output:
                app.coarse_scan(root, 7)
        self.assertIn('1 Verzeichnisse eingefügt', output.getvalue())
        self.assertIn('2000.00 s pro 1000', output.getvalue())

    def test_unfilled_count_scopes_host_and_root_and_ignores_optional_nulls(self):
        database = object.__new__(app.DataBaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        cursor.fetchone.return_value = (42,)
        with mock.patch.object(database, 'connect', return_value=connection):
            self.assertEqual(database.count_unfilled_detailed_directories(7, '/scan'), 42)
        sql, parameters = cursor.execute.call_args.args
        self.assertEqual(parameters, (7, '/scan'))
        self.assertIn('hostid = %s AND root_path = %s', sql)
        for field in ('localfilesize', 'localfilecount', 'subdircount', 'otherentrycount'):
            self.assertIn(field + ' IS NULL', sql)
        self.assertNotIn('emptyfilecount', sql)
        cursor.close.assert_called_once()

    def test_missing_directory_counter_resets_at_each_two_hour_boundary(self):
        database = mock.Mock()
        works = [dict(path='/root/missing' + str(i), parent_path='/root') for i in range(5)]
        database.claim_work_batch.side_effect = [works, []]
        # Reset at exactly 2h, then again at 4h; no real waiting.
        with mock.patch.object(app, 'database_connection', database), \
                mock.patch.object(app.os, 'scandir', side_effect=FileNotFoundError), \
                mock.patch.object(app.time, 'monotonic', side_effect=[0, 1, 7199, 7200, 7201, 14400]), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            app.detailed_scan('/root', 7)
        lines = [line for line in output.getvalue().splitlines() if 'verschwundene Verzeichnisse' in line]
        self.assertEqual([line.rsplit(' ', 1)[1] for line in lines],
                         ['1/24', '2/24', '1/24', '2/24', '1/24'])
        database.rebuild_subtree_statistics.assert_called_once()

    def test_empty_subtree_result_cannot_succeed(self):
        database = object.__new__(app.DataBaseConnection)
        reader, writer = mock.MagicMock(), mock.MagicMock()
        reader.cursor.return_value.fetchone.return_value = (1,)
        reader.cursor.return_value.fetchmany.return_value = []
        with mock.patch.object(database, 'connect', side_effect=[reader, writer]), \
                mock.patch.object(database, 'refresh_zero_content_flags') as refresh:
            with self.assertRaisesRegex(RuntimeError, 'nicht verarbeitet'):
                database.rebuild_subtree_statistics('/root', hostid=1)
        writer.rollback.assert_called_once()
        refresh.assert_not_called()

    def test_recursive_rebuild_uses_root_path_and_host_in_correct_parameter_positions(self):
        import sqlite3
        import hashlib
        from types import SimpleNamespace
        sql = sqlite3.connect(':memory:')
        self.addCleanup(sql.close)
        sql.execute('CREATE TABLE directories (dirid INTEGER, hostid INTEGER, root_path TEXT, '
                    'path_hash TEXT, parentdir INTEGER, name TEXT, localfilesize INTEGER, files_sha256 TEXT, subdircount INTEGER)')
        root = '/Users/alex/Desktop/_TEST/25HS'
        empty = hashlib.sha256().hexdigest()
        sql.executemany('INSERT INTO directories VALUES (?,?,?,?,?,?,?,?,?)', [
            (1, 1, root, app._path_hash(root), 0, '25HS', 2, empty, 1),
            (2, 1, root, app._path_hash(root + '/child'), 1, 'child', 3, empty, 0),
            (3, 0, root, 'other-host', 1, 'foreign-child', 1000, empty, 0),
        ])
        cursor = sql.cursor()
        def execute(statement, params):
            cursor.execute(statement.replace('%s', '?').replace('BINARY d.name', 'd.name COLLATE BINARY'), params)
        read_cursor = SimpleNamespace(execute=execute, fetchone=cursor.fetchone,
                                      fetchmany=cursor.fetchmany, close=cursor.close)
        reader = SimpleNamespace(cursor=lambda **kwargs: read_cursor, close=lambda: None)
        writer = mock.MagicMock()
        database = object.__new__(app.DataBaseConnection)
        with mock.patch.object(database, 'connect', side_effect=[reader, writer]), \
                mock.patch.object(database, 'refresh_zero_content_flags') as refresh:
            database.rebuild_subtree_statistics(root, hostid=1)
        updates = writer.cursor.return_value.execute.call_args_list
        self.assertEqual([call.args[1][-1] for call in updates], [2, 1])
        root_values = updates[-1].args[1]
        self.assertEqual(root_values[5], 5)
        self.assertEqual(root_values[6], '1-0')
        self.assertIsNotNone(root_values[3])
        refresh.assert_called_once_with(hostid=1)


class RuntimeConsistentExclusionTests(unittest.TestCase):
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

    def test_detailed_counts_and_metadata_hash_ignore_exclusions_and_mtime(self):
        with tempfile.TemporaryDirectory() as folder:
            left, right = self.trees(folder)
            results = []
            for hostid, root in enumerate((left, right)):
                db = mock.Mock()
                db.claim_work_batch.side_effect = [[dict(path=str(root), parent_path=None)], []]
                with mock.patch.object(app, 'database_connection', db):
                    app.detailed_scan(str(root), 7, hostid=hostid, skipdirs='.DS_store,@eadir')
                stats = db.complete_detailed_batch.call_args.args[2][0]['stats']
                self.assertEqual(stats['localfilecount'], 1)
                self.assertEqual(stats['subdircount'], 1)
                self.assertEqual(stats['localfilesize'], len(b'same real contents'))
                expected = app.datetime.fromtimestamp(
                    (root / 'data.zip').stat().st_mtime, app.timezone.utc).replace(tzinfo=None)
                self.assertEqual(stats.pop('files_lastwrite'), expected)
                inode = app.datetime.fromtimestamp(
                    root.stat().st_mtime, app.timezone.utc).replace(tzinfo=None)
                self.assertEqual(stats.pop('time_inode_changed'), inode)
                results.append(stats)
            self.assertEqual(results[0], results[1])

    def test_lastwrite_is_persisted_and_cleared_when_absent(self):
        for module in (app, __import__('dirwalker')):
            for timestamp in (module.datetime(2026, 11, 4, 12, 30), None):
                with self.subTest(module=module.__name__, timestamp=timestamp):
                    database = object.__new__(module.DatabaseConnection)
                    connection = mock.MagicMock()
                    connection.__enter__.return_value = connection
                    connection.cursor.return_value.fetchone.return_value = (1,)
                    stats = dict(localfilecount=1, subdircount=0, localfilesize=3,
                                 filehash='abc', otherentrycount=0, files_lastwrite=timestamp)
                    result = dict(work=dict(path_hash='hash'), stats=stats)
                    with mock.patch.object(database, 'connect', return_value=connection), \
                            mock.patch.object(database, '_compact_done_subtrees'):
                        database._complete_detailed_batch_once(7, '/root', [result], hostid=1)
                    update = next(call for call in connection.cursor.return_value.execute.call_args_list
                                  if 'files_lastwrite = %s' in call.args[0])
                    self.assertEqual(update.args[0].count('%s'), len(update.args[1]))
                    self.assertEqual(update.args[1][-5 if 'time_inode_changed = %s' in update.args[0] else -4], timestamp)
                    connection.commit.assert_called_once_with()

    def test_lastwrite_uses_only_direct_included_regular_files(self):
        for module in (app, __import__('dirwalker')):
            with self.subTest(module=module.__name__), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                (root / 'old').write_bytes(b'old')
                (root / 'new').touch()
                (root / 'ignored').write_bytes(b'excluded')
                (root / 'child').mkdir()
                (root / 'child' / 'nested').write_bytes(b'nested')
                (root / 'link').symlink_to(root / 'ignored')
                os.mkfifo(root / 'pipe')
                os.utime(root / 'old', (100, 100))
                os.utime(root / 'new', (200, 200))
                db = mock.Mock()
                db.claim_work_batch.side_effect = [[dict(path=folder, parent_path=None)], []]
                with mock.patch.object(module, 'database_connection', db):
                    module.detailed_scan(folder, 7, skipdirs='ignored')
                stats = db.complete_detailed_batch.call_args.args[2][0]['stats']
                self.assertEqual(stats['files_lastwrite'],
                                 module.datetime(1970, 1, 1, 0, 3, 20))
                self.assertEqual(stats['localfilecount'], 2)
                db.claim_work_batch.side_effect = [[dict(path=folder, parent_path=None)], []]
                with mock.patch.object(module, 'database_connection', db):
                    module.detailed_scan(folder, 7, skipdirs='old,new,ignored,child')
                self.assertIsNone(db.complete_detailed_batch.call_args.args[2][0]['stats']['files_lastwrite'])

    def test_direct_and_remote_content_checks_use_same_exclusions(self):
        excluded = core.exclusion_matcher('.DS_store,@eadir')
        with tempfile.TemporaryDirectory() as folder:
            left, right = self.trees(folder)
            self.assertTrue(app._directory_contents_equal(str(left), str(right), is_excluded=excluded))
            self.assertEqual(core.directory_content_checksum(left, is_excluded=excluded),
                             core.directory_content_checksum(right, is_excluded=excluded))
            (right / 'data.zip').write_bytes(b'other real content')
            self.assertFalse(app._directory_contents_equal(str(left), str(right), is_excluded=excluded))
            self.assertNotEqual(core.directory_content_checksum(left, is_excluded=excluded),
                                core.directory_content_checksum(right, is_excluded=excluded))

    def test_coarse_does_not_enqueue_excluded_subdirectories(self):
        with tempfile.TemporaryDirectory() as folder:
            left, _ = self.trees(folder)
            db = mock.Mock()
            db.claim_work_batch.side_effect = [[dict(path=str(left), parent_path=None)], []]
            db.complete_coarse_batch.return_value = 0
            with mock.patch.object(app, 'database_connection', db):
                app.coarse_scan(str(left), 7, skipdirs='@EADIR', hostid=0)
            result = db.complete_coarse_batch.call_args.args[2][0]
            self.assertEqual(result['children'], [os.path.realpath(left / 'chapter')])


# Binarycheck freshness regression coverage.


class TimestampPersistenceTests(unittest.TestCase):
    def test_local_results_stamp_only_successful_persisted_results(self):
        database = object.__new__(version25.DatabaseConnection)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        with mock.patch.object(database, 'connect', return_value=connection), \
                mock.patch.object(database, 'mark_binarycheck_hashed') as stamp:
            for status, changed in [('equal', 1), ('different', 1), ('unreadable', 1), ('equal', 0)]:
                stamp.reset_mock()
                cursor.rowcount = changed
                database._save_binarycheck_result_once(9, 0, 2, 3, 20, status)
                self.assertEqual(stamp.call_count, int(status in ('equal', 'different') and changed > 0))

    def test_stamp_is_monotonic_and_guarded_against_concurrent_scan(self):
        database = object.__new__(DataBaseConnectionCore)
        cursor = mock.Mock()
        now = datetime(2026, 11, 5)
        database.mark_binarycheck_hashed(cursor, 9, [(0, 1), (0, 1), (1, 2)], now)
        self.assertEqual(cursor.execute.call_count, 2)
        sql, params = cursor.execute.call_args.args
        self.assertIn('GREATEST(COALESCE(d.hashed_at', sql)
        self.assertIn('j.timestart >= d.scanned_at', sql)
        self.assertEqual(params, (9, now, now, 1, 2))

    def test_remote_results_stamp_only_successful_persisted_results(self):
        database = object.__new__(DataBaseConnectionCore)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value
        with mock.patch.object(database, 'connect', return_value=connection), \
                mock.patch.object(database, 'mark_binarycheck_hashed') as stamp:
            for status, changed in [('equal', 1), ('different', 1), ('unreadable', 1), ('equal', 0)]:
                stamp.reset_mock()
                cursor.rowcount = changed
                database._save_remote_result(9, 20, 0, 1, 2, 3, status)
                self.assertEqual(stamp.call_count, int(status in ('equal', 'different') and changed > 0))


if __name__ == '__main__':
    unittest.main()
