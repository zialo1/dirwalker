"""Tests for server_operations.py."""

from unittest import mock
import DataBaseCore as core
# Importing a test module must not probe the configured production database.
with mock.patch.object(core.DataBaseConnectionCore, 'connect', side_effect=RuntimeError('offline test')):
    import version25 as app
import contextlib
import io
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
import json
from pathlib import Path
import subprocess
import textwrap
from DataBaseCLI import DataBaseCLI
import server_operations as server
import socket
import threading


def setUpModule():
    # Keep the compatibility import bound to the current runtime during server tests.
    global _previous_runtime_alias
    _previous_runtime_alias = sys.modules.get('version22')
    sys.modules['version22'] = app


def tearDownModule():
    if _previous_runtime_alias is None:
        sys.modules.pop('version22', None)
    else:
        sys.modules['version22'] = _previous_runtime_alias


ROOT = Path(__file__).resolve().parent

ALLOWED = {"version25.py", "DataBaseCore.py", "server_operations.py", "sqlstructure.py"}

BOOTSTRAP = r'''
import atexit
import importlib.abc
from importlib.machinery import PathFinder
import json
import logging
import os
from pathlib import Path
import sys

config = json.loads(os.environ["DIRWALKER_IMPORT_TEST"])
ROOT = Path(config["root"]).resolve()
OUT = Path(config["output"]).resolve()
SCENARIO = config["scenario"]
COMMAND = config["command"]
CONTROL = config["control"]
# -I excludes user-site packages; allow installed dependencies explicitly while
# retaining an isolated project import path. Spawn repeats this setup.
sys.path.extend(config["dependency_paths"])
ALLOWED = {"version25.py", "DataBaseCore.py", "server_operations.py", "sqlstructure.py"}
BLOCKED = []
sys.path.insert(0, str(ROOT))
sys.dont_write_bytecode = True

def project_file(origin):
    if not origin or origin in ("built-in", "frozen"):
        return None
    path = Path(origin).resolve()
    try:
        relative = path.relative_to(ROOT)
    except ValueError:
        return None
    if ".venv" in relative.parts or "site-packages" in relative.parts:
        return None
    return str(relative)

class BlockOtherProjectModules(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        # Explicitly protect the parser, too, even though it is stdlib.
        if fullname == "argparse":
            BLOCKED.append(fullname)
            raise AssertionError("BlockedProjectModule: argparse")
        spec = PathFinder.find_spec(fullname, path)
        origin = project_file(getattr(spec, "origin", None))
        if origin and origin not in ALLOWED:
            BLOCKED.append(fullname)
            raise AssertionError("BlockedProjectModule: " + fullname)
        return spec

sys.meta_path.insert(0, BlockOtherProjectModules())
# Guarantee that accidental constructor probes cannot connect to a real DB.
os.environ.pop("DB_USER", None)
os.environ.pop("DB_PASSWORD", None)
import version25 as app
assert not {"server_operations", "client_operations", "DataBaseCLI", "doubles_output", "argparse"} & sys.modules.keys()

EVENTS = []
def inventory():
    modules = {}
    for name, module in list(sys.modules.items()):
        origin = project_file(getattr(module, "__file__", None))
        if origin:
            modules[name] = origin
    return modules

def report():
    payload = {"pid": os.getpid(), "modules": inventory(), "blocked": BLOCKED,
               "events": EVENTS, "parser_loaded": "argparse" in sys.modules}
    OUT.mkdir(exist_ok=True)
    (OUT / (str(os.getpid()) + ".json")).write_text(json.dumps(payload))
atexit.register(report)

class Cursor:
    rowcount = 1
    def execute(self, sql, params=()):
        self.sql = sql
        EVENTS.append("binary_sql")
    def fetchone(self):
        return (1, 0)
    def fetchall(self):
        if "SELECT dirid, parentdir, files_sha256" in self.sql:
            return [(1, 0, "0" * 64, 0, 0, "1" * 64)]
        if "SHOW COLUMNS" in self.sql:
            return [("dirid",)]
        if "SELECT dirid FROM `tmp_highpriority`" in self.sql:
            return []
        if "dj.status = 'equal'" in self.sql:
            return []
        if "SELECT dirid, path" in self.sql:
            return [(1, str(OUT / "left")), (2, str(OUT / "right"))]
        return [(1, 2, str(OUT / "left"), str(OUT / "right"), "1-0", "1-0", 1, 0, 1, 0)]
    def close(self):
        pass

class Connection:
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def cursor(self): return Cursor()
    def commit(self): pass
    def rollback(self): pass

class FakeDatabase(app.DataBaseConnection):
    # Only persistence is replaced. Main, server loop, scans, file comparison,
    # dispatch and multiprocessing.run_spawn execute their production code.
    def __init__(self):
        self.database = "isolated_test"
        self.work_claims = 0
        self.job_claimed = False
    def connect(self): return Connection()
    def list_root_path_job_conflicts(self): return []
    def remote_binarycheck_runs(self, hostid): return []
    def binarycheck_exclusion_matcher(self, jobid, hostid): return lambda path: False
    def run_remote_binarycheck(self, jobid, hostid=None): return False
    def ensure_no_priority_worker(self, hostid):
        assert hostid == 0
    def validate_local_host(self, hostid):
        raise AssertionError("Server must not validate local host identity")
    def list_live_excluded_absdirs(self, hostid):
        assert hostid == 0
        EVENTS.append("exclusions:0")
        return [str(OUT / "blocked")]
    def get_recent_runids(self, days=13): return {1}
    def claim_job(self, status, include_binarycheck=False, hostid=0, userhost=None):
        assert hostid == 0 and userhost is None
        assert include_binarycheck == (COMMAND[-1].lower() == "serverplus" or COMMAND[0].lower() == "dirserver.py")
        EVENTS.append("poll:" + status)
        if SCENARIO == "error":
            app._shutdown = True
            raise RuntimeError("Deliberate polling failure")
        if SCENARIO == "idle" or self.job_claimed:
            app._shutdown = True
            return None
        self.job_claimed = True
        task = "binarycheck" if SCENARIO == "binary" else "scan"
        stage = "detailed" if SCENARIO == "detailed" else "coarse"
        return {"jobid": 91, "hostid": 0, "task": task, "stage": stage,
                "startdirname": str(OUT), "scanall": 1, "skipdirs": "", "runid": 1}
    def get_job_status(self, jobid, hostid=None): return "active"
    def set_job_status(self, jobid, status, error=None, stage=None, usererror=None, hostid=None):
        assert hostid == 0
        EVENTS.append("status:" + status)
        app._shutdown = True
        return 2
    def prepare_stage_work(self, jobid, stage, root_path, **kwargs):
        assert kwargs["hostid"] == 0
        assert app._live_excluded_absdirs[0] == (str(OUT / "blocked"),)
        EVENTS.append("scan:" + stage)
        return True
    def claim_work_batch(self, jobid, stage, limit, hostid=None):
        self.work_claims += 1
        if self.work_claims > 1: return []
        path = str(OUT.resolve())
        return [{"path": path, "path_hash": app._path_hash(path), "parent_path": None}]
    def complete_coarse_batch(self, jobid, root_path, results, **kwargs):
        children = results[0]["children"]
        assert str(OUT / "blocked") not in children
        assert str(OUT / "left") in children
        EVENTS.append("coarse_complete")
    def complete_detailed_batch(self, jobid, root_path, results, **kwargs):
        assert results[0]["stats"]["localfilecount"] >= 1
        EVENTS.append("detailed_complete")
    def rebuild_subtree_statistics(self, root_path, hostid=0):
        EVENTS.append("rebuild")
    def run_binarycheck_job(self, jobid, hostid=None):
        super().run_binarycheck_job(jobid, hostid=hostid)
        EVENTS.append("binary_complete")

app.database_connection = FakeDatabase()
app.POLL_SECONDS = 0
# Exercise logging and formatter without sending UDP packets.
app.SysLogHandler = logging.NullHandler
app.SysLogHandler.LOG_DAEMON = 3
class NoNetworkSyslog(logging.NullHandler):
    LOG_DAEMON = 3
    def __init__(self, **kwargs): super().__init__()
app.SysLogHandler = NoNetworkSyslog

if __name__ == "__main__":
    if CONTROL == "import":
        __import__("client_operations")
    elif CONTROL == "alias":
        import types
        rogue = types.ModuleType("unrelated_alias")
        rogue.__file__ = str(ROOT / "client_operations.py")
        sys.modules["unrelated_alias"] = rogue
    sys.argv = COMMAND
    result = app.main()
    report()
    assert result == 0, result
    assert not BLOCKED, BLOCKED
    assert set(inventory().values()) == ALLOWED, inventory()
    assert "argparse" not in sys.modules
'''

class TestServerOperations(unittest.TestCase):
    def test_localserver_claims_only_hostid_one_jobs_for_this_host(self):
        operation = app.server_operations(SimpleNamespace(mode="localserver", local_hostid=1))
        claimed = {"jobid": 17, "status": "insert", "stage": "coarse"}

        def stop_after_job(*_args):
            app._shutdown = True

        with mock.patch.object(app, "_shutdown", False), \
             mock.patch.object(app.signal, "signal"), \
             mock.patch.object(app, "claim_job", side_effect=[claimed]) as claim, \
             mock.patch.object(app, "process_job", side_effect=stop_after_job), \
             mock.patch.object(operation.database, "validate_local_host", side_effect=lambda hostid: hostid), \
             mock.patch.object(operation.database, "list_live_excluded_absdirs", return_value=[]), \
             mock.patch.object(operation.database, "get_recent_runids", return_value=set()), \
             mock.patch.object(operation, "log_server_stopped"), \
             mock.patch.object(app, "POLL_SECONDS", 0):
            with mock.patch.object(app.socket, "gethostname", return_value="host-one"):
                with mock.patch.object(operation.database, "ensure_no_priority_worker"), \
                        mock.patch.object(operation.database, "list_root_path_job_conflicts", return_value=[]):
                    operation.run_server()
        claim.assert_called_once_with("paused", True, 1, "host-one")

    def test_child_entry_routes_scan_stage(self):
        with mock.patch.object(app, "coarse_scan") as coarse:
            app.server_operations._child_entry("coarse", "/tmp/root", 17)
        coarse.assert_called_once_with("/tmp/root", 17, "", 1, (), 0)

        with mock.patch.object(app, "detailed_scan") as detailed:
            app.server_operations._child_entry("detailed", "/tmp/root", 17)
        detailed.assert_called_once_with("/tmp/root", 17, 1, (), "scan", 0)

    def test_log_job_error_writes_to_server_logger(self):
        operation = app.server_operations(SimpleNamespace(mode="server"))
        logger = mock.Mock()
        operation._logger = logger
        with mock.patch.object(operation, "_configure_syslog") as configure:
            operation.log_job_error(42, "scan failed")
        configure.assert_called_once_with()
        logger.error.assert_called_once_with(
            "Job %s auf error gesetzt: %s", 42, "scan failed"
        )

    def test_job_start_is_logged(self):
        operation = app.server_operations(SimpleNamespace(mode="server"))
        logger = mock.Mock()
        operation._logger = logger
        with mock.patch.object(operation, "_configure_syslog") as configure:
            operation.log_job_started(42, "coarse", "/tmp/root")
        configure.assert_called_once_with()
        logger.info.assert_called_once_with(
            "Job %s beginnt: Stufe %s, Verzeichnis %s", 42, "coarse", "/tmp/root"
        )

    def test_server_stop_is_logged(self):
        operation = app.server_operations(SimpleNamespace(mode="server"))
        logger = mock.Mock()
        operation._logger = logger
        with mock.patch.object(operation, "_configure_syslog") as configure:
            operation.log_server_stopped()
        configure.assert_called_once_with()
        logger.info.assert_called_once_with("Servermodus beendet")

    def test_syslog_handler_targets_localhost(self):
        operation = app.server_operations(SimpleNamespace(mode="server"))
        logger = mock.Mock()
        handler = mock.Mock()
        with mock.patch.object(app.logging, "getLogger", return_value=logger), \
             mock.patch.object(app, "SysLogHandler") as handler_type:
            handler_type.return_value = handler
            operation._configure_syslog()
        handler_type.assert_called_once_with(
            address=("localhost", 5101), facility=handler_type.LOG_DAEMON
        )
        logger.addHandler.assert_called_once_with(handler)

    def test_server_reclaims_paused_job_from_coarse_stage(self):
        operation = app.server_operations(SimpleNamespace(mode="server"))
        job = {
            "jobid": 23, "startdirname": "/data", "task": "scan",
            "stage": "coarse", "status": "paused",
        }

        def stop_after_job(_job, _stage, _server):
            app._shutdown = True

        with mock.patch.object(app, "_shutdown", False), \
             mock.patch.object(app.signal, "signal"), \
             mock.patch.object(app, "claim_job", side_effect=[job]) as claim, \
             mock.patch.object(app, "process_job", side_effect=stop_after_job) as process, \
             mock.patch.object(operation.database, "validate_local_host", side_effect=lambda hostid: hostid), \
             mock.patch.object(operation.database, "list_live_excluded_absdirs", return_value=[]), \
             mock.patch.object(operation.database, "get_recent_runids", return_value=set()), \
             mock.patch.object(operation, "log_server_stopped"), \
             mock.patch.object(app, "POLL_SECONDS", 0):
            with mock.patch.object(operation.database, "ensure_no_priority_worker"), \
                        mock.patch.object(operation.database, "list_root_path_job_conflicts", return_value=[]):
                operation.run_server()
        claim.assert_called_once_with("paused", False, 0, None)
        process.assert_called_once_with(job, "coarse", operation)

    def test_run_spawn_terminates_child_when_job_is_stopped(self):
        operation = app.server_operations(SimpleNamespace(mode="server"))
        process = mock.Mock()
        process.is_alive.return_value = True
        context = mock.Mock()
        context.Process.return_value = process
        with mock.patch.object(app.multiprocessing, "get_context", return_value=context), \
             mock.patch.object(operation.database, "get_job_status", return_value="stopped"), \
             mock.patch.object(app, "_shutdown", False):
            result = operation.run_spawn("coarse", "/tmp/root", 17)
        self.assertEqual(result, "stopped")
        process.terminate.assert_called_once_with()
        self.assertEqual(
            process.join.call_args_list,
            [mock.call(timeout=0.5), mock.call()],
        )

    def test_run_spawn_uses_spawn_context_and_static_child_entry(self):
        operation = app.server_operations(SimpleNamespace(mode="server"))
        process = mock.Mock()
        process.is_alive.side_effect = [False]
        process.exitcode = 0
        context = mock.Mock()
        context.Process.return_value = process
        with mock.patch.object(app.multiprocessing, "get_context", return_value=context) as get_context, \
             mock.patch.object(operation.database, "get_job_status", return_value="active"):
            result = operation.run_spawn("coarse", "/tmp/root", 17)
        self.assertEqual(result, "ok")
        get_context.assert_called_once_with("spawn")
        process.start.assert_called_once_with()
        self.assertIs(context.Process.call_args.kwargs["target"], app.server_operations._child_entry)

    def test_run_registers_shutdown_handlers_and_uses_global_connection(self):
        operation = app.server_operations(SimpleNamespace(mode="server"))
        self.assertIs(operation.database, app.database_connection)
        with mock.patch.object(app, "_shutdown", True), \
             mock.patch.object(app.signal, "signal") as register_signal, \
             mock.patch.object(operation.database, "validate_local_host", side_effect=lambda hostid: hostid), \
             mock.patch.object(operation.database, "list_live_excluded_absdirs", return_value=[]), \
             mock.patch.object(operation.database, "get_recent_runids", return_value=set()) as recent_runids, \
             mock.patch.object(operation, "log_server_stopped"), \
             contextlib.redirect_stdout(io.StringIO()):
            with mock.patch.object(operation.database, "ensure_no_priority_worker"), \
                        mock.patch.object(operation.database, "list_root_path_job_conflicts", return_value=[]):
                operation.run()
        self.assertEqual(register_signal.call_count, 2)
        self.assertEqual(
            [call.args[0] for call in register_signal.call_args_list],
            [app.signal.SIGINT, app.signal.SIGTERM],
        )
        recent_runids.assert_called_once_with(days=13)


class TestServerEntryPoints(unittest.TestCase):
    def test_serverplus_claims_all_job_tasks(self):
        operation = app.server_operations(SimpleNamespace(mode="serverplus"))
        def stop_after_empty_poll(*_args):
            app._shutdown = True
            return None

        with mock.patch.object(app, "_shutdown", False), \
             mock.patch.object(
                 operation.database, "get_recent_runids", return_value=set()
             ), mock.patch.object(operation.database, "validate_local_host", side_effect=lambda hostid: hostid), \
             mock.patch.object(operation.database, "list_live_excluded_absdirs", return_value=[]), mock.patch.object(operation, "log_server_stopped"), \
             mock.patch.object(app.signal, "signal"), \
             mock.patch.object(
                 app, "claim_job", side_effect=stop_after_empty_poll
             ) as claim:
            with mock.patch.object(operation.database, "ensure_no_priority_worker"), \
                    mock.patch.object(operation.database, "remote_binarycheck_runs", return_value=[]), \
                    mock.patch.object(operation.database, "list_root_path_job_conflicts", return_value=[]):
                operation.run_server()
        self.assertEqual(
            claim.call_args_list,
            [mock.call("paused", True, 0, None), mock.call("insert", True, 0, None),
             mock.call("pending", True, 0, None)],
        )


class TestServerExclusions(unittest.TestCase):
    def test_server_loads_host_snapshot_once_across_multiple_jobs(self):
        for mode, hostid in (("server", 0), ("localserver", 1)):
            with self.subTest(mode=mode):
                operation = app.server_operations(SimpleNamespace(mode=mode, local_hostid=hostid))
                jobs = [dict(jobid=1, stage="coarse"), dict(jobid=2, stage="coarse")]
                def process(*args):
                    if args[0]["jobid"] == 2:
                        app._shutdown = True
                with mock.patch.object(app, "_shutdown", False), \
                     mock.patch.dict(app._live_excluded_absdirs, {}, clear=True), \
                     mock.patch.object(operation.database, "validate_local_host", side_effect=lambda hostid: hostid), \
             mock.patch.object(operation.database, "list_live_excluded_absdirs", return_value=["/srv/private"]) as read, \
                     mock.patch.object(operation.database, "get_recent_runids", return_value=set()), \
                     mock.patch.object(app, "claim_job", side_effect=jobs), \
                     mock.patch.object(app, "process_job", side_effect=process), \
                     mock.patch.object(app.signal, "signal"), \
                     mock.patch.object(operation, "log_server_stopped"), \
                     mock.patch.object(app, "POLL_SECONDS", 0), \
                     contextlib.redirect_stdout(io.StringIO()):
                    with mock.patch.object(operation.database, "ensure_no_priority_worker"), \
                        mock.patch.object(operation.database, "list_root_path_job_conflicts", return_value=[]):
                        operation.run_server()
                    self.assertEqual(app._live_excluded_absdirs[hostid], ("/srv/private",))
                read.assert_called_once_with(hostid)

    def test_spawn_receives_startup_snapshot(self):
        operation = app.server_operations(SimpleNamespace(mode="server"))
        context = mock.Mock()
        context.Process.return_value.is_alive.return_value = False
        context.Process.return_value.exitcode = 0
        with mock.patch.dict(app._live_excluded_absdirs, {0: ("/srv/private",)}, clear=True), \
             mock.patch.object(app.multiprocessing, "get_context", return_value=context):
            operation.run_spawn("coarse", "/srv", 1)
        self.assertEqual(context.Process.call_args.kwargs["args"][-2], ("/srv/private",))


class TestCoarseGlob(unittest.TestCase):
    def test_child_routes_only_local_coarse_to_glob(self):
        with mock.patch.object(app, "coarse_scan") as coarse:
            app.server_operations._child_entry("coarse", "/root", 1, hostid=1, coarse_glob=True)
            self.assertTrue(coarse.call_args.kwargs["coarse_glob"])
        with mock.patch.object(app, "detailed_scan") as detailed:
            app.server_operations._child_entry("detailed", "/root", 1, hostid=1, coarse_glob=True)
            detailed.assert_called_once_with("/root", 1, 1, (), "scan", 1)

    def test_local_coarse_spawn_passes_glob_flag(self):
        operation = app.server_operations(SimpleNamespace(mode="localserver", local_hostid=1, coarse_glob=True))
        context = mock.Mock()
        context.Process.return_value.is_alive.return_value = False
        context.Process.return_value.exitcode = 0
        with mock.patch.object(app.multiprocessing, "get_context", return_value=context):
            operation.run_spawn("coarse", "/root", 1, hostid=1)
            self.assertTrue(context.Process.call_args.kwargs["args"][-1])
            operation.run_spawn("detailed", "/root", 1, hostid=1)
            self.assertFalse(context.Process.call_args.kwargs["args"][-1])


class TestServerHostSelection(unittest.TestCase):
    def test_invalid_local_host_stops_before_polling_or_signals(self):
        operation = app.server_operations(SimpleNamespace(mode="localserver", local_hostid=7))
        with mock.patch.object(operation.database, "validate_local_host", side_effect=RuntimeError("falscher Host")), \
             mock.patch.object(app, "claim_job") as claim, \
             mock.patch.object(app.signal, "signal") as signal_handler, \
             self.assertRaisesRegex(RuntimeError, "falscher Host"):
            with mock.patch.object(operation.database, "ensure_no_priority_worker"), \
                        mock.patch.object(operation.database, "list_root_path_job_conflicts", return_value=[]):
                operation.run_server()
        claim.assert_not_called()
        signal_handler.assert_not_called()

    def test_positive_local_id_selects_its_jobs_and_snapshot(self):
        operation = app.server_operations(SimpleNamespace(mode="localserver", local_hostid=7))
        def stop(*args):
            app._shutdown = True
        with mock.patch.object(app, "_shutdown", False), \
             mock.patch.object(operation.database, "validate_local_host", return_value=7) as validate, \
             mock.patch.object(operation.database, "list_live_excluded_absdirs", return_value=[]) as exclusions, \
             mock.patch.object(operation.database, "get_recent_runids", return_value=set()), \
             mock.patch.object(app, "claim_job", side_effect=stop) as claim, \
             mock.patch.object(app.signal, "signal"), \
             mock.patch.object(operation, "log_server_stopped"), \
             mock.patch.dict(app._live_excluded_absdirs, {}, clear=True), \
             contextlib.redirect_stdout(io.StringIO()):
            with mock.patch.object(operation.database, "ensure_no_priority_worker"), \
                    mock.patch.object(operation.database, "remote_binarycheck_runs", return_value=[]), \
                    mock.patch.object(operation.database, "list_root_path_job_conflicts", return_value=[]):
                operation.run_server()
        validate.assert_called_once_with(7)
        exclusions.assert_called_once_with(7)
        self.assertTrue(all(call.args[2] == 7 for call in claim.call_args_list))


class TestServerHostValidation(unittest.TestCase):
    def test_server_modes_skip_local_host_validation(self):
        for mode in ("server", "serverplus"):
            operation = app.server_operations(SimpleNamespace(mode=mode))
            with self.subTest(mode=mode), mock.patch.object(app, "_shutdown", True), \
                 mock.patch.object(operation.database, "validate_local_host") as validate, \
                 mock.patch.object(operation.database, "list_live_excluded_absdirs", return_value=[]) as excludes, \
                 mock.patch.object(operation.database, "get_recent_runids", return_value=set()), \
                 mock.patch.object(operation, "log_server_stopped"), mock.patch.object(app.signal, "signal"), \
                 contextlib.redirect_stdout(io.StringIO()):
                with mock.patch.object(operation.database, "ensure_no_priority_worker"), \
                        mock.patch.object(operation.database, "list_root_path_job_conflicts", return_value=[]):
                    operation.run_server()
            validate.assert_not_called()
            excludes.assert_called_once_with(0)


class TestServerModuleLoading(unittest.TestCase):
    def run_isolated(self, command, scenario="idle", control=""):
        with tempfile.TemporaryDirectory(prefix="dirwalker-import-test-") as temp:
            temp = Path(temp)
            script = temp / "bootstrap.py"
            script.write_text(textwrap.dedent(BOOTSTRAP))
            output = temp / "output"
            output.mkdir()
            for name in ("left", "right", "blocked"):
                (output / name).mkdir()
            for name in ("left", "right"):
                (output / name / "same.txt").write_text("same bytes")
            (output / "file.txt").write_text("scan this file")
            env = os.environ.copy()
            env.pop("PYTHONPATH", None)
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            env["DIRWALKER_IMPORT_TEST"] = json.dumps({
                "root": str(ROOT), "output": str(output), "scenario": scenario,
                "command": command, "control": control,
                "dependency_paths": [path for path in sys.path
                                     if path and ("site-packages" in path or "dist-packages" in path)],
            })
            process = subprocess.run(
                [sys.executable, "-I", str(script), str(ROOT), str(output),
                 scenario, json.dumps(command), control],
                cwd=temp, env=env, text=True, capture_output=True, timeout=30,
            )
            reports = [json.loads(path.read_text()) for path in output.glob("*.json")]
        return process, reports

    def assert_clean(self, command, scenario="idle"):
        process, reports = self.run_isolated(command, scenario)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertTrue(reports, "No module inventory produced")
        if scenario != "error":
            self.assertNotIn("Traceback", process.stderr, process.stderr)
        for report in reports:
            self.assertFalse(report["blocked"], report)
            self.assertFalse(report["parser_loaded"], report)
            self.assertNotIn("status:error", report["events"], report)
            self.assertEqual(set(report["modules"].values()), ALLOWED, report)
        return reports

    def test_server_and_serverplus_idle(self):
        for mode in ("server", "serverplus"):
            with self.subTest(mode=mode):
                reports = self.assert_clean(["version25.py", mode])
                self.assertTrue(any("poll:paused" in report["events"] for report in reports))

    def test_case_insensitive_server_modes(self):
        for mode in ("SERVER", "Server", "SERVERPLUS", "ServerPlus"):
            with self.subTest(mode=mode):
                self.assert_clean(["version25.py", mode])

    def test_deployed_script_names_without_arguments(self):
        for filename in ("dirserver.py", "DIRSERVER.PY", "fileserver.py", "FILESERVER.PY"):
            with self.subTest(filename=filename):
                self.assert_clean([filename])

    def test_real_spawned_coarse_scans(self):
        for mode in ("server", "serverplus"):
            with self.subTest(mode=mode):
                reports = self.assert_clean(["version25.py", mode], "coarse")
                self.assertGreaterEqual(len(reports), 2, "Missing child-process inventory")
                self.assertTrue(any("coarse_complete" in report["events"] for report in reports))
                self.assertTrue(any("status:pending" in report["events"] for report in reports))

    def test_real_spawned_detailed_scans(self):
        for mode in ("server", "serverplus"):
            with self.subTest(mode=mode):
                reports = self.assert_clean(["version25.py", mode], "detailed")
                self.assertGreaterEqual(len(reports), 2)
                self.assertTrue(any("detailed_complete" in report["events"] for report in reports))
                self.assertTrue(any("rebuild" in report["events"] for report in reports))
                self.assertTrue(any("status:closed" in report["events"] for report in reports))

    def test_serverplus_spawned_binarycheck(self):
        reports = self.assert_clean(["version25.py", "serverplus"], "binary")
        self.assertGreaterEqual(len(reports), 2)
        self.assertTrue(any("binary_complete" in report["events"] for report in reports))
        self.assertTrue(any("status:closed" in report["events"] for report in reports))

    def test_polling_errors_do_not_load_client_modules(self):
        for mode in ("server", "serverplus"):
            with self.subTest(mode=mode):
                self.assert_clean(["version25.py", mode], "error")

    def test_guard_detects_forbidden_project_import(self):
        process, reports = self.run_isolated(["version25.py", "server"], control="import")
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("BlockedProjectModule: client_operations", process.stderr)

    def test_inventory_detects_forbidden_module_under_alias(self):
        process, reports = self.run_isolated(["version25.py", "server"], control="alias")
        self.assertNotEqual(process.returncode, 0)
        self.assertTrue(any("client_operations.py" in report["modules"].values() for report in reports))


class TestPriorityStartupCheck(unittest.TestCase):
    def test_existing_priority_process_blocks_before_claiming_or_spawning(self):
        operation = app.server_operations(SimpleNamespace(mode='serverplus'))
        with mock.patch.object(operation.database, 'list_root_path_job_conflicts', return_value=[]), \
             mock.patch.object(operation.database, 'ensure_no_priority_worker', side_effect=RuntimeError('PID 1234')) as check, \
             mock.patch.object(app, 'claim_job') as claim, \
             mock.patch.object(app.signal, 'signal') as signals, \
             self.assertRaisesRegex(RuntimeError, 'PID 1234'):
            operation.run_server()
        check.assert_called_once_with(0)
        claim.assert_not_called()
        signals.assert_has_calls([mock.call(app.signal.SIGINT, operation.handle_signal),
                                  mock.call(app.signal.SIGTERM, operation.handle_signal)])

    def test_startup_check_uses_validated_local_host(self):
        operation = app.server_operations(SimpleNamespace(mode='localserver', local_hostid=7))
        with mock.patch.object(operation.database, 'list_root_path_job_conflicts', return_value=[]), \
             mock.patch.object(operation.database, 'validate_local_host', return_value=7), \
             mock.patch.object(operation.database, 'ensure_no_priority_worker', side_effect=RuntimeError('occupied')) as check, \
             self.assertRaises(RuntimeError):
            operation.run_server()
        check.assert_called_once_with(7)


def _hold_test_server_instance(mode, hostid, directory, ready):
    import os
    import server_operations as server_module
    operation = app.server_operations(SimpleNamespace(mode=mode, local_hostid=hostid))
    operation.database = SimpleNamespace(host='test-db', port=3306, database='singleton-test',
                                         validate_local_host=lambda value: value,
                                         list_root_path_job_conflicts=lambda: [],
                                         start_wake_monitor=lambda *args: None,
                                         stop_wake_monitor=lambda: None)
    original_lock = server_module._server_instance_lock
    def run(_hostid):
        ready.send(('ready', os.getpid(), _hostid))
        if ready.poll(15):
            ready.recv()
    operation._run_server = run
    try:
        with mock.patch.object(server_module, '_server_instance_lock',
                               side_effect=lambda db, host: original_lock(db, host, directory)):
            operation.run_server()
    finally:
        ready.close()


class TestExclusiveServerInstances(unittest.TestCase):
    def attempt(self, mode, hostid, directory, database_name='singleton-test'):
        import server_operations as server_module
        operation = app.server_operations(SimpleNamespace(mode=mode, local_hostid=hostid))
        operation.database = SimpleNamespace(host='test-db', port=3306, database=database_name,
                                             validate_local_host=lambda value: value,
                                         list_root_path_job_conflicts=lambda: [],
                                         start_wake_monitor=lambda *args: None,
                                         stop_wake_monitor=lambda: None)
        operation._run_server = mock.Mock(return_value=0)
        original_lock = server_module._server_instance_lock
        with mock.patch.object(server_module, '_server_instance_lock',
                               side_effect=lambda db, host: original_lock(db, host, directory)):
            operation.run_server()
        return operation

    @contextlib.contextmanager
    def running(self, mode, hostid):
        import multiprocessing
        import tempfile
        context = multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as directory:
            reader, writer = context.Pipe(duplex=True)
            process = context.Process(target=_hold_test_server_instance,
                                      args=(mode, hostid, directory, writer))
            process.start()
            writer.close()
            try:
                self.assertTrue(reader.poll(5), 'server did not acquire the lock')
                message = reader.recv()
                self.assertEqual(message, ('ready', process.pid, hostid))
                yield directory, process
            finally:
                if process.is_alive():
                    try:
                        reader.send(('finish',))
                    except (BrokenPipeError, OSError):
                        pass
                process.join(timeout=5)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                process.close()
                reader.close()

    def test_two_localservers_with_same_hostid_are_rejected(self):
        with self.running('localserver', 7) as (directory, process):
            with self.assertRaisesRegex(RuntimeError, f'Host-ID 7.*PID {process.pid}'):
                self.attempt('localserver', 7, directory)

    def test_two_servers_and_server_serverplus_combinations_are_rejected(self):
        for first, second in (('server', 'server'), ('server', 'serverplus'),
                              ('serverplus', 'server'), ('serverplus', 'serverplus')):
            with self.subTest(first=first, second=second), self.running(first, 0) as (directory, process):
                with self.assertRaisesRegex(RuntimeError, 'Serverinstanz'):
                    self.attempt(second, 0, directory)

    def test_different_hostids_can_run_concurrently(self):
        with self.running('localserver', 7) as (directory, process):
            operation = self.attempt('localserver', 8, directory)
            operation._run_server.assert_called_once_with(8)
            operation = self.attempt('serverplus', 0, directory)
            operation._run_server.assert_called_once_with(0)

    def test_different_databases_are_independent(self):
        with self.running('localserver', 7) as (directory, process):
            operation = self.attempt('localserver', 7, directory, database_name='another-db')
            operation._run_server.assert_called_once_with(7)

    def test_forced_exit_releases_lock_despite_leftover_pid_file(self):
        with self.running('localserver', 7) as (directory, process):
            process.terminate()
            process.join(timeout=5)
            self.assertFalse(process.is_alive())
            operation = self.attempt('localserver', 7, directory)
            operation._run_server.assert_called_once_with(7)

    def test_normal_exit_allows_restart_without_removing_lock_file(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as directory:
            self.attempt('localserver', 7, directory)
            self.assertEqual(len(list(Path(directory).glob('dirwalker-server-*.lock'))), 1)
            operation = self.attempt('localserver', 7, directory)
            operation._run_server.assert_called_once_with(7)


class _SmartStartError(RuntimeError):
    def __init__(self, errno, message):
        super().__init__(message)
        self.errno = errno


class TestSmartLocalStartDiagnostics(unittest.TestCase):
    def operation(self):
        return app.server_operations(SimpleNamespace(mode='localserver', local_hostid=7, smart_start=True))

    def test_success_uses_the_existing_localserver_start_path(self):
        operation = self.operation()
        with mock.patch.object(operation, '_find_smart_local_host', return_value=7), \
             mock.patch.object(operation, 'run_server', return_value=0) as run, \
             mock.patch.object(operation, '_explain_local_start_failure') as explain:
            self.assertEqual(operation.run(), 0)
        run.assert_called_once_with()
        explain.assert_not_called()

    def test_failed_start_reports_original_error_and_specific_next_step(self):
        cases = [
            (_SmartStartError(1054, "Unknown column 'r.pid'"), 'migrate_jobsrunid_pid.sql'),
            (_SmartStartError(1045, 'Access denied'), 'DB_PASSWORD'),
            (_SmartStartError(2003, 'Connection refused'), 'DB_PORT'),
            (RuntimeError('Host-ID 7 ist nicht registriert'), '--setmyhostid'),
            (RuntimeError('Host-ID 7 passt nicht zum lokalen Benutzer/Rechner'), 'show hosts'),
            (RuntimeError('Serverinstanz läuft bereits (PID 1234)'), 'regulär beenden'),
            (PermissionError('permission denied'), 'Dateisystemrechte'),
            (RuntimeError('unexpected failure'), ' check '),
        ]
        for error, expected in cases:
            operation = self.operation()
            output = io.StringIO()
            with self.subTest(error=error), mock.patch.object(operation, '_find_smart_local_host', return_value=7), \
                 mock.patch.object(operation, 'run_server', side_effect=error) as run, \
                 contextlib.redirect_stderr(output):
                self.assertEqual(operation.run(), 1)
            run.assert_called_once()
            self.assertIn(str(error), output.getvalue())
            self.assertIn(expected, output.getvalue())
            self.assertIn('Vorschlag:', output.getvalue())

    def test_normal_localserver_still_propagates_errors_to_existing_handler(self):
        operation = app.server_operations(SimpleNamespace(mode='localserver', local_hostid=7))
        with mock.patch.object(operation, 'run_server', side_effect=RuntimeError('failed')), \
             self.assertRaises(RuntimeError):
            operation.run()


class TestAutomaticSmartHostSelection(unittest.TestCase):
    identity = ('alex', '/home/alex', 'Linux', 'pc')

    def operation(self, hostids, jobs=()):
        operation = app.server_operations(SimpleNamespace(mode='localserver', local_hostid=None, smart_start=True))
        database = mock.Mock()
        database.list_root_path_job_conflicts.return_value = []
        database._local_host_values.return_value = self.identity
        database.list_hosts.return_value = [
            (hostid, 'alex', '/home/alex', None, 'Linux', 'pc', None, f'tmp{hostid}_job_work')
            for hostid in hostids]
        database.local_start_jobs.return_value = list(jobs)
        operation.database = database
        return operation

    def test_host_zero_is_excluded_and_matching_positive_id_is_used(self):
        operation = self.operation([0, 7])
        with mock.patch.object(operation, 'run_server', return_value=0) as run, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(operation.run(), 0)
        self.assertEqual(operation.args.local_hostid, 7)
        operation.database.local_start_jobs.assert_called_once_with([7])
        run.assert_called_once()

    def test_error_status_displays_stored_failure_and_does_not_start_or_reset(self):
        operation = self.operation([0, 7], [dict(jobid=13, hostid=7, status='error', stage='detailed',
                                                startdirname='/data', errmsg='PermissionError', usererror='unreadable')])
        operation.database.validate_local_host.return_value = 7
        operation.database.host, operation.database.port = 'test', 3306
        operation.database.database = 'testdb'
        output = io.StringIO()
        with mock.patch.object(operation, '_run_server') as run, \
             mock.patch('builtins.input', return_value='n'), \
             mock.patch('DataBaseCLI.DataBaseCLI.reset_error_job') as reset, \
             contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            self.assertEqual(operation.run(), 1)
        for message in ('Job 13', 'Host-ID 7', 'status=error', 'PermissionError', 'unreadable'):
            self.assertIn(message, output.getvalue())
        run.assert_not_called()
        operation.database.cleanup_server_queries.assert_called_once_with(7, confirm=True)
        reset.assert_not_called()

    def test_confirmed_error_reset_reuses_cli_method_only_for_selected_host(self):
        operation = self.operation([7])
        operation._smart_errors = [dict(jobid=13, hostid=7, status='error', stage='detailed',
                                       startdirname='/data'),
                                   dict(jobid=14, hostid=8, status='error')]
        with mock.patch('builtins.input', return_value='y') as prompt, \
             mock.patch('DataBaseCLI.DataBaseCLI.reset_error_job', return_value=True) as reset:
            self.assertTrue(operation._reset_smart_errors(7))
        prompt.assert_called_once()
        reset.assert_called_once_with(13, hostid=7)

    def test_failed_reset_prevents_start(self):
        operation = self.operation([7])
        operation._smart_errors = [dict(jobid=13, hostid=7, status='error', stage='coarse', startdirname='/data')]
        with mock.patch('builtins.input', return_value='y'), \
             mock.patch('DataBaseCLI.DataBaseCLI.reset_error_job', return_value=False):
            self.assertFalse(operation._reset_smart_errors(7))

    def test_only_host_with_unfinished_work_is_selected(self):
        operation = self.operation([7, 8], [dict(jobid=14, hostid=8, status='pending')])
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(operation._find_smart_local_host(), 8)

    def test_requested_host_resolves_multiple_registrations(self):
        operation = self.operation([7, 8])
        operation.args.local_hostid = 8
        self.assertEqual(operation._find_smart_local_host(), 8)
        operation.database.local_start_jobs.assert_called_once_with([8])

    def test_requested_foreign_host_is_rejected(self):
        operation = self.operation([7])
        operation.args.local_hostid = 8
        with self.assertRaisesRegex(RuntimeError, 'passt nicht zum lokalen'):
            operation._find_smart_local_host()
        operation.database.local_start_jobs.assert_not_called()

    def test_ambiguous_positive_ids_are_reported_without_guessing(self):
        operation = self.operation([0, 7, 8])
        output = io.StringIO()
        with mock.patch.object(operation, 'run_server') as run, contextlib.redirect_stderr(output):
            self.assertEqual(operation.run(), 1)
        run.assert_not_called()
        self.assertIn('7, 8', output.getvalue())
        self.assertIn('localserver --hostid HOSTID', output.getvalue())

    def test_host_zero_alone_never_starts_a_server(self):
        operation = self.operation([0])
        output = io.StringIO()
        with mock.patch.object(operation, 'run_server') as run, contextlib.redirect_stderr(output):
            self.assertEqual(operation.run(), 1)
        run.assert_not_called()
        operation.database.local_start_jobs.assert_not_called()
        self.assertIn('--setmyhostid', output.getvalue())


class TestBoundedShutdown(unittest.TestCase):
    def test_signal_interrupts_child_and_starts_cleanup_only_once(self):
        import server_operations as module
        operation = app.server_operations(SimpleNamespace(mode='localserver'))
        operation._server_hostid, operation._server_group = 7, 'group'
        child = mock.Mock(pid=123)
        with mock.patch.object(app, '_current_child', child), \
             mock.patch.object(app, '_shutdown', False), \
             mock.patch.object(module.os, 'kill') as kill, \
             mock.patch.object(module.threading, 'Thread') as thread:
            operation.handle_signal(2, None)
            operation.handle_signal(2, None)
            kill.assert_called_once_with(123, module.signal.SIGINT)
            self.assertTrue(app._shutdown)
            self.assertEqual(thread.call_count, 2)
            self.assertLessEqual(operation._shutdown_deadline - module.time.monotonic(), 20)

    def test_clean_exit_after_interrupt_is_paused_not_success(self):
        operation = app.server_operations(SimpleNamespace(mode='server'))
        child = mock.Mock(exitcode=0)
        child.is_alive.return_value = False
        context = mock.Mock()
        context.Process.return_value = child
        with mock.patch.object(app.multiprocessing, 'get_context', return_value=context), \
             mock.patch.object(app, '_shutdown', True):
            self.assertEqual(operation.run_spawn('coarse', '/root', 1), 'paused')

    def test_watchdog_enforces_twenty_second_deadline_and_kills_scan_group(self):
        import server_operations as module
        operation = app.server_operations(SimpleNamespace(mode='server'))
        child = mock.Mock(pid=123)
        operation._shutdown_finished = mock.Mock()
        operation._shutdown_finished.wait.return_value = False
        with mock.patch.object(app, '_shutdown', False), \
             mock.patch.object(app, '_current_child', child), \
             mock.patch.object(module.os, 'kill'), \
             mock.patch.object(module.os, '_exit') as exit_process, \
             mock.patch.object(module.os, 'killpg') as kill_group, \
             mock.patch.object(module.threading, 'Thread') as thread:
            operation.handle_signal(2, None)
            watchdog = thread.call_args_list[1].kwargs['target']
            watchdog()
        operation._shutdown_finished.wait.assert_called_once_with(20)
        kill_group.assert_called_once_with(123, module.signal.SIGKILL)
        exit_process.assert_called_once_with(130)


class TestRealInterruptShutdown(unittest.TestCase):
    def test_idle_server_modes_stop_on_actual_ctrl_c_signal(self):
        import signal
        import select
        script = r'''
import signal, socket, time, types, uuid
import server_operations as module
class Database:
    def start_wake_monitor(self, *args): pass
    def stop_wake_monitor(self): pass
    def consume_wake_event(self): return False
    host, port, database = 'test', 3306, 'shutdown-' + uuid.uuid4().hex
    def validate_local_host(self, hostid): return hostid
    def ensure_no_priority_worker(self, hostid): pass
    def list_root_path_job_conflicts(self): return []
    def list_live_excluded_absdirs(self, hostid): return []
    def get_recent_runids(self, days): return set()
    def cleanup_server_queries(self, hostid, group, timeout):
        time.sleep(0.05)
        print('DB-CLEANUP-DONE', flush=True)
runtime = types.SimpleNamespace(database_connection=Database(), _current_child=None,
    _shutdown=False, _live_excluded_absdirs={}, POLL_SECONDS=120, DATABASE_DRIVER='test',
    LOCALSERVER_LOG_FILE='test.log', Error=Exception, claim_job=lambda *args: None)
module._runtime = lambda: runtime
operation = module.server_operations(types.SimpleNamespace(mode=MODE, local_hostid=7))
operation.log_server_stopped = lambda: None
operation.run_server()
'''
        for mode in ('server', 'serverplus', 'localserver'):
            with self.subTest(mode=mode):
                code = 'MODE = ' + repr(mode) + '\n' + script
                process = subprocess.Popen([sys.executable, '-u', '-c', code], cwd=str(ROOT),
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    prefix = ''
                    while 'Server aktiv' not in prefix:
                        self.assertTrue(select.select([process.stdout], [], [], 5)[0], 'Server startup timed out')
                        line = os.read(process.stdout.fileno(), 4096).decode()
                        if not line:
                            output, error = process.communicate(timeout=5)
                            self.fail('Server exited before startup:\n' + error.decode())
                        prefix += line
                    process.send_signal(signal.SIGINT)
                    output, error = process.communicate(timeout=5)
                    output, error = output.decode(), error.decode()
                    self.assertEqual(process.returncode, 0, error)
                    self.assertIn('höchstens 20 Sekunden', output)
                    self.assertIn('DB-CLEANUP-DONE', output)
                    self.assertIn('Server beendet', output)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate()


class TestStatementTimeoutReporting(unittest.TestCase):
    def test_child_reports_timeout_to_parent_before_exit(self):
        import multiprocessing
        report = multiprocessing.get_context('spawn').RawArray('c', 4096)
        error = RuntimeError('SQL timeout')
        error.errno = 1969
        with mock.patch.object(app, 'detailed_scan', side_effect=error), \
             contextlib.redirect_stderr(io.StringIO()), self.assertRaises(RuntimeError):
            app.server_operations._child_entry('detailed', '/root', 17, error_report=report)
        self.assertIn(b'1969', report.value)

    def test_parent_preserves_child_timeout_reason(self):
        import server_operations as module
        operation = app.server_operations(SimpleNamespace(mode='server'))
        context = mock.Mock()
        context.RawArray.return_value.value = b'MariaDB max_statement_time (1969)'
        context.Process.return_value.is_alive.return_value = False
        context.Process.return_value.exitcode = 1
        with mock.patch.object(module.multiprocessing, 'get_context', return_value=context), \
             mock.patch.object(app, '_shutdown', False):
            self.assertEqual(operation.run_spawn('detailed', '/root', 17), 'error')
        self.assertEqual(operation.worker_error, 'MariaDB max_statement_time (1969)')


class TestPollingConnectionFailure(unittest.TestCase):
    def test_connect_error_is_reported_as_network_failure_without_marking_a_job(self):
        operation = app.server_operations(SimpleNamespace(mode='server'))
        operation.database = mock.Mock(host='synol', port=5799)
        operation.database.database = 'filewalker'
        operation.database.list_live_excluded_absdirs.return_value = []
        operation.database.get_recent_runids.return_value = set()
        operation._logger = mock.Mock()
        error = app.Error(msg='Cannot connect', errno=2003)
        def unavailable(*args):
            app._shutdown = True
            raise error
        output = io.StringIO()
        with mock.patch.object(app, '_shutdown', False), \
             mock.patch.object(app.signal, 'signal'), \
             mock.patch.object(app, 'claim_job', side_effect=unavailable), \
             mock.patch.object(app, 'set_job_status') as status, \
             mock.patch.object(operation, '_configure_syslog'), \
             mock.patch.object(operation, 'log_server_stopped'), \
             contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
            operation._run_server(0)
        self.assertIn('synol:5799', output.getvalue())
        self.assertIn('10 Sekunden', output.getvalue())
        self.assertIn('kein max_statement_time-Abbruch', output.getvalue())
        status.assert_not_called()
        operation._logger.error.assert_called_once()


class TestCoarseRescheduleReporting(unittest.TestCase):
    def test_child_and_parent_transfer_reschedule_request(self):
        import multiprocessing
        import server_operations as module
        report = multiprocessing.get_context('spawn').RawArray('c', 4096)
        with mock.patch.object(app, 'detailed_scan', return_value=24):
            app.server_operations._child_entry('detailed', '/root', 16, error_report=report)
        self.assertEqual(report.value, b'RESCAN_COARSE:24')
        operation = app.server_operations(SimpleNamespace(mode='localserver'))
        context = mock.Mock()
        context.RawArray.return_value = report
        context.Process.return_value.is_alive.return_value = False
        context.Process.return_value.exitcode = 0
        with mock.patch.object(module.multiprocessing, 'get_context', return_value=context), \
                mock.patch.object(app, '_shutdown', False):
            self.assertEqual(operation.run_spawn('detailed', '/root', 16), 'rescan_coarse')
        self.assertIsNone(operation.worker_error)

    def test_reschedule_warning_reaches_console_and_syslog_handlers(self):
        operation = app.server_operations(SimpleNamespace(mode='localserver'))
        with mock.patch.object(app, 'SysLogHandler') as syslog, \
                contextlib.redirect_stdout(io.StringIO()) as output:
            syslog.return_value.level = 30
            operation.log_coarse_rescheduled(16, 1, 12)
        self.assertIn('12 verschwundene', output.getvalue())
        self.assertIn('scanall=1', output.getvalue())
        self.assertTrue(syslog.return_value.handle.called)
        record = syslog.return_value.handle.call_args.args[0]
        self.assertIn('neu eingeplant', record.getMessage())


class NetworkRecoveryTests_server_operations(unittest.TestCase):
    def database(self):
        db = object.__new__(app.DataBaseConnection)
        db.host, db.port, db.database = 'test', 3306, 'test'
        db.user, db.password = 'test', 'test'
        return db

    def test_poll_delay_ends_on_first_tick_after_suspend(self):
        operation = app.server_operations(SimpleNamespace(mode='localserver'))
        operation.database = mock.Mock()
        operation.database.consume_wake_event.return_value = False
        with mock.patch.object(server, '_runtime', return_value=SimpleNamespace(_shutdown=False)), \
                mock.patch.object(server.time, 'time', side_effect=[100, 100, 500]), \
                mock.patch.object(server.time, 'sleep') as sleep:
            operation._wait_for_poll(120)
        sleep.assert_called_once_with(0.5)

    def test_recovery_retries_status_using_fresh_connections_and_resumes(self):
        operation = app.server_operations(SimpleNamespace(mode='localserver'))
        db = self.database()
        operation.database = db
        failed = app.Error(msg='network unavailable', errno=2003)
        good = mock.MagicMock()
        good.__enter__.return_value = good
        with mock.patch.object(db, 'connect', side_effect=[failed, failed, good]) as connect, \
                mock.patch.object(db, '_acquire_runid', return_value=('lock', 99)), \
                mock.patch.object(db, 'write_run_record'), \
                mock.patch.object(server, '_runtime', return_value=SimpleNamespace(_shutdown=False)), \
                mock.patch.object(operation, '_network_warning') as log, \
                mock.patch.object(operation, '_wait_for_poll') as wait:
            good.cursor.return_value.fetchone.return_value = ('/root', 'scan', 'detailed', 80, 1)
            good.cursor.return_value.rowcount = 1
            operation.recover_network_job(16, 1)
        self.assertEqual(connect.call_count, 3)
        self.assertEqual(wait.call_args_list, [mock.call(30), mock.call(30)])
        self.assertIn('erfolgreich', log.call_args.args[0])
        self.assertIn('paused', good.cursor.return_value.execute.call_args_list[1].args[1])

    def test_worker_network_failure_is_reported_for_resume(self):
        report = SimpleNamespace(value=b'')
        with mock.patch.object(server, '_runtime', return_value=app), \
                mock.patch.object(app.database_connection, 'start_wake_monitor'), \
                mock.patch.object(app.database_connection, 'stop_wake_monitor') as stop, \
                mock.patch.object(app, 'detailed_scan', side_effect=app.Error(msg='lost', errno=2055)):
            app.server_operations._child_entry('detailed', '/root', 16, error_report=report)
        self.assertTrue(report.value.startswith(b'DB_NETWORK:'))
        stop.assert_called_once()

    def test_status_check_network_failure_keeps_worker_under_supervision(self):
        operation = app.server_operations(SimpleNamespace(mode='localserver'))
        operation.database = mock.Mock()
        operation.database.get_job_status.side_effect = app.Error(msg='lost', errno=2055)
        context = mock.Mock()
        context.RawArray.return_value = SimpleNamespace(value=b'')
        process = context.Process.return_value
        process.is_alive.side_effect = [True, False]
        process.exitcode = 0
        with mock.patch.object(server, '_runtime', return_value=app), \
                mock.patch.object(app, '_shutdown', False), \
                mock.patch.object(server.multiprocessing, 'get_context', return_value=context), \
                mock.patch.object(operation, '_network_warning') as warning:
            self.assertEqual(operation.run_spawn('detailed', '/root', 16, hostid=1), 'ok')
        warning.assert_called_once()
        process.terminate.assert_not_called()
        self.assertIsNone(app._current_child)

    def test_wake_restarts_worker_only_after_confirmed_exit(self):
        operation = app.server_operations(SimpleNamespace(mode='localserver'))
        context = mock.Mock()
        context.RawArray.return_value = SimpleNamespace(value=b'')
        process = context.Process.return_value
        process.is_alive.side_effect = [True, True, False]
        # Wake arrives after spawn, while the parent waits for its worker.
        process.join.side_effect = lambda **kw: operation._wake_restart.set()
        with mock.patch.object(server, '_runtime', return_value=app), \
                mock.patch.object(app, '_shutdown', False), \
                mock.patch.object(server.multiprocessing, 'get_context', return_value=context), \
                mock.patch.object(operation, '_force_stop_child') as stop, \
                mock.patch.object(operation, '_network_warning'):
            self.assertEqual(operation.run_spawn('coarse', '/root', 18, hostid=1), 'network_retry')
        stop.assert_called_once_with(process)
        self.assertIn(mock.call(timeout=2), process.join.call_args_list)

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

    def test_falling_null_count_keeps_worker_running_across_multiple_reports(self):
        stop = self.run_directory_watchdog(
            [100, 90, 80], [0, 0, 1200, 2400], 'ok', [True, True, True, False])
        stop.assert_not_called()

    def test_unchanged_null_count_restarts_stalled_worker(self):
        stop = self.run_directory_watchdog(
            [100, 100], [0, 0, 1200], 'network_retry', [True, True, True, False])
        stop.assert_called_once()

    def test_increased_null_count_is_not_progress(self):
        stop = self.run_directory_watchdog(
            [100, 101], [0, 0, 1200], 'network_retry', [True, True, True, False])
        stop.assert_called_once()

    def test_failed_count_retries_immediately_and_accepts_progress(self):
        stop = self.run_directory_watchdog(
            [100, app.Error(msg='lost', errno=2055), 90],
            [0, 0, 1200], 'ok', [True, True, False])
        stop.assert_not_called()

    def test_failed_count_query_does_not_kill_worker(self):
        stop = self.run_directory_watchdog(
            [100, app.Error(msg='lost', errno=2055), app.Error(msg='still lost', errno=2055)],
            [0, 0, 1200, 1201], 'ok', [True, True, False])
        stop.assert_not_called()

    def test_recovery_stops_after_600_failures_without_final_sleep(self):
        operation = app.server_operations(SimpleNamespace(mode='localserver'))
        operation.database = mock.Mock()
        operation.database.set_job_status.side_effect = app.Error(msg='offline', errno=2003)
        runtime = SimpleNamespace(_shutdown=False)
        with mock.patch.object(server, '_runtime', return_value=runtime), \
                mock.patch.object(operation, '_network_warning') as warning, \
                mock.patch.object(operation, '_wait_for_poll') as wait:
            operation.recover_network_job(18, 1)
        self.assertEqual(operation.database.set_job_status.call_count, 600)
        self.assertEqual(wait.call_count, 599)
        self.assertTrue(runtime._shutdown)
        self.assertIn('600', warning.call_args.args[0])


class ListenerLifecycleTests(unittest.TestCase):
    def test_all_requests_in_a_listener_process_share_one_runid(self):
        db = mock.Mock()
        db.register_remote_listener_run.return_value = 202
        db.serve_remote_checksum.side_effect = [True, True, KeyboardInterrupt]
        with mock.patch.object(server, '_runtime', return_value=SimpleNamespace(database_connection=db)), \
                mock.patch.object(server.os, 'setsid'), mock.patch.object(server.signal, 'signal'), \
                contextlib.redirect_stdout(io.StringIO()):
            server.server_operations._remote_listener_entry(0)
        db.register_remote_listener_run.assert_called_once_with(0)
        self.assertEqual(db.serve_remote_checksum.call_args_list, [mock.call(0, 202)] * 3)
        db.stop_wake_monitor.assert_called_once()
        db.finish_remote_listener_run.assert_called_once_with(202)

    def test_serverplus_starts_listener_and_reuses_idle_process(self):
        operation = app.server_operations(SimpleNamespace(mode='serverplus'))
        operation._server_hostid = 0
        operation.database = mock.Mock()
        operation.database.remote_binarycheck_runs.side_effect = [[(101, 7, 1)], []]
        operation._logger = mock.Mock()
        context = mock.Mock()
        process = context.Process.return_value
        process.is_alive.return_value = True
        with mock.patch.object(server.multiprocessing, 'get_context', return_value=context), \
                mock.patch.object(server.time, 'monotonic', side_effect=[0, 30]):
            self.assertTrue(operation._service_remote_listener())
            self.assertFalse(operation._service_remote_listener())
        context.Process.assert_called_once()
        process.start.assert_called_once()
        self.assertIs(operation._remote_listener, process)


class PerHostJobTests_server_operations(unittest.TestCase):
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

    def test_active_listener_does_not_block_serverplus_own_job(self):
        operation = app.server_operations(SimpleNamespace(mode='serverplus'))
        operation.database = mock.Mock()
        operation.database.list_live_excluded_absdirs.return_value = []
        operation.database.get_recent_runids.return_value = set()
        job = dict(jobid=5, task='binarycheck', hostid=0)
        def claim(*args):
            app._shutdown = True
            return job
        with mock.patch.object(server, '_runtime', return_value=app), \
                mock.patch.object(app, '_shutdown', False), \
                mock.patch.object(server.signal, 'signal'), \
                mock.patch.object(operation, '_service_remote_listener', return_value=True), \
                mock.patch.object(operation, '_wait_for_poll'), \
                mock.patch.object(operation, 'log_server_stopped'), \
                mock.patch.object(app, 'claim_job', side_effect=claim), \
                mock.patch.object(app, 'process_job') as process:
            operation._run_server(0)
        process.assert_called_once_with(job, 'binarycheck', operation)

    def test_listener_trigger_requires_listening_stage_and_checks_every_thirty_seconds(self):
        operation = app.server_operations(SimpleNamespace(mode='serverplus'))
        operation._server_hostid = 0
        operation.database = mock.Mock()
        operation.database.remote_binarycheck_runs.return_value = []
        with mock.patch.object(server.time, 'monotonic', side_effect=[0, 29, 30]):
            operation._service_remote_listener()
            operation._service_remote_listener()
            operation._service_remote_listener()
        self.assertEqual(operation.database.remote_binarycheck_runs.call_count, 2)
        db = object.__new__(app.DataBaseConnection)
        db.binarycheck_exclusion_matcher = mock.Mock(return_value=lambda path: False)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        with mock.patch.object(db, 'connect', return_value=connection):
            db.remote_binarycheck_runs(0)
        sql = connection.cursor.return_value.execute.call_args.args[0]
        self.assertIn("r.stage = 'listening'", sql)
        self.assertIn('j.runid = r.runid', sql)

    def test_listening_poll_connection_failure_is_retried_without_stopping_server(self):
        operation = app.server_operations(SimpleNamespace(mode='serverplus'))
        operation._server_hostid = 0
        operation.database = mock.Mock()
        operation.database.remote_binarycheck_runs.side_effect = [ConnectionError('offline'), []]
        with mock.patch.object(server.time, 'monotonic', side_effect=[0, 29, 30]), \
                mock.patch.object(operation, '_network_warning') as warning:
            self.assertFalse(operation._service_remote_listener())
            self.assertFalse(operation._service_remote_listener())
            self.assertFalse(operation._service_remote_listener())
        self.assertEqual(operation.database.remote_binarycheck_runs.call_count, 2)
        warning.assert_called_once()


class ScanReportingTests_server_operations(unittest.TestCase):
    def test_detailed_reports_outstanding_percentage_after_twenty_minutes(self):
        operation = app.server_operations(SimpleNamespace(mode='localserver'))
        operation.database = mock.Mock()
        operation.database.get_job_status.return_value = 'active'
        operation.database.count_unfilled_detailed_directories.return_value = 50
        operation.database.get_job_idle_seconds.return_value = 0
        operation.database.get_detailed_directory_progress.return_value = (200, 50)
        operation._logger = mock.Mock()
        context = mock.Mock()
        context.RawArray.return_value = SimpleNamespace(value=b'')
        process = context.Process.return_value
        process.is_alive.side_effect = [True, True, False]
        process.exitcode = 0
        with mock.patch.object(server, '_runtime', return_value=app), \
                mock.patch.object(app, '_shutdown', False), \
                mock.patch.object(server.multiprocessing, 'get_context', return_value=context), \
                mock.patch.object(server.time, 'monotonic', side_effect=[0, 1195, 1200]):
            self.assertEqual(operation.run_spawn('detailed', '/root', 7, hostid=1), 'ok')
        operation.database.get_detailed_directory_progress.assert_called_once_with(7, 1, '/root')
        self.assertIn('50 von 200', operation._logger.info.call_args.args[0])
        self.assertIn('25.00 %', operation._logger.info.call_args.args[0])


class TestPostJobPolling(unittest.TestCase):
    def test_one_ten_second_poll_then_normal_interval_for_every_server_mode(self):
        for mode in ('server','serverplus','localserver'):
            with self.subTest(mode=mode):
                operation=server.server_operations(SimpleNamespace(mode=mode))
                operation.database=mock.Mock()
                operation.database.list_live_excluded_absdirs.return_value=[]
                operation.database.get_recent_runids.return_value=set()
                job=dict(jobid=5,task='scan',stage='coarse')
                runtime=SimpleNamespace(_shutdown=False,_live_excluded_absdirs={},
                    POLL_SECONDS=120,DATABASE_DRIVER='test',LOCALSERVER_LOG_FILE='test.log',
                    Error=app.Error,claim_job=mock.Mock(side_effect=[job,None,None,None]))
                current=[100.0]
                runtime.process_job=mock.Mock(side_effect=lambda *args: current.__setitem__(0,1000.0))
                waits=[]
                def wait(seconds):
                    waits.append(seconds)
                    if len(waits)==2:
                        runtime._shutdown=True
                with mock.patch.object(server,'_runtime',return_value=runtime), \
                        mock.patch.object(server.signal,'signal'), \
                        mock.patch.object(server.time,'monotonic',side_effect=lambda: current[0]), \
                        mock.patch.object(operation,'_service_remote_listener'), \
                        mock.patch.object(operation,'_stop_remote_listener'), \
                        mock.patch.object(operation,'log_server_stopped'), \
                        mock.patch.object(operation,'_wait_for_poll',side_effect=wait), \
                        contextlib.redirect_stdout(io.StringIO()):
                    operation._run_server(7 if mode=='localserver' else 0)
                self.assertEqual(waits,[10,120])
                runtime.process_job.assert_called_once()


if __name__ == '__main__':
    unittest.main()
