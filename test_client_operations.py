"""Tests for client_operations.py."""

from unittest import mock
import DataBaseCore as core
# Importing a test module must not probe the configured production database.
with mock.patch.object(core.DataBaseConnectionCore, 'connect', side_effect=RuntimeError('offline test')):
    import version25 as app
import contextlib
import io
import sys
import unittest
import client_operations as client_module
from unittest.mock import MagicMock
from client_operations import ClientArgumentParser
from DataBaseCLI import DataBaseCLI
from datetime import datetime
from types import SimpleNamespace
from client_operations import client_operations
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from doubles_output import doubles_output
from pathlib import Path
import DataBaseCore
import os
import tempfile
import version25
from DataBaseCore import DataBaseConnectionCore


class TestArgumentParsing(unittest.TestCase):
    def test_check_is_a_subcommand(self):
        self.assertEqual(ClientArgumentParser().parse(['check']).mode, 'check')
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            ClientArgumentParser().parse(['--check'])


    def test_directory_administration_uses_double_dash_only(self):
        for name in ('dirs-summary-data', 'dropdir', 'dropall', 'droptmp'):
            with self.subTest(name=name):
                self.assertEqual(ClientArgumentParser().parse(['--' + name]).mode, name)
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    ClientArgumentParser().parse([name])
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            ClientArgumentParser().parse(['--dropall', '--droptmp'])


    def test_check_modifications_option_and_dispatch(self):
        args = client_module.ClientArgumentParser().parse(['--hostid', '2', '--check-modifications'])
        self.assertEqual(args.mode, 'check-modifications')
        self.assertEqual(args.hostid, 2)
        client = object.__new__(client_module.client_operations)
        client.args = args
        client.database = mock.Mock()
        client.database.enqueue_check_modifications.return_value = [(2, 17)]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(client.run(), 0)
        client.database.enqueue_check_modifications.assert_called_once_with(hostid=2)
        client.database.check_modifications.assert_not_called()


    def test_excluded_dirnames_csv_parser_deduplicates_values(self):
        self.assertEqual(
            client_module.ClientArgumentParser.parse_excluded_dirnames("@eaDir, .git,@eaDir,#recycle"),
            ["@eaDir", ".git", "#recycle"],
        )

    def test_excluded_options_dispatch_as_client_modes(self):
        commands = [
            (["--excluded-dirnames", "@eaDir,.git"], "excluded-dirnames"),
            (["show", "excluded"], "show"),
            (["--erase-excluded"], "erase-excluded"),
        ]
        for argv, expected_mode in commands:
            with self.subTest(mode=expected_mode), \
                 mock.patch.object(sys, "argv", [app.__file__, *argv]), \
                 mock.patch.object(app, "client_operations") as client_class:
                self.assertEqual(app.main(), 0)
                self.assertEqual(
                    client_class.call_args.args[0].mode, expected_mode
                )

    def test_listrunid_command_dispatches(self):
        with mock.patch.object(sys, "argv", [app.__file__, "show", "listrunid"]), \
             mock.patch.object(app, "client_operations") as client_class:
            self.assertEqual(app.main(), 0)
        self.assertEqual(client_class.call_args.args[0].mode, "show")
        self.assertEqual(client_class.call_args.args[0].show_name, "listrunid")
        client_class.return_value.run.assert_called_once_with()

    def test_doubles_options_dispatch_as_client_modes(self):
        commands = [
            ("--doubles-list", "doubles-list"),
            ("--doubles-letcheck", "doubles-check"),
            ("--doubles-list-binary", "doubles-list-binary"),
        ]
        for option, expected_mode in commands:
            with self.subTest(option=option), \
                 mock.patch.object(sys, "argv", [app.__file__, option]), \
                 mock.patch.object(app, "client_operations") as client_class:
                self.assertEqual(app.main(), 0)
                self.assertEqual(client_class.call_args.args[0].mode, expected_mode)
                client_class.return_value.run.assert_called_once_with()

    def test_doubles_progress_option_accepts_optional_jobid(self):
        for option, expected_jobid in (
            (["--doubles-progress"], None),
            (["--doubles-progress", "34"], 34),
        ):
            with self.subTest(option=option), \
                 mock.patch.object(
                     sys, "argv", [app.__file__, *option]
                 ), mock.patch.object(app, "client_operations") as client_class:
                self.assertEqual(app.main(), 0)
                args = client_class.call_args.args[0]
                self.assertEqual(args.mode, "doubles-progress")
                self.assertEqual(args.jobid, expected_jobid)

    def test_init_command_dispatches(self):
        with mock.patch.object(sys, "argv", [app.__file__, "init"]), \
             mock.patch.object(app, "client_operations") as client_class:
            self.assertEqual(app.main(), 0)
        self.assertEqual(client_class.call_args.args[0].mode, "init")
        client_class.return_value.run.assert_called_once_with()


    def test_dropall_command_dispatches(self):
        with mock.patch.object(
            sys, "argv", [app.__file__, "--dropall"]
        ), mock.patch.object(app, "client_operations") as client_class:
            self.assertEqual(app.main(), 0)
        args = client_class.call_args.args[0]
        self.assertEqual(args.mode, "dropall")
        client_class.return_value.run.assert_called_once_with()

    def test_check_command_returns_failure_exit_code(self):
        with mock.patch.object(
            sys, "argv", [app.__file__, "check"]
        ), mock.patch.object(app, "client_operations") as client_class:
            client_class.return_value.run.return_value = 1
            self.assertEqual(app.main(), 1)
        self.assertEqual(client_class.call_args.args[0].mode, "check")

    def test_renamed_job_commands_parse(self):
        commands = [
            (["--jobs-insert", "/data"], "jobs-insert"),
            (["--jobs-insert-scanall", "/data"], "jobs-insert-scanall"),
            (["show", "jobs"], "show"),
            (["--jobs-list-active"], "jobs-list-active"),
            (["--jobs-stop", "5"], "jobs-stop"),
            (["--jobs-reset-on-error"], "jobs-reset-on-error"),
        ]
        for argv, expected_mode in commands:
            with self.subTest(mode=expected_mode), \
                 mock.patch.object(sys, "argv", [app.__file__, *argv]), \
                 mock.patch.object(app, "client_operations") as client_class:
                self.assertEqual(app.main(), 0)
                self.assertEqual(client_class.call_args.args[0].mode, expected_mode)

    def test_jobid_arguments_must_be_positive(self):
        with mock.patch.object(
            sys, "argv", [app.__file__, "--jobs-stop", "0"]
        ), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                app.main()
        self.assertEqual(raised.exception.code, 2)

    def test_absolute_option_dispatch_and_validation(self):
        with mock.patch.object(sys, "argv", [app.__file__, "--excluded-absolute", "1", "/srv/private"]), \
             mock.patch.object(app, "client_operations") as client:
            self.assertEqual(app.main(), 0)
            args = client.call_args.args[0]
            self.assertEqual(args.mode, "excluded-absolute")
            self.assertEqual(args.excluded_absolute, (1, "/srv/private"))
        for values in (("bad", "/srv/private"), ("-1", "/srv/private"),
                       ("1", "relative"), ("1", "/srv/a,b"), ("1",)):
            with self.subTest(values=values), \
                 mock.patch.object(sys, "argv", [app.__file__, "--excluded-absolute", *values]), \
                 contextlib.redirect_stderr(io.StringIO()), \
                 self.assertRaises(SystemExit) as raised:
                app.main()
            self.assertEqual(raised.exception.code, 2)

    def test_option_works_before_and_after_localserver(self):
        for options in (["localserver", "--hostid", "1", "--coarse-glob"], ["--coarse-glob", "localserver", "--hostid", "1"]):
            with mock.patch.object(sys, "argv", [app.__file__, *options]), \
                 mock.patch.object(app, "server_operations") as server:
                self.assertEqual(app.main(), 0)
            self.assertTrue(server.call_args.args[0].coarse_glob)
        with mock.patch.object(sys, "argv", [app.__file__, "server", "--coarse-glob"]), \
             contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            app.main()

    def test_host_commands_dispatch_and_local_id_is_required(self):
        for arguments, mode in ((["--setmyhostid"], "setmyhostid"), (["serverinit"], "serverinit"), (["show", "hosts"], "show")):
            with self.subTest(arguments=arguments), mock.patch.object(sys, "argv", [app.__file__, *arguments]), \
                 mock.patch.object(app, "client_operations") as client:
                self.assertEqual(app.main(), 0)
                self.assertEqual(client.call_args.args[0].mode, mode)
        with mock.patch.object(sys, "argv", [app.__file__, "localserver", "--hostid", "7", "--coarse-glob"]), \
             mock.patch.object(app, "server_operations") as server:
            self.assertEqual(app.main(), 0)
        self.assertEqual(server.call_args.args[0].local_hostid, 7)
        for arguments in (["localserver"], ["localserver", "0"], ["localserver", "-1"], ["localserver", "32768"]):
            with self.subTest(arguments=arguments), mock.patch.object(sys, "argv", [app.__file__, *arguments]), \
                 contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                app.main()
            self.assertEqual(raised.exception.code, 2)

    def test_server_commands_do_not_require_a_local_id(self):
        for mode in ("server", "serverplus", "serverinit", "--droptmp", "--dropall"):
            with self.subTest(mode=mode), mock.patch.object(sys, "argv", [app.__file__, mode]), \
                 mock.patch.object(app, "client_operations"), mock.patch.object(app, "server_operations"):
                self.assertEqual(app.main(), 0)

    def test_all_hyphenated_subcommands_accept_double_dash(self):
        arguments = {
            "drop-directories": [],
            "jobs-insert": ["/tmp/data"],
            "jobs-insert-hostid": ["1", "/tmp/data"], "jobs-insert-scanall": ["/tmp/data"],
        }
        for command, values in arguments.items():
            for prefix in ("--",):
                with self.subTest(command=command, prefix=prefix), \
                     mock.patch.object(sys, "argv", [app.__file__, prefix + command, *values]), \
                     mock.patch.object(app, "client_operations") as client:
                    self.assertEqual(app.main(), 0)
                    self.assertEqual(client.call_args.args[0].mode, command)

    def test_global_options_before_prefixed_command_keep_their_values(self):
        with mock.patch.object(sys, "argv", [app.__file__, "--hostid", "7", "--jobs-insert", "/tmp/data"]), \
             mock.patch.object(app, "client_operations") as client:
            self.assertEqual(app.main(), 0)
        args = client.call_args.args[0]
        self.assertEqual(args.hostid, 7)
        self.assertEqual(args.path, "/tmp/data")
        self.assertEqual(args.mode, "jobs-insert")

    def test_existing_option_value_is_not_reinterpreted_as_command(self):
        with mock.patch.object(sys, "argv", [app.__file__, "--excluded-dirnames=--jobs-list"]), \
             mock.patch.object(app, "client_operations") as client:
            self.assertEqual(app.main(), 0)
        self.assertEqual(client.call_args.args[0].mode, "excluded-dirnames")
        self.assertEqual(client.call_args.args[0].excluded_dirnames, "--jobs-list")


    def test_client_parser_preserves_csv_validation(self):
        parse = client_module.ClientArgumentParser.parse_excluded_dirnames
        self.assertEqual(parse("cache, .git,cache"), ["cache", ".git"])
        for value in ("", " , ", '\"a,b\"'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse(value)


class DeepDoubleTests(unittest.TestCase):
    def test_parser_modes(self):
        for option in ('--doubles-letcheck',):
            self.assertEqual(ClientArgumentParser().parse([option]).mode, 'doubles-check')
        for option in ('--doubles-listlevel', '--doubles-listlevels'):
            self.assertEqual(ClientArgumentParser().parse([option]).mode, 'doubles-listlevel')
        self.assertEqual(ClientArgumentParser().parse(['--doubles-list']).mode, 'doubles-list')

    def test_conflicting_modes_rejected(self):
        for args in (['--doubles-letcheck', '--doubles-listlevel'],
                     ['--doubles-listlevel', 'server']):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                ClientArgumentParser().parse(args)

    def database(self, states):
        db = DataBaseCLI(MagicMock())
        connection = db.connection.connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value
        cursor.fetchall.side_effect = [states, [], [(1, 0, 0, 'a', '/a'), (2, 0, 0, 'a', '/b')]]
        return db, connection, cursor


class TestDoubles(unittest.TestCase):
    def test_check_doubles_queues_a_binarycheck_job(self):
        operation = app.client_operations(SimpleNamespace(mode="doubles-check"))
        with mock.patch.object(
            operation.database, "create_binarycheck_job", return_value=(82, 6)
        ) as create, contextlib.redirect_stdout(io.StringIO()) as output:
            operation.run()
        create.assert_called_once_with(confirm_repeat=operation._confirm_binarycheck_repeat)
        self.assertIn("Binarycheck-Job 82 mit 6 Verzeichnisvergleichen", output.getvalue())


class TreeHtmlTests(unittest.TestCase):
    def test_parse_optional_jobid(self):
        for argv, expected in ((['--doubles-tree-html'], None),
                               (['--doubles-tree-html', '34'], 34)):
            args = ClientArgumentParser().parse(argv)
            self.assertEqual(args.mode, 'doubles-tree-html')
            self.assertEqual(args.jobid, expected)

    def test_invalid_combinations(self):
        for argv in (['--doubles-tree-html', '-1'],
                     ['--doubles-tree-html', '--doubles-progress-html'],
                     ['--doubles-tree-html', 'server']):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                ClientArgumentParser().parse(argv)


class TestClientOperations(unittest.TestCase):
    def test_jobs_insert_hostid_dispatches_hostid_one(self):
        operation = app.client_operations(SimpleNamespace(
            mode="jobs-insert-hostid", hostid=1, path="/local/root", skipdirs=""
        ))
        with mock.patch.object(operation, "include_excluded_names", return_value="cache"), \
             mock.patch.object(app.DataBaseCLI, "insert_job") as insert:
            operation.run()
        insert.assert_called_once_with("/local/root", "cache", hostid=1)

    def test_listrunid_dispatches_fifteen_day_runid_listing(self):
        operation = app.client_operations(SimpleNamespace(mode="show", show_name="listrunid"))
        with mock.patch.object(
            app.DataBaseCLI, "list_recent_jobs",
            return_value=[{"runid": 12, "jobid": 1}, {"runid": 13, "jobid": 2}],
        ) as list_jobs, contextlib.redirect_stdout(io.StringIO()) as output:
            operation.run()
        list_jobs.assert_called_once_with(days=15)
        self.assertIn("12", output.getvalue())
        self.assertIn("13", output.getvalue())

    def test_init_dispatches_to_strict_initialization(self):
        operation = app.client_operations(SimpleNamespace(mode="init"))
        with mock.patch.object(app.DataBaseCLI, 'initialize', return_value=0) as init:
            self.assertEqual(operation.run(), 0)
        init.assert_called_once_with()

    def test_init_jobs_dispatches_through_global_connection(self):
        args = SimpleNamespace(mode="init-jobs")
        # Isolate this historical runtime when version25 is also collected.
        with mock.patch.object(client_module, "_runtime", return_value=app):
            operation = app.client_operations(args)
        self.assertIs(operation.database.connection, app.database_connection)
        with mock.patch.object(app.DataBaseCLI, "init_database") as initialize:
            operation.run()
        initialize.assert_called_once_with()

    def test_delete_jobs_dispatches_to_delete_all_jobs(self):
        operation = app.client_operations(SimpleNamespace(mode="jobs-deleteall"))
        with mock.patch.object(app.DataBaseCLI, "delete_all_jobs") as delete:
            operation.run()
        delete.assert_called_once_with()

    def test_dropall_dispatches_to_database_drop(self):
        operation = app.client_operations(SimpleNamespace(mode="dropall"))
        with mock.patch.object(
            app.DataBaseCLI, "drop_all_tables"
        ) as drop_all:
            operation.run()
        drop_all.assert_called_once_with()

    def test_check_returns_schema_status(self):
        operation = app.client_operations(SimpleNamespace(mode="check"))
        with mock.patch.object(
            app.DataBaseCLI, "check_schema", return_value=False
        ) as check:
            self.assertEqual(operation.run(), 1)
        check.assert_called_once_with()

    def test_jobs_stop_sets_stopped_status(self):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value
        cursor.rowcount = 1
        database = app.database_connection
        output = io.StringIO()
        with mock.patch.object(database, "connect") as connect, \
             contextlib.redirect_stdout(output):
            connect.return_value.__enter__.return_value = connection
            app.ApplicationContext.cli_database().change_job_status(5, "stop")
        update_sql = cursor.execute.call_args.args[0]
        update_calls = [call.args for call in cursor.execute.call_args_list]
        update_call = next(call for call in update_calls if "UPDATE" in call[0])
        self.assertIn("status = 'stopped'", update_call[0])
        self.assertIn("'insert', 'pending', 'active', 'paused'", update_call[0])
        self.assertEqual(update_call[1][1], 5)
        self.assertIn("Job 5 wurde gestoppt.", output.getvalue())

    def test_renamed_job_modes_dispatch(self):
        with mock.patch.object(app.DataBaseCLI, "insert_job") as insert, \
             mock.patch.object(
                 app.DataBaseCLI, "list_excluded_names", return_value=[]
             ), \
             mock.patch.object(app.DataBaseCLI, "list_jobs") as list_jobs, \
             mock.patch.object(app.DataBaseCLI, "change_job_status") as change_status:
            app.client_operations(
                SimpleNamespace(mode="jobs-insert", path="/data", skipdirs="cache,tmp")
            ).run()
            app.client_operations(SimpleNamespace(mode="show", show_name="jobs")).run()
            app.client_operations(SimpleNamespace(mode="jobs-list-active")).run()
            app.client_operations(
                SimpleNamespace(mode="jobs-stop", jobid=5)
            ).run()
        insert.assert_called_once_with("/data", "cache,tmp")
        self.assertEqual(list_jobs.call_args_list, [
            mock.call(active_only=False), mock.call(active_only=True)
        ])
        change_status.assert_called_once_with(5, "stop")

    def test_jobs_insert_scanall_sets_flag(self):
        with mock.patch.object(app.DataBaseCLI, "insert_job") as insert, \
             mock.patch.object(
                 app.DataBaseCLI, "list_excluded_names", return_value=[]
             ):
            app.client_operations(SimpleNamespace(
                mode="jobs-insert-scanall", path="/data", skipdirs="cache"
            )).run()
        insert.assert_called_once_with("/data", "cache", scanall=1)

    def test_insert_can_copy_shared_exclusions_into_job_skipdirs(self):
        operation = app.client_operations(SimpleNamespace(
            mode="jobs-insert", path="/data", skipdirs=""
        ))
        with mock.patch.object(
            operation.database, "list_excluded_names", return_value=["@eaDir", ".git"]
        ), mock.patch("builtins.input", return_value="y"), \
             mock.patch.object(app.DataBaseCLI, "insert_job") as insert:
            operation.run()
        insert.assert_called_once_with("/data", "@eaDir,.git")

    def test_insert_can_reject_shared_exclusions(self):
        operation = app.client_operations(SimpleNamespace(
            mode="jobs-insert", path="/data", skipdirs="cache"
        ))
        with mock.patch.object(
            operation.database, "list_excluded_names", return_value=["@eaDir"]
        ), mock.patch("builtins.input", return_value="n"), \
             mock.patch.object(app.DataBaseCLI, "insert_job") as insert:
            operation.run()
        insert.assert_called_once_with("/data", "cache")

    def test_explicit_insert_exclusions_bypass_shared_list_and_prompt(self):
        operation = app.client_operations(SimpleNamespace(
            mode="jobs-insert", path="/data", skipdirs="cache,tmp"
        ))
        with mock.patch.object(
            operation.database, "list_excluded_names"
        ) as list_names, mock.patch(
            "builtins.input", side_effect=AssertionError("should not prompt")
        ), mock.patch.object(app.DataBaseCLI, "insert_job") as insert:
            operation.run()
        list_names.assert_not_called()
        insert.assert_called_once_with("/data", "cache,tmp")

    def test_dropdir_selects_root_and_confirms_with_entry_count(self):
        operation = app.client_operations(
            SimpleNamespace(mode="dropdir", yes=False)
        )
        output = io.StringIO()
        with mock.patch.object(
            operation.database,
            "list_directory_roots",
            return_value=[("/data", 4), ("/archive", 8)],
        ), mock.patch.object(
            operation.database, "delete_directory_root", return_value=8
        ) as delete, mock.patch(
            "builtins.input", side_effect=["2", "y"]
        ) as prompt, contextlib.redirect_stdout(output):
            operation.run()
        self.assertEqual(prompt.call_count, 2)
        delete.assert_called_once_with("/archive")
        self.assertIn("2) /archive (8 Einträge)", output.getvalue())
        self.assertIn("Noch vorhandene Einträge in directories: 8", output.getvalue())

    def test_dropdir_y_skips_confirmation_but_still_selects_root(self):
        operation = app.client_operations(
            SimpleNamespace(mode="dropdir", yes=True)
        )
        with mock.patch.object(
            operation.database, "list_directory_roots", return_value=[("/data", 4)]
        ), mock.patch.object(
            operation.database, "delete_directory_root", return_value=4
        ) as delete, mock.patch("builtins.input", return_value="1") as prompt, \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        prompt.assert_called_once()
        delete.assert_called_once_with("/data")

    def test_dropdir_cancel_leaves_rows_untouched(self):
        operation = app.client_operations(
            SimpleNamespace(mode="dropdir", yes=False)
        )
        with mock.patch.object(
            operation.database, "list_directory_roots", return_value=[("/data", 4)]
        ), mock.patch.object(
            operation.database, "delete_directory_root"
        ) as delete, mock.patch("builtins.input", side_effect=["1", "n"]), \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        delete.assert_not_called()

    def test_delete_job_confirms_then_deletes(self):
        operation = app.client_operations(
            SimpleNamespace(mode="jobs-deleteid", jobid=27, yes=False)
        )
        job = {
            "jobid": 27, "task": "scan", "status": "closed",
            "startdirname": "/data", "userhost": "nas", "timecreated": "now",
        }
        with mock.patch.object(operation.database, "get_job", return_value=job), \
             mock.patch.object(operation.database, "delete_job", return_value=True) as delete, \
             mock.patch("builtins.input", return_value="y") as prompt, \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        prompt.assert_called_once()
        delete.assert_called_once_with(27)

    def test_delete_job_can_be_cancelled(self):
        operation = app.client_operations(
            SimpleNamespace(mode="jobs-deleteid", jobid=27, yes=False)
        )
        job = {
            "jobid": 27, "task": "scan", "status": "closed",
            "startdirname": "/data", "userhost": "nas", "timecreated": "now",
        }
        with mock.patch.object(operation.database, "get_job", return_value=job), \
             mock.patch.object(operation.database, "delete_job") as delete, \
             mock.patch("builtins.input", return_value="n"), \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        delete.assert_not_called()

    def test_delete_job_yes_option_skips_prompt(self):
        operation = app.client_operations(
            SimpleNamespace(mode="jobs-deleteid", jobid=27, yes=True)
        )
        job = {
            "jobid": 27, "task": "scan", "status": "closed",
            "startdirname": "/data", "userhost": "nas", "timecreated": "now",
        }
        with mock.patch.object(operation.database, "get_job", return_value=job), \
             mock.patch.object(operation.database, "delete_job", return_value=True) as delete, \
             mock.patch("builtins.input", side_effect=AssertionError("Unexpected confirmation prompt")) as prompt, \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        prompt.assert_not_called()
        delete.assert_called_once_with(27)

    def test_delete_job_missing_id_does_not_prompt_or_delete(self):
        operation = app.client_operations(
            SimpleNamespace(mode="jobs-deleteid", jobid=27, yes=False)
        )
        with mock.patch.object(operation.database, "get_job", return_value=None), \
             mock.patch.object(operation.database, "delete_job") as delete, \
             mock.patch("builtins.input", side_effect=AssertionError("Unexpected confirmation prompt")) as prompt, \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        prompt.assert_not_called()
        delete.assert_not_called()

    def test_reset_after_error_lists_error_jobs_and_resets_selected_id(self):
        operation = app.client_operations(SimpleNamespace(mode="jobs-reset-on-error"))
        jobs = [{"jobid": 42}, {"jobid": 43}]
        with mock.patch.object(app.DataBaseCLI, "list_jobs", return_value=jobs) as list_error, \
             mock.patch.object(app.DataBaseCLI, "reset_error_job", return_value=True) as reset, \
             mock.patch("builtins.input", return_value="43"), \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        list_error.assert_called_once_with(status="error", reset_error_display=True)
        reset.assert_called_once_with(43)

    def test_reset_after_error_rejects_id_not_in_error_list(self):
        operation = app.client_operations(SimpleNamespace(mode="jobs-reset-on-error"))
        with mock.patch.object(app.DataBaseCLI, "list_jobs", return_value=[{"jobid": 42}]), \
             mock.patch.object(app.DataBaseCLI, "reset_error_job") as reset, \
             mock.patch("builtins.input", return_value="99"), \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        reset.assert_not_called()


class TestClientExclusions(unittest.TestCase):
    def test_directory_names_use_only_original_table(self):
        operation = app.client_operations(SimpleNamespace(
            mode="excluded-dirnames", excluded_dirnames=".git,cache"))
        operation.database = mock.Mock()
        with contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        operation.database.add_excluded_names.assert_called_once_with([".git", "cache"])
        operation.database.add_live_excluded_absdirs.assert_not_called()

    def test_absolute_option_writes_only_host_specific_table(self):
        operation = app.client_operations(SimpleNamespace(
            mode="excluded-absolute", excluded_absolute=(1, "/srv/private")))
        operation.database = mock.Mock()
        with contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        operation.database.add_live_excluded_absdirs.assert_called_once_with(["/srv/private"], 1)
        operation.database.add_excluded_names.assert_not_called()


class TestClientHostOperations(unittest.TestCase):
    def test_show_hosts_prints_registry(self):
        operation = app.client_operations(SimpleNamespace(mode="show", show_name="hosts"))
        operation.database = mock.Mock()
        operation.database.list_hosts.return_value = [(7, "alex", "/home/alex", None, "Linux", "pc", datetime(2026, 10, 28))]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            operation.run()
        self.assertIn("7 | alex | /home/alex | NULL | Linux | pc", output.getvalue())

    def test_jobs_insert_can_use_registered_host_id(self):
        operation = app.client_operations(SimpleNamespace(mode="jobs-insert", hostid=7, path="/home/alex", skipdirs="cache"))
        with mock.patch.object(app.DataBaseCLI, "insert_job") as insert:
            operation.run()
        insert.assert_called_once_with("/home/alex", "cache", hostid=7)


class TestShowMode(unittest.TestCase):
    def test_parses_names_and_retains_host_selection(self):
        for name in (None, 'hosts', 'jobs', 'jobs-list', 'excluded', 'listrunid', 'dirs-listrunid', 'directories'):
            argv = ['show'] + ([name] if name else [])
            args = client_module.ClientArgumentParser().parse(argv)
            self.assertEqual(args.mode, 'show')
            self.assertEqual(args.show_name, name)
        for argv in (['--hostid', '7', 'show', 'excluded'], ['show', 'excluded', '--hostid', '7']):
            self.assertEqual(client_module.ClientArgumentParser().parse(argv).hostid, 7)

    def test_old_arguments_are_removed(self):
        for arg in ('dirs-listrunid', '--dirs-listrunid', 'jobs-list', '--jobs-list',
                    '--show-hosts', '--show-excluded', '--listrunid'):
            with self.subTest(arg=arg):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    client_module.ClientArgumentParser().parse([arg])
                self.assertEqual(error.exception.code, 2)

    def operation(self, name=None):
        args = client_module.ClientArgumentParser().parse(['show'] + ([name] if name else []))
        operation = app.client_operations(args)
        operation.database = mock.Mock()
        return operation

    def test_no_name_lists_tables_without_heading(self):
        operation = self.operation()
        operation.database.list_tables.return_value = ['directories', 'jobs', 'tmp_doubles_letchecked']
        with contextlib.redirect_stdout(io.StringIO()) as output:
            operation.run()
        self.assertEqual(output.getvalue().splitlines(), ['directories', 'jobs', 'tmp_doubles_letchecked'])

    def test_raw_preview_has_no_header_and_gives_full_count(self):
        operation = self.operation('directories')
        operation.database.table_preview.return_value = ([(n, '/data/'+str(n)) for n in range(40)], 100)
        with contextlib.redirect_stdout(io.StringIO()) as output:
            operation.run()
        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 41)
        self.assertEqual(lines[0], '0 | /data/0')
        self.assertEqual(lines[-2], '39 | /data/39')
        self.assertEqual(lines[-1], 'Datensätze insgesamt: 100')
        operation.database.table_preview.assert_called_once_with('directories')

    def test_empty_raw_table_prints_only_total(self):
        operation = self.operation('empty')
        operation.database.table_preview.return_value = ([], 0)
        with contextlib.redirect_stdout(io.StringIO()) as output:
            operation.run()
        self.assertEqual(output.getvalue(), 'Datensätze insgesamt: 0\n')

    def test_embedded_newlines_do_not_create_extra_record_lines(self):
        operation = self.operation('notes')
        operation.database.table_preview.return_value = ([(1, 'first\nsecond', None)], 1)
        with contextlib.redirect_stdout(io.StringIO()) as output:
            operation.run()
        self.assertEqual(output.getvalue().splitlines(), ['1 | first\\nsecond | NULL', 'Datensätze insgesamt: 1'])

    def test_formatted_directory_runids_and_exclusions_are_dispatched(self):
        for name, method, args in (('dirs-listrunid', 'list_directory_runids', ()),
                                   ('excluded', 'show_excluded_names', (0,))):
            operation = self.operation(name)
            with mock.patch.object(operation, method) as display:
                operation.run()
            display.assert_called_once_with(*args)


class TestInitDirectoriesOption(unittest.TestCase):
    def test_new_option_dispatches(self):
        for name in ('--drop-directories', '--init-alldirs'):
            args = client_module.ClientArgumentParser().parse([name])
            self.assertEqual(args.mode, 'drop-directories')
            operation = app.client_operations(args)
            with mock.patch.object(operation.database, 'init_alldirs') as initialize:
                operation.run()
            initialize.assert_called_once_with()

    def test_old_option_is_removed(self):
        for name in ('init-alldirs',):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                client_module.ClientArgumentParser().parse([name])


class TestInitJobsFlagOnly(unittest.TestCase):
    def test_only_prefixed_init_jobs_is_accepted(self):
        args = client_module.ClientArgumentParser().parse(['--init-jobs'])
        self.assertEqual(args.mode, 'init-jobs')
        for name in ('init-jobs', 'init-alldirs'):
            with self.subTest(name=name), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                client_module.ClientArgumentParser().parse([name])

    def test_init_jobs_cannot_be_combined_with_show(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            client_module.ClientArgumentParser().parse(['--init-jobs', 'show'])


class TestCurrentAdministrativeOptions(unittest.TestCase):
    def test_flag_only_commands_accept_current_names_and_reject_bare_names(self):
        options = [
            ('jobs-start', ['42'], 'jobs-start', 'jobid', 42),
            ('jobs-stop', ['42'], 'jobs-stop', 'jobid', 42),
            ('jobs-deleteid', ['42', '-Y'], 'jobs-deleteid', 'jobid', 42),
            ('jobs-deleteall', [], 'jobs-deleteall', None, None),
            ('jobs-list-active', [], 'jobs-list-active', None, None),
            ('jobs-reset-on-error', [], 'jobs-reset-on-error', None, None),
            ('dirs-droprunid', ['0', '-Y'], 'dirs-droprunid', 'runid', 0),
        ]
        for name, values, mode, field, expected in options:
            with self.subTest(name=name):
                args = ClientArgumentParser().parse(['--'+name, *values])
                self.assertEqual(args.mode, mode)
                if field:
                    self.assertEqual(getattr(args, field), expected)
                if '-Y' in values:
                    self.assertTrue(args.yes)
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    ClientArgumentParser().parse([name, *values])

    def test_retired_names_and_abbreviations_are_rejected(self):
        for argv in (['jobs-active'], ['--jobs-active'], ['jobs-reseterror'],
                     ['--jobs-reseterror'], ['--jobs-list'], ['d', '42'], ['-d', '42']):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    ClientArgumentParser().parse(argv)

    def test_yes_is_preserved_before_and_after_dropdir(self):
        for argv in (['-Y', '--dropdir'], ['--dropdir', '-Y']):
            with self.subTest(argv=argv):
                self.assertTrue(ClientArgumentParser().parse(argv).yes)


class TestRenamedDirectoryReset(unittest.TestCase):
    def test_drop_directories_dispatches_existing_confirmation_flow(self):
        args = ClientArgumentParser().parse(['--drop-directories'])
        self.assertEqual(args.mode, 'drop-directories')
        operation = client_module.client_operations(args)
        with mock.patch.object(operation.database, 'init_alldirs') as initialize:
            operation.run()
        initialize.assert_called_once_with()

    def test_old_command_is_replaced_but_init_alldirs_alias_is_preserved(self):
        with contextlib.redirect_stderr(io.StringIO()):
            for command in ('init-directories', '--init-directories'):
                with self.subTest(command=command), self.assertRaises(SystemExit):
                    ClientArgumentParser().parse([command])
        self.assertEqual(ClientArgumentParser().parse(['--init-alldirs']).mode, 'drop-directories')

    def test_priority_command_accepts_host_then_multiple_directory_ids(self):
        args = ClientArgumentParser().parse(['--insert-priority', '7', '123', '456', '789'])
        self.assertEqual((args.mode, args.priority_hostid, args.priority_dirids),
                         ('insert-priority', 7, [123, 456, 789]))
        operation = client_module.client_operations(args)
        with mock.patch.object(operation.database, 'insert_priority', return_value=3) as insert, \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        insert.assert_called_once_with(7, [123, 456, 789])


class TestDoublesWorkflowHelp(unittest.TestCase):
    def test_help_prints_schedule_server_progress_and_html_in_order(self):
        output = io.StringIO()
        with mock.patch.object(sys, 'argv', ['dirwalker.py']), \
             contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as exit_:
            ClientArgumentParser().parse(['--doubles-help'])
        self.assertEqual(exit_.exception.code, 0)
        text = output.getvalue()
        self.assertTrue(text.startswith('Hilfe zum Vorgehen: Von --doubles-letcheck'))
        self.assertLessEqual(max(map(len, text.splitlines())), 72)
        commands = ['python3 dirwalker.py --doubles-letcheck',
                    'python3 dirwalker.py --doubles-progress JOBID',
                    'python3 dirwalker.py --doubles-tree-html JOBID']
        positions = [text.index(command) for command in commands]
        self.assertEqual(positions, sorted(positions))
        for phrase in ('closed',
                       'doubles-tree-JOBID.html', 'KEIN Pflichtschritt', 'tmp_deepdoubles_result'):
            self.assertIn(phrase, text)

    def test_help_does_not_dispatch_client_or_server_or_access_database(self):
        with mock.patch.object(sys, 'argv', [app.__file__, '--doubles-help']), \
             mock.patch.object(app, 'client_operations') as client, \
             mock.patch.object(app, 'server_operations') as server, \
             mock.patch.object(app.database_connection, 'connect') as connect, \
             contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as exit_:
            app.main()
        self.assertEqual(exit_.exception.code, 0)
        client.assert_not_called()
        server.assert_not_called()
        connect.assert_not_called()

    def test_general_help_lists_doubles_help(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(SystemExit):
            ClientArgumentParser().parse(['--help'])
        self.assertIn('--doubles-help', output.getvalue())


class TestRemovedDoublesCheckAlias(unittest.TestCase):
    def test_removed_alias_is_rejected_and_not_advertised(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exit_:
            ClientArgumentParser().parse(['--doubles-check'])
        self.assertEqual(exit_.exception.code, 2)
        for flag in ('--help', '--doubles-help'):
            output = io.StringIO()
            with contextlib.redirect_stdout(output), self.assertRaises(SystemExit):
                ClientArgumentParser().parse([flag])
            self.assertNotIn('--doubles-check', output.getvalue())
            self.assertIn('--doubles-letcheck', output.getvalue())


class TestOnlyPrefixedAdministrativeCommands(unittest.TestCase):
    def test_bare_spellings_are_rejected_and_prefixed_commands_keep_arguments(self):
        commands = {
            'drop-directories': [],
            'jobs-insert': ['/tmp/data', 'cache,tmp'],
            'jobs-insert-scanall': ['/tmp/data'],
            'jobs-insert-hostid': ['1', '/tmp/data'],
            'insert-priority': ['7', '123', '456'],
        }
        for command, values in commands.items():
            with self.subTest(command=command):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    ClientArgumentParser().parse([command, *values])
                args = ClientArgumentParser().parse(['--' + command, *values])
                self.assertEqual(args.mode, command)
                if command.startswith('jobs-insert'):
                    self.assertEqual(args.path, '/tmp/data')
                if command == 'insert-priority':
                    self.assertEqual((args.priority_hostid, args.priority_dirids), (7, [123, 456]))

    def test_global_options_and_post_command_confirmation_flag(self):
        args = ClientArgumentParser().parse(['--hostid', '7', '--jobs-insert', '/tmp/data'])
        self.assertEqual((args.hostid, args.path), (7, '/tmp/data'))


class TestAlphabeticalOptionalHelp(unittest.TestCase):
    def test_optional_argument_entries_are_alphabetical(self):
        import re
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(SystemExit):
            ClientArgumentParser().parse(['--help'])
        text = output.getvalue()
        section = text.split('optional arguments:', 1)[-1].split('options:', 1)[-1]
        entries = [line for line in section.splitlines() if re.match(r'^  -\S', line)]
        names = []
        for entry in entries:
            option_text = re.split(r'\s{2,}', entry.strip(), maxsplit=1)[0]
            options = re.findall(r'(?<!\S)(--?[A-Za-z][A-Za-z-]*)', option_text)
            names.append(next((name for name in options if name.startswith('--')), options[0]).lstrip('-').casefold())
        self.assertGreater(len(names), 20)
        self.assertEqual(names, sorted(names))


class TestAdministrativeOptionsInsteadOfSubcommands(unittest.TestCase):
    def test_moved_options_appear_only_in_optional_argument_section(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(SystemExit):
            ClientArgumentParser().parse(['--help'])
        text = output.getvalue()
        positional, optional = text.split('positional arguments:', 1)[1].split('optional arguments:', 1)
        for option in ('--drop-directories', '--insert-priority', '--jobs-insert',
                       '--jobs-insert-hostid', '--jobs-insert-scanall'):
            self.assertNotIn(option, positional)
            self.assertIn(option, optional)

    def test_global_hostid_can_follow_scan_path_and_exclusions(self):
        args = ClientArgumentParser().parse(['--jobs-insert', '/tmp/data', 'cache,tmp', '--hostid', '7'])
        self.assertEqual((args.mode, args.path, args.skipdirs, args.hostid),
                         ('jobs-insert', '/tmp/data', 'cache,tmp', 7))

    def test_conflicting_modes_and_invalid_value_counts_are_rejected(self):
        invalid = [
            ['--drop-directories', '--jobs-list-active'],
            ['--drop-directories', 'serverplus'],
            ['--jobs-insert', '/tmp/data', '--jobs-stop', '1'],
            ['--insert-priority', '7'],
            ['--insert-priority', '7', '0'],
            ['--jobs-insert', '/tmp/data', 'cache', 'extra'],
        ]
        for argv in invalid:
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                ClientArgumentParser().parse(argv)


class TestExplicitJobHostId(unittest.TestCase):
    def test_explicit_hostid_is_dispatched_for_multiple_ids(self):
        for hostid in (0, 1, 7, 32767):
            args = ClientArgumentParser().parse(['--jobs-insert-hostid', str(hostid), '/tmp/data', 'cache,tmp'])
            self.assertEqual((args.hostid, args.path, args.skipdirs), (hostid, '/tmp/data', 'cache,tmp'))
            operation = client_module.client_operations(args)
            with mock.patch.object(operation, 'include_excluded_names', return_value='cache,tmp'), \
                 mock.patch.object(operation.database, 'insert_job') as insert:
                operation.run()
            insert.assert_called_once_with('/tmp/data', 'cache,tmp', hostid=hostid)

    def test_invalid_hosts_missing_paths_and_old_option_are_rejected(self):
        invalid = [
            ['--jobs-insert-hostid1', '/tmp/data'],
            ['--jobs-inser-hostid', '1', '/tmp/data'],
            ['--jobs-insert-hostid', '1'],
            ['--jobs-insert-hostid', 'abc', '/tmp/data'],
            ['--jobs-insert-hostid', '-1', '/tmp/data'],
            ['--jobs-insert-hostid', '32768', '/tmp/data'],
            ['--hostid', '7', '--jobs-insert-hostid', '8', '/tmp/data'],
        ]
        for argv in invalid:
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                ClientArgumentParser().parse(argv)

    def test_same_local_identity_can_validate_multiple_registered_hostids(self):
        database = object.__new__(app.DataBaseConnection)
        identity = ('alex', '/home/alex', 'Linux', 'same-computer')
        def hosts(hostid):
            return [(hostid, identity[0], identity[1], None, identity[2], identity[3],
                     None, f'tmp{hostid}_job_work')]
        with mock.patch.object(database, 'list_hosts', side_effect=hosts), \
             mock.patch.object(database, '_local_host_values', return_value=identity):
            self.assertEqual(database.validate_local_host(7), 7)
            self.assertEqual(database.validate_local_host(8), 8)


class TestLocalSmartStartOption(unittest.TestCase):
    def test_smart_start_selects_normal_localserver_with_diagnostics_enabled(self):
        args = ClientArgumentParser().parse(['--smartlocalserver', '--coarse-glob'])
        self.assertEqual((args.mode, args.local_hostid, args.smart_start), ('localserver', None, True))
        self.assertTrue(args.coarse_glob)

    def test_invalid_hostids_and_combined_modes_are_rejected(self):
        for argv in (['--smartlocalserver', '0'], ['--smartlocalserver', '32768'],
                     ['--smartlocalserver', '7', 'localserver', '7'],
                     ['--smartlocalserver', '7', '--jobs-list-active']):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                ClientArgumentParser().parse(argv)

    def test_main_dispatches_smart_start_to_server_and_returns_failure_status(self):
        with mock.patch.object(sys, 'argv', [app.__file__, '--smartlocalserver']), \
             mock.patch.object(app, 'server_operations') as server:
            server.return_value.run.return_value = 1
            self.assertEqual(app.main(), 1)
        self.assertIsNone(server.call_args.args[0].local_hostid)
        self.assertTrue(server.call_args.args[0].smart_start)


    def test_explicit_hostid_is_used(self):
        args = ClientArgumentParser().parse(['--smartlocalserver', '--hostid', '7'])
        self.assertEqual((args.mode, args.local_hostid, args.smart_start), ('localserver', 7, True))

    def test_old_name_and_negative_id_are_rejected(self):
        for argv in (['--localsmartstart'], ['--smartlocalserver', '-1']):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                ClientArgumentParser().parse(argv)


class TestDoublesProgressHostOption(unittest.TestCase):
    def test_host_filter_is_optional_and_host_zero_is_explicit(self):
        for argv, jobid, hostid, explicit in (
                (['--doubles-progress'], None, 0, False),
                (['--doubles-progress', '34'], 34, 0, False),
                (['--doubles-progress', '--hostid', '1'], None, 1, True),
                (['--hostid', '0', '--doubles-progress', '34'], 34, 0, True)):
            args = ClientArgumentParser().parse(argv)
            self.assertEqual((args.jobid, args.hostid, args.hostid_explicit), (jobid, hostid, explicit))

    def test_dispatch_passes_host_only_when_supplied(self):
        for argv, expected in (
                (['--doubles-progress'], mock.call(None)),
                (['--doubles-progress', '34', '--hostid', '1'], mock.call(34, hostid=1)),
                (['--doubles-progress', '--hostid', '0'], mock.call(None, hostid=0))):
            operation = client_module.client_operations(ClientArgumentParser().parse(argv))
            with mock.patch.object(operation, 'show_doubles_progress') as progress:
                operation.run()
            self.assertEqual(progress.call_args, expected)

    def test_invalid_host_is_rejected(self):
        for hostid in ('-1', '32768', 'abc'):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                ClientArgumentParser().parse(['--doubles-progress', '--hostid', hostid])


class TestLocalServerHostOption(unittest.TestCase):
    def test_localserver_accepts_named_host_before_or_after_subcommand(self):
        for argv in (['localserver', '--hostid', '1'],
                     ['--hostid', '1', 'localserver'],
                     ['--hostid', '1', 'localserver', '--coarse-glob']):
            with self.subTest(argv=argv):
                args = ClientArgumentParser().parse(argv)
                self.assertEqual((args.mode, args.local_hostid), ('localserver', 1))

    def test_smartlocalserver_accepts_the_same_named_host(self):
        for argv in (['--smartlocalserver', '--hostid', '1'],
                     ['--hostid', '1', '--smartlocalserver'],
                     ['--smartlocalserver', '--hostid', '1', '--coarse-glob']):
            with self.subTest(argv=argv):
                args = ClientArgumentParser().parse(argv)
                self.assertEqual((args.mode, args.local_hostid, args.smart_start), ('localserver', 1, True))
        args = ClientArgumentParser().parse(['--smartlocalserver'])
        self.assertIsNone(args.local_hostid)

    def test_missing_invalid_or_conflicting_local_hosts_are_rejected(self):
        for argv in (['localserver'], ['localserver', '--hostid', '0'],
                     ['localserver', '--hostid', '-1'], ['localserver', '--hostid', '32768'],
                     ['localserver', '1', '--hostid', '2'],
                     ['--smartlocalserver', '1', '--hostid', '2'],
                     ['--smartlocalserver', '--hostid', '0']):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                ClientArgumentParser().parse(argv)

    def test_named_host_reaches_server_dispatch(self):
        for argv in (['localserver', '--hostid', '1'], ['--smartlocalserver', '--hostid', '1']):
            with mock.patch.object(sys, 'argv', [app.__file__, *argv]), \
                 mock.patch.object(app, 'server_operations') as server:
                server.return_value.run.return_value = 0
                self.assertEqual(app.main(), 0)
            self.assertEqual(server.call_args.args[0].local_hostid, 1)


class TestPositionalLocalHostRemoved(unittest.TestCase):
    def test_positional_host_ids_are_rejected_even_if_named_host_matches(self):
        for argv in (['localserver', '1'], ['--smartlocalserver', '1'],
                     ['localserver', '1', '--hostid', '1'],
                     ['--smartlocalserver', '1', '--hostid', '1']):
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                ClientArgumentParser().parse(argv)


class PriorityTests_client_operations(unittest.TestCase):
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

    def test_cli_accepts_ids_and_rejects_invalid_values(self):
        args = ClientArgumentParser().parse(['--insert-priority', '1', '7', '8'])
        self.assertEqual((args.mode, args.priority_hostid, args.priority_dirids),
                         ('insert-priority', 1, [7, 8]))
        for values in (['-1', '7'], ['1'], ['1', '0'], ['32768', '7']):
            with self.assertRaises(SystemExit):
                ClientArgumentParser().parse(['--insert-priority', *values])


class BinarycheckHostTests(unittest.TestCase):
    def test_command_processes_all_hosts_even_with_host_option(self):
        operation = object.__new__(client_operations)
        operation.args = ClientArgumentParser().parse(['--doubles-letcheck', '--hostid', '1'])
        operation.database = mock.Mock()
        operation.database.create_binarycheck_job.return_value = (None, 0)
        with contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        operation.database.create_binarycheck_job.assert_called_once_with(
            confirm_repeat=operation._confirm_binarycheck_repeat)


class DeleteLiveExcludedTests(unittest.TestCase):
    def test_option_defaults_to_all_hosts_and_can_scope_host(self):
        args = ClientArgumentParser().parse(['--dirs-delete-live-excluded', '-Y'])
        self.assertEqual(args.mode, 'dirs-delete-live-excluded')
        self.assertFalse(args.hostid_explicit)
        args = ClientArgumentParser().parse(['--dirs-delete-live-excluded', '--hostid', '2', '-Y'])
        self.assertEqual(args.hostid, 2)
        self.assertTrue(args.hostid_explicit)

    def test_confirmation_decline_does_not_delete(self):
        operation = object.__new__(client_operations)
        operation.args = ClientArgumentParser().parse(['--dirs-delete-live-excluded'])
        operation.database = mock.Mock()
        operation.database.live_excluded_directories.return_value = 12
        with mock.patch('builtins.input', return_value='n'), contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        operation.database.live_excluded_directories.assert_called_once_with(hostid=None)

    def test_yes_counts_then_deletes_for_selected_host(self):
        operation = object.__new__(client_operations)
        operation.args = ClientArgumentParser().parse(['--dirs-delete-live-excluded', '--hostid', '1', '-Y'])
        operation.database = mock.Mock()
        operation.database.live_excluded_directories.side_effect = [12, 12]
        with contextlib.redirect_stdout(io.StringIO()):
            operation.run()
        self.assertEqual(operation.database.live_excluded_directories.call_args_list,
                         [mock.call(hostid=1), mock.call(hostid=1, delete=True)])


class DirectorySummaryTests(unittest.TestCase):
    def report(self, entries):
        operation = object.__new__(client_operations)
        operation.database = mock.Mock()
        operation.database.list_directory_hostids.return_value = []
        operation.database.directory_summary_data.return_value = [
            dict(hostid=host, runid=run, jobid=run + 100, directory_count=count, run_timestamp=stamp)
            for host, run, count, stamp in entries
        ]
        with contextlib.redirect_stdout(io.StringIO()) as output:
            operation.show_directory_summary_data()
        return output.getvalue()

    def test_summary_includes_one_job_dataset_with_runs_and_utc_end_time(self):
        operation = object.__new__(client_operations)
        operation.database = mock.Mock()
        operation.database.directory_summary_data.return_value = [
            dict(hostid=2, runid=100, jobid=10, directory_count=5, run_timestamp=None),
            dict(hostid=2, runid=101, jobid=10, directory_count=3, run_timestamp=None),
        ]
        operation.database.list_directory_hostids.return_value = [
            dict(hostid=2, jobid=10, runids=[100,101], timeend=datetime(2026,11,4,12,30), directory_count=8, root_paths=['/root/a','/root/b']),
            dict(hostid=2, jobid=11, runids=[102], timeend=None, directory_count=3, root_paths=['/other']),
            dict(hostid=7, jobid=None, runids=[999], timeend=None, directory_count=1, root_paths=[]),
        ]
        with contextlib.redirect_stdout(io.StringIO()) as output:
            operation.show_directory_summary_data()
        text = output.getvalue()
        self.assertIn('Host | Job-ID | Directories | Runids | Ende (UTC)', text)
        self.assertEqual(text.count('2 |  10 | 00008 | 100, 101 | 2026-11-04 12:30:00'), 1)
        self.assertIn('2 |  11 | 00003 | 102 | -', text)
        self.assertIn('7 |   - | 00001 | 999 | -', text)
        self.assertIn('| /root/a; /root/b', text)
        self.assertIn('Ende (UTC) | Root paths', text)
        self.assertNotIn('None', text)
        operation.database.list_directory_hostids.assert_called_once_with()

    def test_option_and_command(self):
        for command in ('--dirs-summary-data',):
            self.assertEqual(ClientArgumentParser().parse([command]).mode, 'dirs-summary-data')

    def test_empty(self):
        self.assertIn('Keine Einträge', self.report([]))

    def test_bars_first_totals_and_paired_legend_with_unique_letters(self):
        stamp = datetime(2026, 11, 3, 17, 23)
        lines = self.report([(0, 29, 10, stamp), (0, 8, 90, stamp),
                             (1, 71, 80, None), (1, 32, 20, stamp)]).splitlines()
        self.assertTrue(lines[0].startswith('0 |'))
        self.assertTrue(lines[1].startswith('1 |'))
        self.assertTrue(lines[0].endswith('Gesamt: 100'))
        self.assertTrue(lines[1].endswith('Gesamt: 100'))
        for line in lines[:2]:
            self.assertEqual(len(line.split('|')[1]), 64)
        self.assertIn('A: job 108', lines[0])
        self.assertIn('C: job 171', lines[1])
        self.assertIn('Time', lines[2])
        self.assertIn('A->8', lines[3])
        self.assertIn('B->29', lines[3])
        self.assertIn('000090', lines[3])
        self.assertNotIn('2026-11-03', lines[3])
        self.assertIn('|   A->8  000090', lines[3])
        self.assertIn('C->71', lines[4])
        self.assertIn('D->32', lines[4])
        self.assertNotIn('Verteilung je Host', '\n'.join(lines))

    def test_omitted_segments_and_odd_legend_row(self):
        lines = self.report([(7, 3, 97, None), (7, 2, 2, None), (7, 1, 1, None)]).splitlines()
        bar = lines[0].split('|')[1]
        self.assertEqual(len(bar), 64)
        self.assertTrue(bar.startswith('<'))
        self.assertTrue(bar.endswith('>..'))
        self.assertNotIn('B', bar)
        self.assertNotIn('C', bar)
        self.assertIn('C->1', lines[-1])
        self.assertNotIn('None', '\n'.join(lines))

    def test_letters_include_j_and_skip_i(self):
        lines = self.report([(0, i, 1, None) for i in range(11)]).splitlines()
        self.assertNotIn('I', lines[0])
        self.assertIn('J', lines[0])
        self.assertIn('K', lines[0])

    def test_host_bars_share_a_common_count_scale(self):
        lines = self.report([(0, 8, 1000, None), (1, 9, 500, None),
                             (2, 10, 250, None)]).splitlines()
        self.assertEqual([len(line.split('|')[1]) for line in lines[:3]], [64, 32, 16])
        self.assertTrue(lines[0].startswith('0 |'))
        self.assertTrue(lines[0].endswith('Gesamt: 1000'))

    def test_terminal_bars_use_inverse_colors(self):
        operation = object.__new__(client_operations)
        operation.database = mock.Mock()
        operation.database.list_directory_hostids.return_value = []
        operation.database.directory_summary_data.return_value = [
            dict(hostid=0, runid=1, directory_count=100, run_timestamp=None)
        ]
        output = io.StringIO()
        with mock.patch.object(output, 'isatty', return_value=True), contextlib.redirect_stdout(output):
            operation.show_directory_summary_data()
        first_line = output.getvalue().splitlines()[0]
        self.assertIn('|\033[7m<', first_line)
        self.assertIn('>\033[0m| Gesamt: 100', first_line)
        self.assertNotIn('\033[', self.report([(0, 1, 100, None)]))

    def test_null_statistics_portion_is_not_inverted(self):
        operation = object.__new__(client_operations)
        operation.database = mock.Mock()
        operation.database.list_directory_hostids.return_value = []
        for pending, inverted_length in ((0, 64), (50, 32), (100, 0)):
            with self.subTest(pending=pending):
                operation.database.directory_summary_data.return_value = [
                    dict(hostid=0, runid=1, directory_count=100,
                         incomplete_count=Decimal(pending), run_timestamp=None)
                ]
                output = io.StringIO()
                with mock.patch.object(output, 'isatty', return_value=True), contextlib.redirect_stdout(output):
                    operation.show_directory_summary_data()
                bar = output.getvalue().splitlines()[0].split('|')[1]
                if inverted_length:
                    colored = bar.split('\033[7m')[1].split('\033[0m')[0]
                    self.assertEqual(len(colored), inverted_length)
                else:
                    self.assertNotIn('\033[', bar)
                self.assertEqual(len(bar.replace('\033[7m', '').replace('\033[0m', '')), 64)

    def test_relative_days_and_hour_only_for_today(self):
        now = datetime.now(timezone.utc)
        today = self.report([(0, 1, 100, now)])
        self.assertIn(now.astimezone().strftime('%Hh%M'), today)
        older = self.report([(0, 1, 100, now - timedelta(days=3))])
        self.assertIn('-3 days', older)

    def test_completion_modes_derive_from_statistics_and_metadata_hashes(self):
        operation = object.__new__(client_operations)
        operation.database = mock.Mock()
        operation.database.list_directory_hostids.return_value = []
        for incomplete, missing_hashes, expected in [(1, 1, 1), (0, 1, 2), (0, 0, 3), (1, 0, 1)]:
            operation.database.directory_summary_data.return_value = [
                dict(hostid=0, runid=8, directory_count=10, run_timestamp=None,
                     incomplete_count=Decimal(incomplete), missing_hash_count=Decimal(missing_hashes))
            ]
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                operation.show_directory_summary_data()
            self.assertIn(f'A->8  000010  {expected}   -', output.getvalue())

    def test_active_job_star_between_mode_and_time(self):
        operation = object.__new__(client_operations)
        operation.database = mock.Mock()
        operation.database.list_directory_hostids.return_value = []
        for active, marker in ((1, '*'), (0, ' ')):
            operation.database.directory_summary_data.return_value = [
                dict(hostid=0, runid=8, directory_count=10, run_timestamp=None,
                     incomplete_count=0, missing_hash_count=0, active_job=active)
            ]
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                operation.show_directory_summary_data()
            self.assertIn(f'A->8  000010  3{marker}  -', output.getvalue())

    def test_bar_uses_job_id_while_legend_retains_run_id(self):
        lines = self.report([(0, 8, 100, None)]).splitlines()
        self.assertIn('A: job 108', lines[0])
        self.assertNotIn('runid', lines[0])
        self.assertIn('A->8', lines[-1])

    def test_all_tiny_blocks_are_represented_by_dots_at_shared_scale(self):
        lines = self.report([(0, 1, 100, None), (1, 2, 1, None), (1, 3, 1, None)]).splitlines()
        self.assertEqual(len(lines[0].split('|')[1]), 64)
        self.assertEqual(lines[1].split('|')[1], '.')

    def test_omitted_dots_use_completed_entry_share_including_partial_runs(self):
        operation = object.__new__(client_operations)
        operation.database = mock.Mock()
        operation.database.list_directory_hostids.return_value = []
        for incomplete, expected_inverted in ((3, 0), (2, 1), (1, 3), (0, 4)):
            operation.database.directory_summary_data.return_value = [
                dict(hostid=0, runid=3, directory_count=94, incomplete_count=0,
                     missing_hash_count=0, run_timestamp=None),
                dict(hostid=0, runid=2, directory_count=3, incomplete_count=incomplete,
                     missing_hash_count=1, run_timestamp=None),
                dict(hostid=0, runid=1, directory_count=3, incomplete_count=incomplete,
                     missing_hash_count=1, run_timestamp=None),
            ]
            output = io.StringIO()
            with mock.patch.object(output, 'isatty', return_value=True), contextlib.redirect_stdout(output):
                operation.show_directory_summary_data()
            bar = output.getvalue().splitlines()[0].split('|')[1]
            if expected_inverted:
                self.assertIn('\033[7m' + '.' * expected_inverted + '\033[0m', bar)
            else:
                self.assertNotIn('\033[7m.', bar)
            self.assertEqual(bar.replace('\033[7m', '').replace('\033[0m', '').count('.'), 4)


class TreeSnapshotTests(unittest.TestCase):
    def test_parser_and_dispatch(self):
        for argv, jobid in [(['--doubles-tree'], None), (['--doubles-tree', '39'], 39)]:
            args = ClientArgumentParser().parse(argv)
            self.assertEqual((args.mode, args.jobid), ('doubles-tree', jobid))
            operation = object.__new__(client_operations)
            operation.args = args
            renderer = mock.Mock()
            with mock.patch.object(operation, '_duplicate_output', return_value=renderer):
                operation.run()
            renderer.show_doubles_tree.assert_called_once_with(jobid)


class ScanallHostTests(unittest.TestCase):
    def test_explicit_host_and_legacy_path(self):
        for argv, host in [(['0', '/volume1/path'],0), (['1','/volume1/path'],1), (['/volume1/path'],0)]:
            args = ClientArgumentParser().parse(['--jobs-insert-scanall'] + argv)
            self.assertEqual(args.path, '/volume1/path')
            self.assertEqual(args.hostid, host)
            self.assertEqual(args.skipdirs, '')

    def test_selected_host_is_passed_to_database(self):
        operation = object.__new__(client_operations)
        operation.args = ClientArgumentParser().parse(['--jobs-insert-scanall','1','/volume1/path'])
        operation.database = mock.Mock()
        operation.database.list_directory_hostids.return_value = []
        with mock.patch.object(operation, 'include_excluded_names', return_value=''):
            operation.run()
        operation.database.insert_job.assert_called_once_with('/volume1/path', '', scanall=1, hostid=1)


# Binarycheck freshness regression coverage.


class RepeatPromptTests(unittest.TestCase):
    def test_only_explicit_yes_accepts_repetition(self):
        for answer, expected in [('', False), ('n', False), ('y', True), (' Y ', True)]:
            with self.subTest(answer=answer), mock.patch('builtins.input', return_value=answer) as prompt:
                self.assertEqual(client_operations._confirm_binarycheck_repeat(None, 3), expected)
                self.assertIn('[yN]', prompt.call_args.args[0])
        with mock.patch('builtins.input', side_effect=EOFError):
            self.assertFalse(client_operations._confirm_binarycheck_repeat(1, 2))


if __name__ == '__main__':
    unittest.main()
