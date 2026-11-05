# version 0.25.2

"""Server-Steuerung, Signalbehandlung und Scan-Kindprozesse für version22."""

__version__ = "0.25.2"

import html
import fcntl
import hashlib
import tempfile
from contextlib import contextmanager
import logging
from logging.handlers import SysLogHandler
import multiprocessing
import os
import re
import signal
import socket
import sys
import time
import traceback
import json
import threading
import uuid
from pathlib import Path
from datetime import datetime, timedelta, timezone
from DataBaseCore import statement_timeout_message, is_network_error


def _runtime():
    from version22 import ApplicationContext
    return ApplicationContext.runtime()

@contextmanager
def _server_instance_lock(database, hostid, directory=None):
    """Hold one OS lock per database endpoint/hostid across all server modes.

    Leave the file after normal release. Explicit reset assumes servers are stopped and may remove the
    file; verify its inode after acquiring the lock to handle that race safely. The OS releases the lock even after a forced exit;
    the PID text alone never decides whether an instance is running.
    """
    scope = repr((str(getattr(database, 'host', 'localhost')).casefold(),
                  int(getattr(database, 'port', 3306)), str(database.database), int(hostid)))
    digest = hashlib.sha256(scope.encode('utf-8')).hexdigest()
    folder = Path(tempfile.gettempdir()) if directory is None else Path(directory)
    path = folder / f'dirwalker-server-{digest}.lock'
    flags = os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0)
    while True:
        descriptor = os.open(str(path), flags, 0o600)
        with os.fdopen(descriptor, 'r+', encoding='utf-8') as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.seek(0)
                owner = handle.read().strip() or 'noch unbekannt'
                raise RuntimeError(
                    f'Für Host-ID {hostid} und Datenbank {database.database} läuft bereits '
                    f'eine Serverinstanz (PID {owner}); neuer Start abgebrochen.'
                ) from None
            try:
                # resetlocks may unlink a leftover file after we opened it.
                # Never start a server holding only an obsolete, unlinked inode.
                try:
                    current = os.stat(str(path), follow_symlinks=False)
                except FileNotFoundError:
                    continue
                opened = os.fstat(handle.fileno())
                if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
                    continue
                handle.seek(0)
                handle.truncate()
                handle.write(str(os.getpid()))
                handle.flush()
                yield
                return
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class server_operations:
    """Coordinate job polling, signals and spawned workers."""

    def __init__(self, args):
        self.args = args
        self.database = _runtime().database_connection
        self.recent_runids = set()
        self._syslog_handler = None
        self._logger = None
        self._coarse_debug_event = None
        self._shutdown_finished = threading.Event()
        self._shutdown_started = False
        self.worker_error = None
        self._connection_failures = 0
        self._resume_ready = False
        self._stall_timeout = max(30, int(os.environ.get("DIRWALKER_STALL_TIMEOUT", "1200")))
        self._wake_restart = threading.Event()
        self._remote_listener = None
        self._next_remote_check = 0.0
        self._remote_work_active = False


    @staticmethod
    def _remote_listener_entry(hostid):
        os.setsid()
        def stop(_signum, _frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        database = _runtime().database_connection
        database.start_wake_monitor()
        listener_runid = None
        try:
            listener_runid = database.register_remote_listener_run(hostid)
            if listener_runid is None:
                return
            print(f'Listening Host {hostid}: Runid {listener_runid}, PID {os.getpid()}.', flush=True)
            while True:
                try:
                    if not database.serve_remote_checksum(hostid, listener_runid):
                        time.sleep(1)
                except Exception as error:
                    if not is_network_error(error):
                        raise
                    print(f'Listening Host {hostid}: neue DB-Verbindung nach Fehler: {error}', flush=True)
                    time.sleep(5)
        except KeyboardInterrupt:
            pass
        finally:
            try:
                if listener_runid is not None:
                    database.finish_remote_listener_run(listener_runid)
            except Exception as error:
                print(f'Listener-Runid {listener_runid} konnte nicht geschlossen werden: {error}', flush=True)
            database.stop_wake_monitor()

    def _service_remote_listener(self):
        if (self.args.mode not in ('serverplus', 'localserver')
                or not hasattr(self, '_server_hostid')):
            return False
        now = time.monotonic()
        listener = self._remote_listener
        if now < self._next_remote_check:
            return self._remote_work_active
        self._next_remote_check = now + 30
        hostid = getattr(self, '_server_hostid', 0)
        try:
            runs = self.database.remote_binarycheck_runs(hostid)
        except Exception as error:
            if not is_network_error(error):
                raise
            self._network_warning(f'Listening-Prüfung fehlgeschlagen; neuer Versuch in 30s: {error}')
            return self._remote_work_active
        if listener is not None and not listener.is_alive():
            listener.join(timeout=0)
            self._remote_listener = listener = None
        if runs and listener is None:
            self._remote_listener = multiprocessing.get_context('spawn').Process(
                target=self._remote_listener_entry, args=(hostid,),
                name=f'remote-listening-{hostid}',
            )
            self._remote_listener.start()
            self._configure_syslog()
            self._logger.info('Listening: Host %s liefert Inhaltsprüfsummen für aktive Remote-Runs.', hostid)
        self._remote_work_active = bool(runs)
        return self._remote_work_active

    def _stop_remote_listener(self):
        listener = self._remote_listener
        if listener is None:
            return
        if listener.is_alive():
            listener.terminate()
        listener.join(timeout=2)
        if listener.is_alive():
            self._force_stop_child(listener)
            listener.join(timeout=1)
        self._remote_listener = None


    def _configure_syslog(self):
        """Use syslog; localserver sends WARNING and above and prints locally."""
        if self._logger is not None:
            return
        logger = logging.getLogger(f"filewalker.server.{os.getpid()}")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        if self.args.mode == "localserver":
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            )
            logger.addHandler(console_handler)
        syslog_handler = _runtime().SysLogHandler(
            address=("localhost", 5101), facility=_runtime().SysLogHandler.LOG_DAEMON
        )
        if self.args.mode == "localserver":
            syslog_handler.setLevel(logging.WARNING)
        syslog_handler.setFormatter(_runtime().IETF_syslog_formatter())
        logger.addHandler(syslog_handler)
        self._logger = logger
        self._syslog_handler = syslog_handler


    def log_job_started(self, jobid, stage, root_path):
        """Den Jobstart im Syslog oder beim localserver auf der Konsole zeigen."""
        self._configure_syslog()
        self._logger.info(
            "Job %s beginnt: Stufe %s, Verzeichnis %s", jobid, stage, root_path
        )


    def log_server_stopped(self):
        """Das Beenden des Servermodus protokollieren."""
        self._configure_syslog()
        self._logger.info("Servermodus beendet")


    def log_coarse_rescheduled(self, jobid, hostid, count):
        self._configure_syslog()
        message = (f"Job {jobid}, Host-ID {hostid}: {count} verschwundene oder ersetzte "
                   "Verzeichnisse; coarse mit allscan (scanall=1) neu eingeplant.")
        self._logger.warning(message)
        if self.args.mode != "localserver":
            print(message, flush=True)


    def log_job_error(self, jobid, message):
        """Einen Fehler im Syslog oder beim localserver auf der Konsole zeigen."""
        self._configure_syslog()
        self._logger.error("Job %s auf error gesetzt: %s", jobid, message)


    @staticmethod
    def _log_worker_note(message):
        """Send committed repair notices to both console and the existing syslog endpoint."""
        print(message,flush=True)
        handler=SysLogHandler(address=('localhost',5101),facility=SysLogHandler.LOG_DAEMON)
        try:
            handler.setFormatter(_runtime().IETF_syslog_formatter())
            logger=logging.Logger('filewalker.worker.repair',logging.WARNING)
            logger.addHandler(handler)
            logger.warning(message)
        finally:
            handler.close()

    @staticmethod
    def _child_entry(
        stage, root_path, jobid, skipdirs="", scanall=1, recent_runids=(), task="scan",
        hostid=0, debug_event=None, live_excluded_absdirs=(), coarse_glob=False,
        error_report=None,
    ):
        """Ausgewählte Scan-Stufe mit dem Ausschluss-Snapshot ausführen."""
        # A separate process group keeps terminal Ctrl-C in the coordinator;
        # the deadline can also stop a nested priority worker if necessary.
        if multiprocessing.parent_process() is not None:
            os.setsid()
        def stop_child(_signum, _frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGINT, stop_child)
        signal.signal(signal.SIGTERM, stop_child)
        _runtime()._live_excluded_absdirs[hostid] = tuple(live_excluded_absdirs)
        _runtime().database_connection.start_wake_monitor()
        try:
            if task == 'rescan-after-days':
                next_stage = _runtime().database_connection.run_age_rescan_job(
                    jobid, hostid, note=server_operations._log_worker_note)
                if next_stage == 'fixunreadable' and error_report is not None:
                    error_report.value = b'AGE_RETRY_FIX'
            elif task == "check-modifications":
                count = _runtime().database_connection.check_modifications(hostid=hostid)
                print(f"Job {jobid}: {count} geänderte Verzeichnisse markiert.", flush=True)
            elif task == "rescan":
                roots, jobs = _runtime().database_connection.directories_rescan_marked(hostid=hostid)
                print(f"Job {jobid}: {roots} Startverzeichnisse geprüft; {jobs} Detailjobs eingereiht.",
                      flush=True)
            elif task == "binarycheck":
                _runtime().database_connection.run_binarycheck_job(jobid, hostid=hostid)
            elif stage == "coarse":
                if coarse_glob and hostid > 0:
                    _runtime().coarse_scan(
                        root_path, jobid, skipdirs, scanall, recent_runids, hostid,
                        debug_event=debug_event, coarse_glob=True,
                    )
                elif debug_event is None:
                    _runtime().coarse_scan(
                        root_path, jobid, skipdirs, scanall, recent_runids, hostid
                    )
                else:
                    _runtime().coarse_scan(
                        root_path, jobid, skipdirs, scanall, recent_runids, hostid,
                        debug_event=debug_event,
                    )
            else:
                vanished_count = _runtime().detailed_scan(
                    root_path, jobid, scanall, recent_runids, task, hostid,
                    **({"skipdirs": skipdirs} if skipdirs else {})
                )
                if isinstance(vanished_count, int) and vanished_count >= 24 and error_report is not None:
                    error_report.value = b'RESCAN_COARSE:24'
        except KeyboardInterrupt:
            return
        except BaseException as error:
            if is_network_error(error) and error_report is not None:
                error_report.value = ('DB_NETWORK:' + str(error)).encode('utf-8')[:4095]
                return
            message = statement_timeout_message(error) or f"{type(error).__name__}: {error}"
            if error_report is not None:
                # One writer, read only after process exit: no shared lock that
                # could remain held if the worker is killed during reporting.
                error_report.value = message.encode('utf-8')[:4095]
            traceback.print_exc()
            raise
        finally:
            _runtime().database_connection.stop_wake_monitor()


    def run_spawn(
        self, stage, root_path, jobid, skipdirs="", scanall=1, recent_runids=(), task="scan",
        hostid=0,
    ):
        """Eine Scan-Stufe mit dem Spawn-Startverfahren ausführen."""
        context = multiprocessing.get_context("spawn")
        self.worker_error = None
        error_report = context.RawArray('c', 4096)
        debug_event = None
        if (
            stage == "coarse" and hostid > 0
            and self.args.mode == "localserver"
        ):
            debug_event = self._coarse_debug_event
        process = context.Process(
            target=self._child_entry,
            args=(
                stage, root_path, jobid, skipdirs, scanall, recent_runids, task,
                hostid, debug_event, _runtime()._live_excluded_absdirs.get(hostid, ()),
                bool(stage == "coarse" and hostid > 0
                     and self.args.mode == "localserver"
                     and getattr(self.args, "coarse_glob", False)),
            ),
            kwargs={'error_report': error_report},
            name=f"{stage}-scan-{jobid}",
        )
        _runtime()._current_child = process
        self._wake_restart.clear()
        process.start()
        check_interval = (5 if self.args.mode == 'localserver' else
                          max(0.5, _runtime().POLL_SECONDS))
        next_status_check = 0.0
        next_progress_update = time.monotonic() + 1200
        unfilled_previous = None
        unfilled_current = None
        next_directory_check = 0.0
        directory_progress_until = None
        watch_directories = (stage == 'detailed' and task != 'binarycheck'
                             and self.args.mode == 'localserver')
        try:
            while process.is_alive():
                process.join(timeout=0.5)
                self._service_remote_listener()
                if _runtime()._shutdown:
                    if process.is_alive():
                        process.join(timeout=max(0, self._shutdown_deadline - time.monotonic()))
                    if process.is_alive():
                        self._force_stop_child(process)
                        process.join(timeout=0.5)
                        print('Scan-Prozess nach Ablauf der Stop-Frist beendet.', flush=True)
                    return "paused"
                if self._wake_restart.is_set():
                    self._wake_restart.clear()
                    self._network_warning(
                        f"Job {jobid}: Aufwachen erkannt; alten Scan-Prozess beenden, "
                        "danach neue DB-Verbindung und Checkpoint-Fortsetzung.")
                    if process.is_alive():
                        self._force_stop_child(process)
                    process.join(timeout=2)
                    if process.is_alive():
                        raise RuntimeError("Alter Scan-Prozess konnte nicht beendet werden; kein Neustart.")
                    return "network_retry"
                now = time.monotonic()
                if now >= next_status_check:
                    next_status_check = now + check_interval
                    try:
                        status = self.database.get_job_status(jobid, hostid=hostid)
                        idle = (self.database.get_job_idle_seconds(jobid, hostid)
                                if self.args.mode == 'localserver' and status == 'active' else None)
                        if watch_directories and status == 'active' and now >= next_directory_check:
                            count_root = os.path.realpath(os.path.abspath(root_path))
                            try:
                                unfilled_current = self.database.count_unfilled_detailed_directories(
                                    hostid, count_root
                                )
                            except Exception as error:
                                if not is_network_error(error):
                                    raise
                                self._network_warning(
                                    f"Job {jobid}: Fortschrittszählung fehlgeschlagen; "
                                    f"erneuter Versuch mit neuer DB-Verbindung: {error}")
                                # Each count call opens and closes its own transport.
                                unfilled_current = self.database.count_unfilled_detailed_directories(
                                    hostid, count_root
                                )
                            if unfilled_previous is None or unfilled_current < unfilled_previous:
                                directory_progress_until = now + self._stall_timeout
                            unfilled_previous = unfilled_current
                            next_directory_check = now + min(1200, self._stall_timeout)
                    except Exception as error:
                        if not is_network_error(error):
                            raise
                        self._network_warning(f"Job {jobid}: Netzwerkprüfung fehlgeschlagen; "
                                              "Scan-Prozess bleibt überwacht. " + str(error))
                        next_status_check = time.monotonic() + 30
                        continue
                    if (isinstance(idle, (int, float)) and idle >= self._stall_timeout
                            and (not watch_directories or (
                                directory_progress_until is not None
                                and now >= directory_progress_until))):
                        detail = (f"; weiterhin {unfilled_current} Verzeichnisse mit NULL-Statistiken"
                                  if watch_directories else "")
                        self._network_warning(
                            f"Job {jobid}: Timeout nach {idle}s ohne gespeicherten Fortschritt"
                            f"{detail}; alten Worker beenden und Checkpoint neu starten.")
                        if process.is_alive():
                            self._force_stop_child(process)
                        process.join(timeout=2)
                        if process.is_alive():
                            raise RuntimeError("Scan-Prozess lebt trotz Timeout weiter; kein Neustart.")
                        return "network_retry"
                    if stage == "detailed" and task != "binarycheck" and now >= next_progress_update:
                        try:
                            total, outstanding = self.database.get_detailed_directory_progress(
                                jobid, hostid, os.path.realpath(os.path.abspath(root_path))
                            )
                        except Exception as error:
                            if not is_network_error(error):
                                raise
                            self._network_warning(f"Job {jobid}: Fortschrittsabfrage fehlgeschlagen: {error}")
                        else:
                            percent = 100.0 * outstanding / total if total else 0.0
                            message = (f"Job {jobid}: detailed; noch {outstanding} von {total} "
                                       f"Verzeichnissen offen ({percent:.2f} %)"
                                       + (f"; {unfilled_current} mit NULL-Statistiken"
                                          if unfilled_current is not None else ""))
                            self._configure_syslog()
                            self._logger.info(message)
                            if self.args.mode != "localserver":
                                print(message, flush=True)
                        next_progress_update = now + 1200
                    if status == "stopped":
                        if process.is_alive():
                            process.terminate()
                        process.join()
                        return "stopped"
            if _runtime()._shutdown:
                return "paused"
            report = error_report.value
            if isinstance(report, bytes) and report.startswith(b'DB_NETWORK:'):
                self.worker_error = report.decode('utf-8', errors='replace')
                return "network_retry"
            if report == b'AGE_RETRY_FIX' and process.exitcode == 0:
                return 'age_retry_fix'
            if report == b'RESCAN_COARSE:24' and process.exitcode == 0:
                return "rescan_coarse"
            if isinstance(report, bytes) and report:
                self.worker_error = report.decode('utf-8', errors='replace')
                print(f'Job {jobid}, Host-ID {hostid}, Stufe {stage}: {self.worker_error}', flush=True)
            if process.exitcode == 0:
                return "ok"
            if process.exitcode is not None and process.exitcode < 0:
                return "paused"
            return "error"
        finally:
            _runtime()._current_child = None


    @staticmethod
    def _force_stop_child(process):
        """Stop the isolated scan group, including priority grandchildren."""
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            if process.is_alive():
                process.kill()

    def handle_signal(self, signum, _frame):
        """Interrupt workers, cancel owned DB queries and bound shutdown to 20 seconds."""
        _runtime()._shutdown = True
        if self._shutdown_started:
            return
        self._shutdown_started = True
        self._shutdown_deadline = time.monotonic() + 20
        print('Stop angefordert: DB-Verbindungen schließen und Rollback abwarten '
              '(höchstens 20 Sekunden).', flush=True)
        child = _runtime()._current_child
        if child is not None and child.is_alive():
            os.kill(child.pid, signal.SIGINT)

        def cleanup():
            try:
                self.database.cleanup_server_queries(self._server_hostid, self._server_group, timeout=19)
            except Exception as error:
                print('DB-Aufräumen beim Stop: {}'.format(error), flush=True)

        def watchdog():
            if not self._shutdown_finished.wait(20):
                print('20 Sekunden abgelaufen: Server wird beendet. Ein laufendes '
                      'Datenbank-Rollback kann serverseitig weiterlaufen.', flush=True)
                current = _runtime()._current_child
                if current is not None:
                    self._force_stop_child(current)
                os._exit(130)

        self._cleanup_thread = threading.Thread(target=cleanup, daemon=True)
        self._cleanup_thread.start()
        threading.Thread(target=watchdog, daemon=True).start()


    def handle_debug_signal(self, _signum, _frame):
        """Toggle verbose coarse-scan messages in localserver and its child."""
        event = self._coarse_debug_event
        if event is None:
            return
        if event.is_set():
            event.clear()
            state = "aus"
        else:
            event.set()
            state = "an"
        message = f"Localserver Coarse-Debug: {state}"
        print(message, flush=True)
        _runtime()._append_localserver_log(message)


    def run(self):
        if not getattr(self.args, 'smart_start', False):
            return self.run_server()
        self._startup_complete = False
        try:
            self.args.local_hostid = self._find_smart_local_host()
            return self.run_server()
        except (_runtime().Error, OSError, RuntimeError, ValueError) as exc:
            self._explain_local_start_failure(exc)
            return 1

    def _find_smart_local_host(self):
        """Choose only an unambiguous local positive registration; inspect error jobs first."""
        identity = self.database._local_host_values()
        hosts = [row for row in self.database.list_hosts()
                 if 1 <= int(row[0]) <= 32767
                 and (row[1], row[2], row[4], row[5]) == identity]
        if not hosts:
            raise RuntimeError('Keine passende positive Host-ID für lokalen Benutzer/Rechner registriert.')
        hostids = sorted({int(row[0]) for row in hosts})
        requested = getattr(self.args, 'local_hostid', None)
        self._smart_automatic_host = requested is None
        if requested is not None:
            if requested not in hostids:
                raise RuntimeError(f'Host-ID {requested} passt nicht zum lokalen Benutzer/Rechner.')
            hostids = [requested]
        jobs = self.database.local_start_jobs(hostids)
        errors = [job for job in jobs if job['status'] == 'error']
        hosts_with_work = {int(job['hostid']) for job in jobs}
        candidates = sorted(hosts_with_work) if hosts_with_work else hostids
        if len(candidates) != 1:
            raise RuntimeError(f'Mehrere passende Host-IDs ({", ".join(map(str, candidates))}); '
                               'automatische Auswahl ist nicht eindeutig.')
        hostid = candidates[0]
        self.args.local_hostid = hostid
        self._smart_errors = errors
        return hostid

    def _reset_smart_errors(self, hostid):
        """Reuse the CLI's atomic reset, restricted to the selected positive host."""
        if hostid <= 0:
            raise ValueError('Smartstart benötigt eine positive Host-ID.')
        accepted = True
        for job in getattr(self, '_smart_errors', ()):
            if int(job['hostid']) != hostid:
                continue
            detail = '; '.join(str(job[field]) for field in ('errmsg', 'usererror') if job.get(field))
            print(f"Job {job['jobid']}, Host-ID {hostid}, Stufe {job['stage']}: "
                  f"status=error; {job['startdirname']}; {detail or 'kein gespeicherter Fehlertext'}",
                  flush=True)
            try:
                answer = input(f"Fehlerstatus für Job {job['jobid']} wie bei "
                               "--jobs-reset-on-error zurücksetzen? [yN] ").strip().lower()
            except EOFError:
                answer = ''
            if answer != 'y':
                accepted = False
                continue
            # Lazy import preserves the server-only import boundary otherwise.
            from DataBaseCLI import DataBaseCLI
            if not DataBaseCLI(self.database).reset_error_job(job['jobid'], hostid=hostid):
                print(f"Job {job['jobid']} wurde nicht zurückgesetzt; Status oder Host-ID geändert.", flush=True)
                accepted = False
            else:
                print(f"Job {job['jobid']}: Fehlerstatus zurückgesetzt.", flush=True)
        return accepted

    def _explain_local_start_failure(self, error):
        """Explain startup failures; mutations require explicit interactive consent."""
        hostid = getattr(self.args, 'local_hostid', None)
        hostid = hostid if hostid is not None else 'automatisch'
        phase = 'Betrieb' if getattr(self, '_startup_complete', False) else 'Start'
        print(f"FEHLER beim localserver-{phase} für Host-ID {hostid}: {error}", file=sys.stderr)
        program = os.path.basename(sys.argv[0])
        command = f"python3 {program}"
        code = getattr(error, 'errno', None)
        message = str(error).casefold()
        if code in (1044, 1045):
            suggestion = 'DB_USER, DB_PASSWORD und die Datenbankberechtigungen prüfen.'
        elif code in (2002, 2003, 2005, 2006, 2013):
            suggestion = 'DB_HOST und DB_PORT sowie Erreichbarkeit und Dienststatus des Datenbankservers prüfen.'
        elif code == 1049:
            suggestion = f'Datenbanknamen prüfen; für eine neue Datenbank zuerst {command} --init-jobs ausführen.'
        elif code == 1054 and re.search(r'\b(pid|process_host|process_start)\b', message):
            suggestion = 'migrate_jobsrunid_pid.sql einmal in der verwendeten Datenbank ausführen und erneut starten.'
        elif code == 1146:
            suggestion = (f'Fehlende Tabelle anhand der Fehlermeldung prüfen; {command} --init-jobs '
                          f'legt fehlende Jobtabellen an, {command} --setmyhostid registriert den lokalen Host.')
        elif code == 1054:
            suggestion = f'Mit {command} check das Schema prüfen und die zur fehlenden Spalte passende Migration anwenden.'
        elif 'db_user' in message or 'db_password' in message:
            suggestion = 'DB_USER und DB_PASSWORD in der Umgebung setzen bzw. die Datenbank-Konfiguration laden.'
        elif 'serverinstanz' in message or 'priority-binarycheck läuft bereits' in message:
            suggestion = ('Den in der Fehlermeldung genannten Prozess prüfen; die vorhandene Instanz verwenden '
                          'oder regulär beenden, bevor dieselbe Host-ID erneut gestartet wird.')
        elif 'passt nicht zum lokalen' in message:
            suggestion = (f'{command} show hosts ausführen und die zum aktuellen Benutzer/Rechner gehörende ID '
                          f'wählen; falls nötig mit {command} --setmyhostid eine passende Registrierung anlegen.')
        elif 'nicht registriert' in message or 'tabelle hosts fehlt' in message or 'keine passende positive host-id' in message:
            suggestion = f'{command} --setmyhostid ausführen und anschließend die dabei ausgegebene positive Host-ID verwenden.'
        elif 'status=error' in message:
            suggestion = (f'Gespeicherte Jobfehler oben beheben; danach {command} --jobs-reset-on-error '
                          'ausführen und gezielt eine der angezeigten Fehlerjob-IDs wählen.')
        elif 'mehrere passende host-ids' in message:
            suggestion = f'{command} show hosts prüfen und die gewünschte ID ausdrücklich mit {command} localserver --hostid HOSTID starten.'
        elif 'tabellenname' in message or 'schema' in message:
            suggestion = f'{command} check und {command} show hosts ausführen; die gemeldete Schemaabweichung gezielt korrigieren.'
        elif isinstance(error, PermissionError):
            suggestion = 'Dateisystemrechte für die genannte Datei bzw. das temporäre Sperrverzeichnis prüfen.'
        elif 'prozessprüfung' in message or 'startkennung' in message:
            suggestion = 'Leserechte auf lokale Prozessinformationen prüfen (/proc unter Linux, ps unter macOS).'
        else:
            suggestion = (f'Originalfehler oben, Serverprotokoll und Datenbank-Konfiguration prüfen; '
                          f'zusätzlich {command} check ausführen.')
        print('Vorschlag: ' + suggestion, file=sys.stderr)


    def _network_warning(self, message):
        self._configure_syslog()
        self._logger.warning(message)
        if self.args.mode != 'localserver':
            print(message, flush=True)

    def _on_wake(self, gap):
        self._wake_restart.set()
        self._network_warning(
            f"Aufwachen/Prozesspause erkannt ({gap:.1f}s): alte DB-Sockets verworfen; "
            "Netzwerk wird sofort mit einer neuen Verbindung geprüft.")

    def _wait_for_poll(self, seconds):
        # Wall time advances across macOS suspend; subtracting sleep intervals
        # would incorrectly wait out the old delay after the laptop wakes.
        if self._resume_ready:
            self._resume_ready = False
            return
        deadline = time.time() + seconds
        while not _runtime()._shutdown:
            self._service_remote_listener()
            if self.database.consume_wake_event():
                return
            remaining = deadline - time.time()
            if remaining <= 0:
                return
            time.sleep(min(0.5, remaining))

    def _connection_attempt_failed(self, error):
        self._connection_failures += 1
        self._network_warning(
            f"Neue DB-Verbindung fehlgeschlagen ({self._connection_failures}/600): {error}")
        if self._connection_failures >= 600:
            self._network_warning(
                "600 neue DB-Verbindungsversuche fehlgeschlagen; localserver wird beendet. "
                "Job-Checkpoint bleibt erhalten; DB-Status kann bei Ausfall active bleiben.")
            _runtime()._shutdown = True
            return True
        return False

    def recover_network_job(self, jobid, hostid):
        """Wait for fresh connectivity, then resume the persisted checkpoint."""
        self._network_warning(f"Job {jobid}: Netzwerk unterbrochen; alle 30s neue DB-Verbindung.")
        while not _runtime()._shutdown:
            try:
                # set_job_status opens a new connection on EVERY attempt. Do
                # not replay an unknown COMMIT; resume the saved work queue.
                runid = self.database.set_job_status(jobid, 'paused', hostid=hostid)
                self._connection_failures = 0
                if runid is not None:
                    self._resume_ready = True
                    self._network_warning(f"Job {jobid}: neue DB-Verbindung erfolgreich; "
                                          "Checkpoint zur Fortsetzung auf paused gesetzt.")
                return
            except Exception as error:
                if not is_network_error(error):
                    raise
                if self._connection_attempt_failed(error):
                    return
                self._wait_for_poll(30)

    def _check_root_job_startup(self):
        """Warn on competing jobs; offer an explicit, default-no local repair."""
        conflicts = self.database.list_root_path_job_conflicts()
        if not conflicts:
            return True
        fatal = self.args.mode == 'localserver'
        label = 'FEHLER' if fatal else 'WARNUNG'
        for conflict in conflicts:
            jobs = ', '.join(map(str, conflict['jobids']))
            print(f"{label}: root_path={conflict['root_path']} hat unterschiedliche Job-IDs: {jobs}",
                  file=sys.stderr, flush=True)
        if not fatal:
            return True
        candidates = self.database.plan_root_job_cleanup([row['root_path'] for row in conflicts])
        if candidates:
            print('Vorschlag: neuesten abgeschlossenen Job je Root behalten; ältere Jobs und '
                  'nicht aktive Jobs ohne Stoppzeit aus directories entfernen. Aktive Jobs bleiben erhalten.', file=sys.stderr)
            for row in candidates:
                ended = row['timeend'].strftime('%Y-%m-%d %H:%M:%S UTC') if row['timeend'] else 'keine Stoppzeit'
                print(f"  Host {row['hostid']}, Job {row['jobid']}, {row['directory_count']} Einträge, "
                      f"{ended}: {row['root_path']}", file=sys.stderr)
            try:
                answer = input('Diese Directory-Einträge löschen (Dateien bleiben erhalten)? [yN] ').strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = ''
            if answer == 'y':
                deleted = self.database.delete_root_job_candidates(candidates)
                print(f'{deleted} Directory-Einträge gelöscht.', flush=True)
                if not self.database.list_root_path_job_conflicts():
                    return True
                print('Weitere Job-Konflikte vorhanden.', file=sys.stderr)
        print('localserver wird wegen widersprüchlicher Root-Jobs nicht gestartet.',
              file=sys.stderr, flush=True)
        return False

    def run_server(self):
        """Allow exactly one server mode per database and host ID on this computer."""
        hostid = 0
        if self.args.mode == "localserver":
            hostid = self.database.validate_local_host(self.args.local_hostid)
        with _server_instance_lock(self.database, hostid):
            if not self._check_root_job_startup():
                return 1
            if getattr(self.args, 'smart_start', False):
                if not self.database.cleanup_server_queries(hostid, confirm=True):
                    return 1
                label = 'Automatisch ermittelte' if getattr(self, '_smart_automatic_host', True) else 'Gewählte'
                print(f'{label} Host-ID: {hostid}', flush=True)
                if not self._reset_smart_errors(hostid):
                    return 1
            previous_scope = os.environ.get('DIRWALKER_SERVER_SCOPE')
            self._server_hostid, self._server_group = hostid, uuid.uuid4().hex
            os.environ['DIRWALKER_SERVER_SCOPE'] = json.dumps(dict(
                hostid=hostid, machine=socket.gethostname(), group=self._server_group))
            self.database.start_wake_monitor(self._on_wake)
            try:
                return self._run_server(hostid)
            finally:
                self.database.stop_wake_monitor()
                if self._shutdown_started:
                    thread = getattr(self, '_cleanup_thread', None)
                    if thread is not None:
                        thread.join(timeout=max(0, self._shutdown_deadline - time.monotonic()))
                    print('Server beendet; DB-Aufräumen abgeschlossen oder gemeldet.', flush=True)
                self._shutdown_finished.set()
                if previous_scope is None:
                    os.environ.pop('DIRWALKER_SERVER_SCOPE', None)
                else:
                    os.environ['DIRWALKER_SERVER_SCOPE'] = previous_scope

    def _run_server(self, hostid):
        """Jobs abfragen und deren Scan-Stufen nacheinander abarbeiten."""
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        # Process identity is checked before claiming jobs or spawning workers.
        self.database.ensure_no_priority_worker(hostid)
        if self.args.mode == "localserver":
            self._coarse_debug_event = multiprocessing.get_context("spawn").Event()
            signal.signal(signal.SIGUSR1, self.handle_debug_signal)
            print(
                f"localserver PID {os.getpid()}; SIGUSR1 schaltet Coarse-Debug an oder aus.",
                flush=True,
            )
        _runtime()._live_excluded_absdirs[hostid] = tuple(self.database.list_live_excluded_absdirs(hostid))
        self.recent_runids = self.database.get_recent_runids(days=13)
        print(
            f"Server aktiv; Polling-Intervall: {_runtime().POLL_SECONDS}s; "
            f"DB: {self.database.database}; Treiber: {_runtime().DATABASE_DRIVER}",
            flush=True,
        )
        if self.args.mode == "localserver":
            print(
                f"Coarse-Debug-Datei: {_runtime().LOCALSERVER_LOG_FILE}; "
                "SIGUSR1 schaltet die Ausgabe an oder aus.",
                flush=True,
            )

        self._startup_complete = True
        try:
            while not _runtime()._shutdown:
                started = time.monotonic()
                network_failed = False
                job_processed = False
                try:
                    self._service_remote_listener()
                    # A paused job resumes in its persisted stage and checkpoint queue.
                    job = None
                    local_host = self.args.mode == "localserver"
                    include_binarycheck = self.args.mode in ("serverplus", "localserver")
                    for candidate_status in ("paused", "insert", "pending"):
                        job = _runtime().claim_job(
                            candidate_status, include_binarycheck, hostid,
                            socket.gethostname() if local_host else None,
                        )
                        if job is not None:
                            break
                    self._connection_failures = 0
                    if job is not None:
                        stage = job.get("stage") or (
                            "coarse" if job.get("status") == "insert" else "detailed"
                        )
                        if job.get("task") == "binarycheck":
                            stage = "binarycheck"
                        _runtime().process_job(job, stage, self)
                        job_processed = True
                except (_runtime().Error, OSError, RuntimeError, ValueError) as error:
                    message = statement_timeout_message(error)
                    network_failed = is_network_error(error)
                    if network_failed:
                        if self._connection_attempt_failed(error):
                            break
                        message = (f'Datenbankverbindung zu {self.database.host}:'
                                   f'{self.database.port} nicht möglich: {error}. '
                                   'Verbindungsaufbau ist auf 10 Sekunden begrenzt; '
                                   'erneuter Versuch in 30s mit neuer Verbindung (I/O-Limit 30s). '
                                   'Dies ist kein max_statement_time-Abbruch.')
                    if message:
                        print(message, flush=True)
                        self._configure_syslog()
                        self._logger.error(message)
                    traceback.print_exc()

                remaining = (30 if network_failed else 10 if job_processed else
                             max(0, _runtime().POLL_SECONDS - (time.monotonic() - started)))
                self._wait_for_poll(remaining)
        finally:
            self._stop_remote_listener()
            self.log_server_stopped()

