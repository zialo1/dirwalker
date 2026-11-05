# version 0.25.2

"""CLI-Datenbankverwaltung über eine übergebene DataBaseConnection."""

__version__ = "0.25.2"

import hashlib
import os
import re
import socket
import time
import sqlstructure
from sqlstructure import SQLStructureCLI
from DataBaseCore import is_network_error
from datetime import timedelta

from version22 import DataBaseConnection as DatabaseConnection, _path_hash


class DataBaseCLI(SQLStructureCLI):
    """CLI schema and data management through an injected DataBaseConnection."""

    def __init__(self, connection):
        self.connection = connection

    def __getattr__(self, name):
        # Shared read operations, configuration and schema constants belong to
        # the connection API; CLI mutations remain in this class.
        return getattr(self.connection, name)

    def connect(self):
        multiplier = self.__dict__.get('_timeout_multiplier', 1)
        if multiplier == 1:
            return self.connection.connect()
        connection = self.connection.connect(timeout_multiplier=multiplier)
        try:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.set_statement_timeout_multiplier(cursor, multiplier)
            finally:
                self.connection._close_preserving_error(cursor)
        except Exception:
            self.connection._close_preserving_error(connection)
            raise
        return connection

    def connect_server(self):
        return self.connection.connect_server()

    def connect_database(self):
        return self.connection.connect_database()

    def enqueue_directories_rescan_marked(self, hostid=0):
        """Queue a marked-directory rescan without inspecting the filesystem."""
        hostid = int(hostid)
        if not 0 <= hostid <= 32767:
            raise ValueError("Host-ID muss zwischen 0 und 32767 liegen.")
        now = self.utc_now
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(f"""INSERT INTO {self.TABLE_JOBS}
                    (userhost, hostid, startdirname, task, status, stage, skipdirs,
                     scanall, timecreated, timeupdated, last_activity)
                    VALUES (%s, %s, %s, %s, 'insert', 'coarse', '', 1, %s, %s, %s)""",
                    (socket.gethostname(), hostid, '<marked-directories>',
                     'rescan', now, now, now))
                jobid = cursor.lastrowid
                connection.commit()
                return jobid
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

    def enqueue_rescan_after_days(self, days, hostid=None):
        """Queue one fast scan per aged inventory owner job and host, without local I/O."""
        days = int(days)
        if days < 0:
            raise ValueError('Die Anzahl Tage darf nicht negativ sein.')
        if hostid is not None and not 0 <= int(hostid) <= 32767:
            raise ValueError('Host-ID muss zwischen 0 und 32767 liegen.')
        cutoff = self.utc_now - timedelta(days=days)
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(f"""SELECT DISTINCT d.hostid, owner.jobid, owner.skipdirs, COALESCE(h.machine,owner.userhost)
                    FROM {self.TABLE_DIRECTORIES} d
                    JOIN {self.TABLE_JOBS_RUNID} r ON r.runid = d.runid
                    JOIN {self.TABLE_JOBS} owner ON owner.jobid = r.jobid AND owner.hostid = d.hostid
                    LEFT JOIN {self.TABLE_HOSTS} h ON h.hostid = d.hostid
                    WHERE COALESCE(owner.timeend, owner.timecreated) < %s
                    """ + (' AND d.hostid = %s' if hostid is not None else '')
                    + ' ORDER BY d.hostid, owner.jobid',
                    (cutoff, int(hostid)) if hostid is not None else (cutoff,))
                owners = cursor.fetchall()
                created = []
                now = self.utc_now
                for selected, source, skipdirs, machine in owners:
                    cursor.execute(f"SELECT jobid FROM {self.TABLE_JOBS} WHERE hostid = %s "
                                   "AND source_jobid = %s AND task = 'rescan-after-days' "
                                   "AND status IN ('insert','pending','active','paused') LIMIT 1",
                                   (selected, source))
                    if cursor.fetchone() is not None:
                        continue
                    cursor.execute(f"""INSERT INTO {self.TABLE_JOBS}
                        (userhost, hostid, startdirname, task, status, stage, skipdirs, scanall,
                         source_jobid, timecreated, timeupdated, last_activity)
                        VALUES (%s, %s, '<age-rescan>', 'rescan-after-days', 'insert', 'firstscan', %s, 1,
                                %s, %s, %s, %s)""", (machine, selected, skipdirs or '', source, now, now, now))
                    created.append((selected, source, cursor.lastrowid))
                connection.commit()
                return created
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

    def enqueue_check_modifications(self, hostid=None):
        """Queue one timestamp-check job per selected registered host."""
        if hostid is not None:
            hostid = int(hostid)
            if not 0 <= hostid <= 32767:
                raise ValueError("Host-ID muss zwischen 0 und 32767 liegen.")
        now = self.utc_now
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT hostid, machine FROM {self.TABLE_HOSTS} "
                    + ("WHERE hostid = %s " if hostid is not None else "")
                    + "ORDER BY hostid", (hostid,) if hostid is not None else ())
                hosts = cursor.fetchall()
                if hostid is not None and not hosts:
                    raise ValueError(f"Host-ID {hostid} ist nicht registriert.")
                created = []
                for selected, machine in hosts:
                    cursor.execute(f"""INSERT INTO {self.TABLE_JOBS}
                        (userhost, hostid, startdirname, task, status, stage, skipdirs,
                         scanall, timecreated, timeupdated, last_activity)
                        VALUES (%s, %s, '<check-modifications>', 'check-modifications',
                                'insert', 'coarse', '', 1, %s, %s, %s)""",
                        (machine, selected, now, now, now))
                    created.append((selected, cursor.lastrowid))
                connection.commit()
                return created
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()









    def droptmp(self):
        """Drop numeric host queues, comparison results and deep results."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.select_table_names(cursor, self.database)
                tables = sorted(row[0] for row in cursor.fetchall()
                                if re.fullmatch(r"tmp[0-9]+_job_work", row[0])
                                or row[0] in ("tmp_deepdoubles_result", "tmp_doubles_letchecked", "tmp_remotedoubles",
                                              "tmp_deepdoubles_candidates", "mp_deepdoubles_candidates"))
                for table in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                connection.commit()
                return tables
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()




    def _host_home_dirid(self, cursor, hostid, homepath):
        sqlstructure.SQLStructure.select_table_count(cursor, self.database, self.TABLE_DIRECTORIES)
        if not cursor.fetchone()[0]:
            return None
        cursor.execute(
            f"SELECT dirid FROM {self.TABLE_DIRECTORIES} "
            "WHERE hostid = %s AND path_hash = %s AND BINARY path = BINARY %s",
            (hostid, _path_hash(homepath), homepath),
        )
        row = cursor.fetchone()
        return row[0] if row else None


    def _write_local_host(self, server_host=False):
        """Serialize registration; host zero is upserted without removing other hosts."""
        user, homepath, system, machine = self._local_host_values()
        lock_name = "hosts:" + hashlib.sha256(self.database.encode()).hexdigest()[:58]
        with self.connect() as connection:
            cursor = connection.cursor()
            locked = False
            try:
                cursor.execute("SELECT GET_LOCK(%s, 30)", (lock_name,))
                row = cursor.fetchone()
                if not row or row[0] != 1:
                    raise RuntimeError("Host-Tabelle ist gesperrt; bitte erneut versuchen.")
                locked = True
                self.create_hosts_table(cursor)
                if server_host:
                    hostid = 0
                else:
                    cursor.execute(
                        f"SELECT hostid, `user` FROM {self.TABLE_HOSTS} "
                        "WHERE BINARY homepath = BINARY %s "
                        "AND BINARY `system` = BINARY %s AND BINARY machine = BINARY %s "
                        "ORDER BY (BINARY `user` = BINARY %s) DESC, (hostid > 0) DESC, hostid LIMIT 1",
                        (homepath, system, machine, user),
                    )
                    row = cursor.fetchone()
                    create_new = row is None
                    if row is not None:
                        while True:
                            try:
                                answer = input(
                                    f"System {system!r}, Rechner {machine!r} und Home {homepath!r} "
                                    f"sind bereits als Host-ID {row[0]} registriert. "
                                    "Weitere Host-ID anlegen? [yN] "
                                ).strip().lower()
                            except EOFError:
                                answer = ''
                            if answer in ('', 'n', 'y'):
                                break
                            print("Bitte y oder N eingeben.")
                        create_new = answer == 'y'
                        if not create_new:
                            if int(row[0]) <= 0 or row[1] != user:
                                # Reusing another user's entry (or server host 0)
                                # would change ownership or violate localserver rules.
                                connection.rollback()
                                return None
                            hostid = int(row[0])
                    if create_new:
                        cursor.execute(f"SELECT COALESCE(MAX(hostid), 0) + 1 FROM {self.TABLE_HOSTS}")
                        hostid = int(cursor.fetchone()[0])
                    if not 1 <= hostid <= 32767:
                        raise ValueError("Keine gültige positive Host-ID verfügbar (1 bis 32767).")
                homedirid = self._host_home_dirid(cursor, hostid, homepath)
                table = (self.create_job_work_table(hostid, cursor, migrate_legacy=False)
                         if server_host else self.create_job_work_table(hostid, cursor))
                cursor.execute(
                    f"INSERT INTO {self.TABLE_HOSTS} "
                    "(hostid, `user`, homepath, homedirid, `system`, machine, timecreated, tabellenname) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE `user` = VALUES(`user`), "
                    "homepath = VALUES(homepath), homedirid = VALUES(homedirid), "
                    "`system` = VALUES(`system`), machine = VALUES(machine), tabellenname = VALUES(tabellenname)",
                    (hostid, user, homepath, homedirid, system, machine, self.utc_now, table),
                )
                connection.commit()
                return hostid
            except Exception:
                connection.rollback()
                raise
            finally:
                try:
                    if locked:
                        cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                        cursor.fetchone()
                finally:
                    cursor.close()


    def set_my_hostid(self):
        """Register a positive ID; ask before adding an ID for known machine/home data."""
        return self._write_local_host()



    def initialize(self):
        """Check timeout and fully invalidate old job references for a new inventory."""
        self.init_database()
        self._show_statement_timeout_hint()
        self.ensure_jobs_constraints()
        print('init: Wird directories neu angelegt, werden jobs, jobsrunid, '
              'Arbeitslisten sowie Vergleichs- und Prioritätsergebnisse geleert. '
              'Alle Server und Worker müssen vorher gestoppt sein.', flush=True)
        replaced = self.init_alldirs()
        if replaced:
            self._clear_inventory_jobs()
        self._write_local_host(server_host=True)
        return 0 if self.check_schema() else 1

    def _clear_inventory_jobs(self):
        """Delete stale dirid/runid references together; preserve tables and host IDs."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.select_table_names(cursor, self.database)
                tables = {row[0] for row in cursor.fetchall()}
                work_tables = sorted(table for table in tables
                                     if re.fullmatch(r'tmp[0-9]+_job_work', table))
                for table in work_tables:
                    cursor.execute(f'DELETE FROM `{table}`')
                for table in ('job_work', 'tmp_highpriority', 'tmp_deepdoubles_result',
                              'tmp_remotedoubles', self.TABLE_DOUBLE_JOBS.strip('`'), self.TABLE_JOBS_RUNID,
                              self.TABLE_JOBS):
                    if table in tables:
                        cursor.execute(f'DELETE FROM `{table}`')
                if self.TABLE_HOSTS in tables:
                    cursor.execute(f'UPDATE {self.TABLE_HOSTS} SET homedirid = NULL')
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()
        print('init: Jobs, Runid-Historie, Arbeitslisten und veraltete '
              'Verzeichnisverweise gelöscht; Tabellen und Host-IDs bleiben erhalten.', flush=True)

    def serverinit(self):
        """Check timeout and register host zero without resetting existing data."""
        self._show_statement_timeout_hint()
        self.ensure_jobs_constraints()
        return self._write_local_host(server_host=True)




    def _reset_host_work_tables(self):
        """Drop numeric host queues and recreate them empty, always including host 0."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                tables = self._work_tables(cursor)
                hostids = sorted({0, *(int(table[3:-9]) for table in tables)})
                # Validate every discovered ID before destructive DDL; table names
                # come only from the numeric tmp<HOSTID>_job_work catalog filter.
                for hostid in hostids:
                    self.work_table_name(hostid)
                for table in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                for hostid in hostids:
                    self.create_job_work_table(hostid, cursor, migrate_legacy=False)
                connection.commit()
                return [self.work_table_name(hostid) for hostid in hostids]
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

    def _mark_active_jobs_stale(self):
        """Mark every active jobs row stale, assuming no workers are running."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"UPDATE {self.TABLE_JOBS} SET status = 'stale', timeupdated = %s "
                    "WHERE status = 'active'",
                    (self.utc_now,),
                )
                changed = cursor.rowcount
                connection.commit()
                return changed
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

    def _reset_binarycheck_results(self):
        """serverinit resets comparison results to the current schema; no conversion."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute("DROP TABLE IF EXISTS tmp_remotedoubles")
                cursor.execute(f"DROP TABLE IF EXISTS {self.TABLE_DOUBLE_JOBS}")
                self.create_double_jobs_table(cursor)
                self.create_remote_doubles_table(cursor)
                cursor.execute("DROP TABLE IF EXISTS tmp_highpriority")
                self.create_priority_table(cursor)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()



    def insert_priority(self, hostid, dirids):
        """Queue existing host-scoped directory IDs atomically and idempotently."""
        hostid = int(hostid)
        ids = list(dict.fromkeys(int(value) for value in dirids))
        if not 0 <= hostid <= 32767 or not ids or any(
                value <= 0 or value > 18446744073709551615 for value in ids):
            raise ValueError("HOSTID muss 0..32767 sein; positive DIRIDs erforderlich.")
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                placeholders = ','.join(['%s'] * len(ids))
                cursor.execute(
                    f"SELECT dirid FROM {self.TABLE_DIRECTORIES} "
                    f"WHERE hostid = %s AND dirid IN ({placeholders})",
                    (hostid, *ids),
                )
                found = {row[0] for row in cursor.fetchall()}
                missing = set(ids) - found
                if missing:
                    raise ValueError(f"Unbekannte DIRIDs für Host {hostid}: {sorted(missing)}")
                cursor.executemany(
                    "INSERT INTO tmp_highpriority (hostid, dirid) VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE dirid = VALUES(dirid)",
                    [(hostid, value) for value in ids],
                )
                connection.commit()
                return len(ids)
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()



    def delete_all_jobs(self):
        """Alle Einträge aus der Jobtabelle löschen."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                for table in self._work_tables(cursor):
                    cursor.execute(f"DELETE FROM `{table}`")
                cursor.execute(f"DELETE FROM {self.TABLE_JOBS}")
                deleted = cursor.rowcount
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()
        print(f"Jobs gelöscht: {deleted}")




    def create_remote_doubles_table(self, cursor):
        self.connection.create_remote_doubles_table(cursor)


    def drop_all_tables(self):
        """Ask once about core tables; always reset the other managed tables."""
        answer = input(
            "Auch directories, jobs, hosts und jobsrunid löschen? [yN] "
        ).strip().lower()
        delete_core = answer == "y"
        with self.connect() as connection:
            cursor = connection.cursor()
            foreign_key_checks = None
            try:
                if delete_core:
                    # Preserve legacy job_work even if its foreign key points to jobs.
                    sqlstructure.SQLStructure.select_foreign_key_checks(cursor)
                    foreign_key_checks = int(cursor.fetchone()[0])
                    sqlstructure.SQLStructure.set_foreign_key_checks(cursor, 0)
                for table in self._work_tables(cursor):
                    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                # Always discard letcheck results, even when core tables are kept.
                tables = ["tmp_remotedoubles", self.TABLE_DOUBLE_JOBS,
                          self.TABLE_EXCLUDED_NAMES, self.TABLE_LIVE_EXCLUDED_ABSDIRS]
                if delete_core:
                    tables.extend([self.TABLE_DIRECTORIES, self.TABLE_JOBS_RUNID,
                                   self.TABLE_HOSTS, self.TABLE_JOBS])
                for table in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                try:
                    if foreign_key_checks is not None:
                        sqlstructure.SQLStructure.set_foreign_key_checks(cursor, foreign_key_checks)
                finally:
                    cursor.close()
        self.init_database()
        if delete_core:
            self.init_alldirs(reset=False)
            self.create_hosts_table()
        print("Ausschluss- und Duplikattabellen neu angelegt; tmp-Tabellen gelöscht. "
              "job_work bleibt erhalten.")
        if delete_core:
            print("directories, jobs, hosts und jobsrunid wurden ebenfalls neu angelegt. "
                  "Server mit serverinit und lokale Hosts mit --setmyhostid einrichten.")
        else:
            print("directories, jobs, hosts und jobsrunid wurden nicht gelöscht.")


    def list_tables(self):
        """List available tables/views in the configured database."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.select_table_names(cursor, self.database, ordered=True)
                return [row[0] for row in cursor.fetchall()]
            finally:
                cursor.close()

    def table_preview(self, name):
        """Read at most 40 records and the full count from an existing table."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.select_named_table(cursor, self.database, name)
                table = cursor.fetchone()
                if table is None:
                    raise ValueError(f"Tabelle nicht vorhanden: {name}")
                # Identifiers cannot be bound parameters. Validate through the
                # catalog, then quote the actual table name including backticks.
                identifier = '`' + table[0].replace('`', '``') + '`'
                cursor.execute(f"SELECT COUNT(*) FROM {identifier}")
                total = cursor.fetchone()[0]
                cursor.execute(f"SELECT * FROM {identifier} LIMIT 40")
                return cursor.fetchall(), total
            finally:
                cursor.close()


    def get_job(self, jobid):
        """Einen Job zur Anzeige vor dem gezielten Löschen abfragen."""
        with self.connect() as connection:
            cursor = connection.cursor(dictionary=True)
            try:
                cursor.execute(
                    f"SELECT jobid, userhost, startdirname, task, status, stage, timecreated, last_activity, hostid "
                    f"FROM {self.TABLE_JOBS} WHERE jobid = %s",
                    (jobid,),
                )
                return cursor.fetchone()
            finally:
                cursor.close()


    def list_recent_jobs(self, days=15):
        """Return non-time fields for jobs started within the last ``days`` days."""
        now = self.utc_now
        cutoff = now - timedelta(days=days)
        with self.connect() as connection:
            cursor = connection.cursor(dictionary=True)
            try:
                cursor.execute(
                    f"""SELECT jobid, userhost, startdirname, startdirid, task,
                               status, stage, skipdirs, scanall, runid, errmsg,
                               usererror
                        FROM {self.TABLE_JOBS}
                        WHERE timestart >= %s AND timestart <= %s AND runid > 0
                        ORDER BY runid, jobid""",
                    (cutoff, now),
                )
                return cursor.fetchall()
            finally:
                cursor.close()


    def restart_stopped_job(self, jobid):
        """Einen gestoppten Job als neuen Datensatz wieder einreihen."""
        with self.connect() as connection:
            cursor = connection.cursor(dictionary=True)
            try:
                cursor.execute(
                    f"SELECT startdirname, task, status, stage, skipdirs, scanall, hostid FROM {self.TABLE_JOBS} "
                    "WHERE jobid = %s FOR UPDATE",
                    (jobid,),
                )
                old_job = cursor.fetchone()
                if old_job is None or old_job["status"] != "stopped":
                    connection.commit()
                    return None

                hostid = int(old_job.get("hostid", 0))
                task = old_job["task"]
                stage = old_job["stage"]
                initial_status = "insert" if stage == "coarse" else "pending"
                if initial_status is None:
                    raise ValueError(
                        f"Unbekannter Jobtask für Neustart: {task}"
                    )
                now = self.utc_now
                cursor.execute(
                    f"""INSERT INTO {self.TABLE_JOBS} (
                        userhost, hostid, startdirname, task, status, stage, skipdirs, scanall,
                        timecreated, timeupdated, last_activity
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        socket.gethostname(), old_job.get("hostid", 0), old_job["startdirname"], task,
                        initial_status, stage, old_job.get("skipdirs") or "",
                        old_job.get("scanall") or 0, now, now, now,
                    ),
                )
                new_jobid = cursor.lastrowid
                cursor.execute(
                    f"""INSERT INTO {self.work_table_name(hostid)} (
                        jobid, stage, hostid, path_hash, path, parent_path, child_count, state
                    )
                    SELECT %s, stage, hostid, path_hash, path, parent_path, child_count,
                           CASE WHEN state = 'active' THEN 'pending' ELSE state END
                    FROM {self.work_table_name(hostid)} WHERE jobid = %s""",
                    (new_jobid, jobid),
                )
                if task == "binarycheck":
                    cursor.execute(f"""
                        INSERT INTO {self.TABLE_DOUBLE_JOBS}
                            (jobid, hostid, hostidB, left_dirid, right_dirid, status, verification_method,
                             source_left_dirid, source_right_dirid, finished_at, error_message,
                             is_requested, runid)
                        SELECT %s, hostid, hostidB, left_dirid, right_dirid, status, verification_method,
                               source_left_dirid, source_right_dirid, finished_at, error_message,
                               is_requested, runid
                        FROM {self.TABLE_DOUBLE_JOBS} WHERE jobid = %s
                    """, (new_jobid, jobid))
                connection.commit()
                return {
                    "jobid": new_jobid,
                    "task": task,
                    "status": initial_status,
                    "stage": stage,
                }
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()


    def remove_unused_jobs(self, jobids):
        """Recheck approved IDs and remove only still-unused jobs and their work queues."""
        approved={int(jobid) for jobid in jobids}
        if not approved:
            return 0
        with self.connect() as connection:
            cursor=connection.cursor()
            try:
                eligible={row['jobid'] for row in self.list_unused_jobs(cursor=cursor,for_update=True)} & approved
                tables=self._work_tables(cursor)
                self.select_table_count(cursor,self.database,self.TABLE_MODIFIED)
                has_report=bool(cursor.fetchone()[0])
                deleted=0
                for jobid in sorted(eligible):
                    for table in tables:
                        cursor.execute(f"DELETE FROM `{table}` WHERE jobid=%s",(jobid,))
                    if has_report:
                        cursor.execute(f"DELETE FROM {self.TABLE_MODIFIED} WHERE batch_jobid=%s",(jobid,))
                    cursor.execute(f"DELETE FROM {self.TABLE_JOBS} WHERE jobid=%s",(jobid,))
                    deleted+=cursor.rowcount
                connection.commit()
                return deleted
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                self._close_preserving_error(cursor)

    def delete_job(self, jobid):
        """Einen einzelnen Jobdatensatz löschen und Erfolg zurückgeben."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                for table in self._work_tables(cursor):
                    cursor.execute(f"DELETE FROM `{table}` WHERE jobid = %s", (jobid,))
                cursor.execute(
                    f"DELETE FROM {self.TABLE_JOBS} WHERE jobid = %s", (jobid,)
                )
                deleted = cursor.rowcount > 0
                connection.commit()
                return deleted
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()


    def list_directory_roots(self):
        """Root-Pfade und die Zahl ihrer Verzeichniseinträge abfragen."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT root_path, COUNT(*) FROM {self.TABLE_DIRECTORIES} "
                    "GROUP BY root_path ORDER BY root_path"
                )
                return cursor.fetchall()
            finally:
                cursor.close()


    def add_excluded_names(self, names):
        """Create the shared table if needed and add names without duplicates."""
        if any(os.path.isabs(name) for name in names):
            raise ValueError("Absolute Pfade gehören in live_excluded_absdirs.")
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                self.create_excluded_names_table(cursor)
                if names:
                    cursor.executemany(
                        f"INSERT IGNORE INTO {self.TABLE_EXCLUDED_NAMES} (name) VALUES (%s)",
                        [(name,) for name in names],
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()


    def add_live_excluded_absdirs(self, paths, hostid=0):
        """Store absolute paths for one host without resolving them on the client."""
        paths = list(dict.fromkeys(os.path.normpath(path) for path in paths))
        if any(not os.path.isabs(path) or "," in path for path in paths):
            raise ValueError("Live-Ausschlüsse müssen absolute Pfade ohne Komma sein.")
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                self.create_live_excluded_absdirs_table(cursor)
                cursor.executemany(
                    f"INSERT IGNORE INTO {self.TABLE_LIVE_EXCLUDED_ABSDIRS} "
                    "(hostid, path_hash, path) VALUES (%s, %s, %s)",
                    [(int(hostid), _path_hash(path), path) for path in paths],
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()


    def delete_live_excluded_absdir(self, hostid, path):
        """Delete exactly one absolute exclusion belonging to the selected host."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"DELETE FROM {self.TABLE_LIVE_EXCLUDED_ABSDIRS} "
                    "WHERE hostid = %s AND path_hash = %s AND BINARY path = BINARY %s",
                    (int(hostid), _path_hash(path), path),
                )
                deleted = cursor.rowcount
                connection.commit()
                return deleted
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()


    def erase_live_excluded_absdirs(self, hostid=0):
        """Clear absolute exclusions for one host, preserving the table."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.select_table_count(cursor, self.database, self.TABLE_LIVE_EXCLUDED_ABSDIRS)
                if cursor.fetchone()[0]:
                    cursor.execute(
                        f"DELETE FROM {self.TABLE_LIVE_EXCLUDED_ABSDIRS} WHERE hostid = %s",
                        (int(hostid),),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()


    def list_excluded_names(self):
        """Return stored names, or None when excluded_names has not been set up."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.select_table_count(cursor, self.database, self.TABLE_EXCLUDED_NAMES)
                if cursor.fetchone()[0] == 0:
                    return None
                cursor.execute(
                    f"SELECT name FROM {self.TABLE_EXCLUDED_NAMES} ORDER BY BINARY name"
                )
                return [row[0] for row in cursor.fetchall()]
            finally:
                cursor.close()


    def erase_excluded_names(self):
        """Drop the global excluded_names table."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"DROP TABLE IF EXISTS {self.TABLE_EXCLUDED_NAMES}"
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()


    def delete_directory_root(self, root_path):
        """Alle Directory-Datensätze eines Root-Pfads löschen."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"DELETE FROM {self.TABLE_DIRECTORIES} WHERE root_path = %s",
                    (root_path,),
                )
                cursor.execute(
                    f"SELECT COUNT(*) FROM {self.TABLE_DIRECTORIES}"
                )
                remaining = cursor.fetchone()[0]
                connection.commit()
                return remaining
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()


    def purge_directories_from_exclusion_lists(self, hostid=None):
        """Allow three times the configured connection, I/O and SQL time for purging."""
        previous = self.__dict__.get('_timeout_multiplier', 1)
        self._timeout_multiplier = previous * 3
        try:
            return self._purge_directories_from_exclusion_lists(hostid=hostid)
        finally:
            self._timeout_multiplier = previous

    PURGE_BATCH_SIZE = 500

    def _purge_directories_from_exclusion_lists(self, hostid=None):
        """Delete inventory paths excluded by names or host-specific absolute paths.

        Names match whole path components case-insensitively. Absolute paths are
        literal, case-sensitive subtree prefixes. Process hosts sequentially in
        bounded primary-key batches, including nonmatching rows so even sparse
        matches never require an unbounded exclusion query. No files are touched.
        """
        if hostid is not None:
            hostid = int(hostid)
            if not 0 <= hostid <= 32767:
                raise ValueError("Host-ID muss zwischen 0 und 32767 liegen.")
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                predicates = []
                for table in (self.TABLE_EXCLUDED_NAMES, self.TABLE_LIVE_EXCLUDED_ABSDIRS):
                    self.select_table_count(cursor, self.database, table)
                    if cursor.fetchone()[0] == 0:
                        continue
                    if table == self.TABLE_EXCLUDED_NAMES:
                        predicates.append(f"""EXISTS (
                            SELECT 1 FROM {table} e WHERE e.name <> ''
                            AND LEFT(e.name, 1) <> '/'
                            AND LOCATE(CONCAT('/', LOWER(e.name), '/'),
                                CONCAT('/', LOWER(TRIM(BOTH '/' FROM d.path)), '/')) > 0
                        )""")
                    else:
                        predicates.append(f"""EXISTS (
                            SELECT 1 FROM {table} e WHERE e.hostid = d.hostid
                            AND LEFT(e.path, 1) = '/' AND (
                                BINARY d.path = BINARY TRIM(TRAILING '/' FROM e.path)
                                OR BINARY LEFT(d.path, CHAR_LENGTH(TRIM(TRAILING '/' FROM e.path)) + 1)
                                   = BINARY CONCAT(TRIM(TRAILING '/' FROM e.path), '/')
                            )
                        )""")
                if not predicates:
                    return 0
                hosts = ([hostid] if hostid is not None
                         else self.list_directory_hostids(cursor=cursor, include_jobs=False))
                statement = (f"DELETE d FROM {self.TABLE_DIRECTORIES} d WHERE ("
                             + ' OR '.join(predicates) + ') AND d.hostid = %s')
                deleted = 0
                for selected in hosts:
                    print(f"Host {selected}: Ausschlüsse prüfen (je {self.PURGE_BATCH_SIZE} Datensätze).",
                          flush=True)
                    last_id = 0
                    host_deleted = 0
                    while True:
                        # Select an unfiltered ID window first: LIMIT on matching
                        # rows could still evaluate exclusions for the whole host.
                        cursor.execute(
                            f"SELECT dirid FROM {self.TABLE_DIRECTORIES} "
                            "WHERE hostid = %s AND dirid > %s ORDER BY dirid LIMIT %s",
                            (selected, last_id, self.PURGE_BATCH_SIZE))
                        ids = tuple(row[0] for row in cursor.fetchall())
                        if not ids:
                            connection.commit()
                            break
                        placeholders = ','.join(['%s'] * len(ids))
                        cursor.execute(statement + f' AND d.dirid IN ({placeholders})',
                                       (selected,) + ids)
                        affected = cursor.rowcount
                        connection.commit()
                        # Never retry an uncertain commit: report only confirmed
                        # counts; rerunning the command safely processes remaining IDs.
                        host_deleted += affected
                        deleted += affected
                        last_id = ids[-1]
                        print(f"Host {selected}: bis DIRID {last_id} geprüft; "
                              f"{host_deleted} gelöscht.", flush=True)
                    print(f"Host {selected}: {host_deleted} ausgeschlossene Directory-Datensätze gelöscht.",
                          flush=True)
                return deleted
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                self._close_preserving_error(cursor)

    def live_excluded_directories(self, hostid=None, delete=False):
        """Count or delete excluded inventory rows, respecting host/path boundaries."""
        predicate = (
            f"EXISTS (SELECT 1 FROM {self.TABLE_LIVE_EXCLUDED_ABSDIRS} e "
            "WHERE e.hostid = d.hostid AND LEFT(e.path, 1) = '/' AND ("
            "BINARY d.path = BINARY TRIM(TRAILING '/' FROM e.path) OR "
            "BINARY LEFT(d.path, CHAR_LENGTH(TRIM(TRAILING '/' FROM e.path)) + 1) "
            "= BINARY CONCAT(TRIM(TRAILING '/' FROM e.path), '/')))"
        )
        parameters = ()
        if hostid is not None:
            predicate += " AND d.hostid = %s"
            parameters = (int(hostid),)
        if not delete:
            return self._live_excluded_read(
                f"SELECT COUNT(*) FROM {self.TABLE_DIRECTORIES} d WHERE " + predicate,
                parameters, count=True,
            )
        deleted = 0
        last_id = 0
        while True:
            rows = self._live_excluded_read(
                f"SELECT d.dirid FROM {self.TABLE_DIRECTORIES} d WHERE " + predicate
                + " AND d.dirid > %s ORDER BY d.dirid LIMIT 3000",
                parameters + (last_id,),
            )
            if not rows:
                return deleted
            ids = tuple(row[0] for row in rows)
            placeholders = ','.join(['%s'] * len(ids))
            sql = (f"DELETE d FROM {self.TABLE_DIRECTORIES} d WHERE " + predicate
                   + f" AND d.dirid IN ({placeholders})")
            # Keep the same IDs after a lost COMMIT response: replay cannot
            # delete another batch, and already committed rows are absent.
            affected = 0
            for attempt in range(3):
                try:
                    with self.connect() as connection:
                        cursor = connection.cursor()
                        try:
                            cursor.execute(sql, parameters + ids)
                            affected = max(affected, cursor.rowcount)
                            connection.commit()
                        except Exception:
                            self._rollback_preserving_error(connection)
                            raise
                        finally:
                            self._close_preserving_error(cursor)
                    break
                except Exception as error:
                    if not self._live_excluded_retryable(error) or attempt == 2:
                        raise
                    time.sleep(1)
            deleted += affected
            last_id = ids[-1]
            print(f"{deleted} Directory-Einträge gelöscht (Zwischenstand).", flush=True)

    @staticmethod
    def _live_excluded_retryable(error):
        return is_network_error(error) or getattr(error, 'errno', None) in (1205, 1213)

    def _live_excluded_read(self, sql, parameters, count=False):
        for attempt in range(3):
            try:
                with self.connect() as connection:
                    cursor = connection.cursor()
                    try:
                        cursor.execute(sql, parameters)
                        return cursor.fetchone()[0] if count else cursor.fetchall()
                    finally:
                        self._close_preserving_error(cursor)
            except Exception as error:
                if not self._live_excluded_retryable(error) or attempt == 2:
                    raise
                time.sleep(1)


    def directory_summary_data(self):
        """Count directory rows per host/run, including run creation time."""
        with self.connect() as connection:
            cursor = connection.cursor(dictionary=True)
            try:
                cursor.execute(
                    "SELECT d.runid, d.hostid, COUNT(*) AS directory_count, "
                    "SUM(d.localfilesize IS NULL OR d.localfilecount IS NULL "
                    "OR d.subdircount IS NULL OR d.otherentrycount IS NULL) AS incomplete_count, "
                    "SUM(d.files_sha256 IS NULL OR d.subdirs_sha256 IS NULL "
                    "OR d.combined_sha256 IS NULL) AS missing_hash_count, "
                    "MAX(EXISTS(SELECT 1 FROM " + self.TABLE_JOBS + " j "
                    "WHERE j.hostid = d.hostid AND j.status = 'active' "
                    "AND (j.runid = d.runid OR j.jobid = r.jobid))) AS active_job, "
                    "r.timecreated AS run_timestamp, r.jobid AS jobid "
                    f"FROM {self.TABLE_DIRECTORIES} d "
                    f"LEFT JOIN {self.TABLE_JOBS_RUNID} r ON r.runid = d.runid "
                    "GROUP BY d.runid, d.hostid, r.timecreated, r.jobid "
                    "ORDER BY d.runid DESC, d.hostid ASC"
                )
                return cursor.fetchall()
            finally:
                cursor.close()


    def list_directory_runids(self):
        """Runids und getrennte Trefferzahlen aus runid und finished liefern."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                counts = {}
                for field in ("runid", "finished"):
                    cursor.execute(
                        f"SELECT {field}, COUNT(*) FROM {self.TABLE_DIRECTORIES} "
                        f"GROUP BY {field}"
                    )
                    for runid, count in cursor.fetchall():
                        counts.setdefault(runid, {"runid": 0, "finished": 0})
                        counts[runid][field] = count
                return [
                    (runid, counts[runid]["runid"], counts[runid]["finished"])
                    for runid in sorted(counts, key=lambda value: (value is None, value))
                ]
            finally:
                cursor.close()


    def list_duplicate_subtree_roots(self):
        """Return duplicate subtree roots, omitting duplicates below duplicates."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"""WITH RECURSIVE duplicate_groups AS (
                            SELECT combined_sha256, subtree_structure
                            FROM {self.TABLE_DIRECTORIES}
                            WHERE zero_content_only = FALSE AND combined_sha256 IS NOT NULL
                              AND subtree_structure IS NOT NULL
                            GROUP BY combined_sha256, subtree_structure
                            HAVING COUNT(*) > 1
                        ), duplicate_nodes AS (
                            SELECT d.dirid, d.parentdir, d.path,
                                   d.subtree_structure, d.subtree_size_bytes,
                                   d.combined_sha256, d.hostid
                            FROM {self.TABLE_DIRECTORIES} d
                            JOIN duplicate_groups g
                              ON g.combined_sha256 = d.combined_sha256
                             AND g.subtree_structure = d.subtree_structure
                            WHERE d.zero_content_only = FALSE
                        ), ancestors AS (
                            SELECT dirid AS node_id, parentdir AS ancestor_id, hostid
                            FROM duplicate_nodes
                            WHERE parentdir <> 0

                            UNION ALL

                            SELECT a.node_id, p.parentdir AS ancestor_id, a.hostid
                            FROM ancestors a
                            JOIN {self.TABLE_DIRECTORIES} p
                              ON p.dirid = a.ancestor_id AND p.hostid = a.hostid
                            WHERE p.parentdir <> 0
                        )
                        SELECT d.dirid, d.path, d.subtree_structure,
                               d.subtree_size_bytes, d.combined_sha256, d.hostid
                        FROM duplicate_nodes d
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM ancestors a
                            JOIN duplicate_nodes duplicate_ancestor
                              ON duplicate_ancestor.dirid = a.ancestor_id
                             AND duplicate_ancestor.hostid = a.hostid
                            WHERE a.node_id = d.dirid AND a.hostid = d.hostid
                        )
                        ORDER BY d.hostid, d.combined_sha256, d.path"""
                )
                return cursor.fetchall()
            finally:
                cursor.close()


    def get_directory_paths(self, dirids):
        """Return (dirid, full_path) pairs for the supplied directory IDs."""
        if not dirids:
            return []
        placeholders = ", ".join("%s" for _ in dirids)
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT dirid, path FROM {self.TABLE_DIRECTORIES} "
                    f"WHERE dirid IN ({placeholders}) AND zero_content_only = FALSE",
                    tuple(dirids),
                )
                return cursor.fetchall()
            finally:
                cursor.close()


    def _read_duplicate_candidates(self, hostid, hashes=None):
        """Read at most 1000 metadata records per indexed primary-key page."""
        rows = []
        last_id = 0
        while True:
            with self.connect() as connection:
                cursor = connection.cursor()
                try:
                    params = (hostid, last_id)
                    hash_filter = ''
                    if hashes is not None:
                        hash_filter = ' AND combined_sha256 IN (' + ','.join(['%s'] * len(hashes)) + ')'
                        params += tuple(hashes)
                    cursor.execute(
                        f"SELECT dirid, path, subtree_structure, subtree_size_bytes, combined_sha256, hostid "
                        f"FROM {self.TABLE_DIRECTORIES} WHERE hostid = %s AND dirid > %s "
                        "AND zero_content_only = FALSE AND combined_sha256 IS NOT NULL "
                        "AND files_sha256 IS NOT NULL AND subdirs_sha256 IS NOT NULL "
                        "AND subtree_structure IS NOT NULL" + hash_filter + " ORDER BY dirid LIMIT 1000",
                        params,
                    )
                    batch = cursor.fetchall()
                finally:
                    cursor.close()
            rows.extend(batch)
            if len(batch) < 1000:
                return rows
            last_id = batch[-1][0]

    def list_scoped_duplicate_roots(self, hosts):
        """Compare one host, or one pair, without a global recursive SQL query."""
        hosts = tuple(hosts)
        cache = self._candidate_host_rows
        if len(hosts) == 1:
            rows = self._read_duplicate_candidates(hosts[0])
            cache[hosts[0]] = rows
        else:
            left = cache[hosts[0]]
            hashes = sorted({row[4] for row in left})
            right = []
            for offset in range(0, len(hashes), 200):
                right.extend(self._read_duplicate_candidates(hosts[1], hashes[offset:offset + 200]))
            rows = left + right
        groups = {}
        for row in rows:
            groups.setdefault((row[4], row[2]), []).append(row)
        candidates = [row for group in groups.values()
                      if (len(group) > 1 if len(hosts) == 1 else len({item[5] for item in group}) == 2)
                      for row in group]
        paths = {(row[5], os.path.normpath(row[1])) for row in candidates}
        result = []
        for row in candidates:
            path = os.path.normpath(row[1])
            parent = os.path.dirname(path)
            nested = False
            while parent != path:
                if (row[5], parent) in paths:
                    nested = True
                    break
                path, parent = parent, os.path.dirname(parent)
            if not nested:
                result.append(row)
        return result

    @staticmethod
    def _comparison_key(hostid, remote, left, right):
        return tuple(sorted(((int(hostid),int(left)),(int(remote),int(right)))))

    def _filter_binarycheck_repeats(self,cursor,comparisons,confirm_repeat=None,before_prompt=None):
        """Exclude current queued pairs and require consent for fresh completed pairs."""
        requested={self._comparison_key(host,remote,left,right):(host,(remote,left,right))
                   for host,pairs in comparisons.items() for remote,left,right in pairs}
        keys=list(requested)
        if not requested:
            return {}
        queued=set()
        completed=set()
        stamps={}
        for offset in range(0,len(keys),100):
            group=keys[offset:offset+100]
            conditions=[]
            params=[]
            for ((ha,a),(hb,b)) in group:
                conditions.append('((p.hostid=%s AND p.left_dirid=%s AND p.hostidB=%s AND p.right_dirid=%s) '
                                  'OR (p.hostid=%s AND p.left_dirid=%s AND p.hostidB=%s AND p.right_dirid=%s))')
                params.extend((ha,a,hb,b,hb,b,ha,a))
            cursor.execute(f"""SELECT p.hostid,p.hostidB,p.left_dirid,p.right_dirid,
                    p.status,p.finished_at,j.status,j.timestart,
                    a.scanned_at,a.hashed_at,b.scanned_at,b.hashed_at
                FROM {self.TABLE_DOUBLE_JOBS} p JOIN {self.TABLE_JOBS} j ON j.jobid=p.jobid
                JOIN {self.TABLE_DIRECTORIES} a ON a.hostid=p.hostid AND a.dirid=p.left_dirid
                JOIN {self.TABLE_DIRECTORIES} b ON b.hostid=p.hostidB AND b.dirid=p.right_dirid
                WHERE j.task='binarycheck' AND ("""+' OR '.join(conditions)+')',tuple(params))
            for ha,hb,a,b,status,finished,job_status,started,scan_a,hash_a,scan_b,hash_b in cursor.fetchall():
                key=self._comparison_key(ha,hb,a,b)
                if key not in requested:
                    continue
                if status in ('pending','unreadable') and job_status in ('insert','pending','active','paused'):
                    queued.add(key)
                elif (status in ('equal','different') and finished is not None and started is not None
                      and scan_a is not None and scan_b is not None
                      and max(scan_a,scan_b) <= started <= finished):
                    # Fresh historical results can initialize the newly introduced field.
                    completed.add(key)
                    for host,dirid,scan,hashed in ((ha,a,scan_a,hash_a),(hb,b,scan_b,hash_b)):
                        if hashed is None or hashed < scan:
                            stamps[(host,dirid,scan)]=max(stamps.get((host,dirid,scan),finished),finished)
        for (host,dirid,scan),stamp in stamps.items():
            cursor.execute(f"UPDATE {self.TABLE_DIRECTORIES} SET hashed_at=%s "
                           "WHERE hostid=%s AND dirid=%s AND scanned_at=%s "
                           "AND (hashed_at IS NULL OR hashed_at < %s)",
                           (stamp,host,dirid,scan,stamp))
        completed-=queued
        result={host:set() for host in comparisons}
        repeats={host:set() for host in comparisons}
        for key,(host,pair) in requested.items():
            if key in completed:
                repeats[host].add(pair)
            elif key not in queued:
                result[host].add(pair)
        for host in sorted(comparisons):
            pending=sum(1 for key in queued if requested[key][0]==host)
            print(f'Host {host}: {len(result[host])} neue, {len(repeats[host])} bereits geprüfte, '
                  f'{pending} bereits eingereihte Vergleiche.',flush=True)
        total=sum(map(len,repeats.values()))
        if total and confirm_repeat is not None and before_prompt is not None:
            before_prompt()
        if total and confirm_repeat is not None and confirm_repeat(None,total):
            for host,pairs in sorted(repeats.items()):
                if pairs and confirm_repeat(host,len(pairs)):
                    result[host].update(pairs)
        return {host:pairs for host,pairs in result.items() if pairs}

    def create_binarycheck_job(self, confirm_repeat=None):
        """Allow three times the configured time for duplicate-check preparation."""
        previous = self.__dict__.get('_timeout_multiplier', 1)
        self._timeout_multiplier = previous * 3
        try:
            return (self._create_binarycheck_job() if confirm_repeat is None
                    else self._create_binarycheck_job(confirm_repeat=confirm_repeat))
        finally:
            self._timeout_multiplier = previous

    def _create_binarycheck_job(self, confirm_repeat=None):
        """Queue bytewise checks for every duplicate subtree group."""
        comparison_skipdirs = ','.join(self.list_excluded_names() or [])
        hosts = sorted(set(self.list_directory_hostids(include_jobs=False)))
        comparisons_by_host = {hostid: set() for hostid in hosts}
        self._candidate_host_rows = {}
        scopes = [(host,) for host in hosts]
        scopes.extend((left, right) for i, left in enumerate(hosts) for right in hosts[i + 1:])
        try:
            for scope in scopes:
                print('Kandidatensuche Host ' + ' <-> '.join(map(str, scope)), flush=True)
                duplicate_rows = self.list_scoped_duplicate_roots(scope)
                groups = {}
                for row in duplicate_rows:
                    if len(row) < 6:
                        raise RuntimeError('Host-ID fehlt bei Directory-Treffer; kein Binarycheck-Job angelegt.')
                    if row[5] in scope:
                        groups.setdefault((row[4], row[2]), {}).setdefault(int(row[5]), []).append(row[0])
                for by_host in groups.values():
                    for pair_host in by_host:
                        by_host[pair_host] = sorted(set(by_host[pair_host]))
                    if len(scope) == 1:
                        for pair_host, ids in by_host.items():
                            comparisons_by_host[pair_host].update((pair_host, ids[0], other) for other in ids[1:])
                    elif all(host in by_host for host in scope):
                        leader = next((host for host in scope if host > 0), scope[0])
                        remote = next(host for host in scope if host != leader)
                        comparisons_by_host[leader].add((remote, by_host[leader][0], by_host[remote][0]))
        finally:
            self.__dict__.pop('_candidate_host_rows', None)

        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                self.create_double_jobs_table(cursor)
                self.create_remote_doubles_table(cursor)
                comparisons_by_host=self._filter_binarycheck_repeats(cursor,comparisons_by_host,confirm_repeat,
                                                                        before_prompt=connection.commit)
                if not comparisons_by_host:
                    connection.commit()
                    return None, 0

                now = self.utc_now
                created = []
                for hostid, comparisons in sorted(comparisons_by_host.items()):
                    comparisons = sorted(comparisons)
                    userhost = socket.gethostname()
                    if hostid > 0:
                        cursor.execute(f"SELECT machine FROM {self.TABLE_HOSTS} WHERE hostid = %s", (hostid,))
                        registered = cursor.fetchone()
                        if registered is not None:
                            userhost = registered[0]
                    cursor.execute(f"""
                        INSERT INTO {self.TABLE_JOBS} (
                            userhost, hostid, startdirname, task, status, stage, skipdirs,
                            scanall, timecreated, timeupdated, last_activity
                        ) VALUES (%s, %s, '<duplicate-subtrees>', 'binarycheck',
                                  'insert', 'coarse', %s, 1, %s, %s, %s)
                    """, (userhost, hostid, comparison_skipdirs, now, now, now))
                    jobid = cursor.lastrowid
                    cursor.executemany(
                        f"""INSERT INTO {self.TABLE_DOUBLE_JOBS}
                            (jobid, hostid, hostidB, left_dirid, right_dirid, status, is_requested, runid)
                            VALUES (%s, %s, %s, %s, %s, 'pending', TRUE, 0)""",
                        [(jobid, hostid, remotehost, dirid, candidate)
                         for remotehost, dirid, candidate in comparisons],
                    )
                    created.append((jobid, len(comparisons)))
                connection.commit()
                return created[0] if len(created) == 1 else created
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()


    @staticmethod
    def _deep_double_levels(nodes, final_hashes):
        """Match candidate pairs across hosts and ascend through each host tree.

        nodes contains (dirid, parentdir, hostid, combined_sha256, path).
        A visited pair prevents corrupt parent cycles from looping forever.
        Depth zero denotes a matching candidate with no matching parent pair.
        """
        from itertools import combinations

        by_id = {(row[2], row[0]): row for row in nodes}
        groups = {}
        for row in nodes:
            if row[3] is not None and (row[2], row[3]) not in final_hashes:
                groups.setdefault(row[3], []).append(row)
        result = []
        for _hash, group in sorted(groups.items()):
            for left, right in combinations(sorted(group), 2):
                current_left, current_right = left, right
                seen = {tuple(sorted(((left[2], left[0]), (right[2], right[0]))))}
                depth = 0
                while True:
                    parent_left = by_id.get((current_left[2], current_left[1]))
                    parent_right = by_id.get((current_right[2], current_right[1]))
                    if (parent_left is None or parent_right is None
                            or (parent_left[2], parent_left[0]) == (parent_right[2], parent_right[0])
                            or parent_left[3] is None
                            or parent_left[3] != parent_right[3]):
                        break
                    pair = tuple(sorted(((parent_left[2], parent_left[0]),
                                         (parent_right[2], parent_right[0]))))
                    if pair in seen:
                        break
                    seen.add(pair)
                    depth += 1
                    current_left, current_right = parent_left, parent_right
                result.append((left[2], left[0], right[2], right[0],
                               depth, left[4], right[4]))
        return result

    def list_deep_double_levels(self, jobid=None, *, persist=True, all_jobs=False):
        """Analyze the available positive results, including incomplete batches.

        The requested (or newest) batch is identified by the shared creation
        timestamp used by create_binarycheck_job, including every host. Temporary
        tables are private to this connection; existing analyses are untouched.
        Results are saved per batch in tmp_deepdoubles_result for tree reports.
        Hash matches are candidates, not additional bytewise verification.
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                job_filter = " AND jobid = %s" if jobid is not None else ""
                params = (jobid,) if jobid is not None and not all_jobs else ()
                batch_filter = (f"AND j.timecreated = (SELECT timecreated FROM {self.TABLE_JOBS} "
                                f"WHERE task = 'binarycheck'{job_filter} ORDER BY jobid DESC LIMIT 1)"
                                if not all_jobs else '')
                cursor.execute(f"""
                    SELECT j.jobid, j.hostid, j.status, COUNT(dj.left_dirid),
                           COALESCE(SUM(CASE WHEN dj.status IN ('equal', 'different', 'skipped_empty') THEN 1 ELSE 0 END), 0)
                    FROM {self.TABLE_JOBS} j
                    LEFT JOIN {self.TABLE_DOUBLE_JOBS} dj
                      ON dj.jobid = j.jobid AND dj.hostid = j.hostid AND dj.is_requested = TRUE
                    WHERE j.task = 'binarycheck' {batch_filter}
                    GROUP BY j.jobid, j.hostid, j.status
                    ORDER BY j.jobid
                """, params)
                jobs = cursor.fetchall()
                if not jobs:
                    raise RuntimeError("Kein Binarycheck-Batch vorhanden; zuerst --doubles-letcheck ausführen.")
                self.deep_double_analysis_summary = (
                    "Auswertung vorhandener positiver Vergleichsergebnisse. "
                    + "; ".join(
                        f"Job {check_jobid}, Host {check_hostid}: {completed}/{total} "
                        f"angeforderte Vergleiche abgeschlossen (Status {check_status})"
                        for check_jobid, check_hostid, check_status, total, completed in jobs
                    )
                    + ". Nicht bestätigte Bereiche werden ausgelassen; die Liste kann unvollständig sein."
                )
                jobids = tuple(row[0] for row in jobs)
                placeholders = ','.join(['%s'] * len(jobids))
                cursor.execute(f"""
                    SELECT DISTINCT d.hostid, d.combined_sha256
                    FROM {self.TABLE_DOUBLE_JOBS} dj
                    JOIN {self.TABLE_DIRECTORIES} d
                      ON ((d.hostid = dj.hostid AND d.dirid = dj.left_dirid)
                          OR (d.hostid = dj.hostidB AND d.dirid = dj.right_dirid))
                    WHERE dj.jobid IN ({placeholders}) AND dj.status = 'equal'
                      AND dj.is_requested = TRUE
                      AND d.combined_sha256 IS NOT NULL
                """, jobids)
                final_hashes = set(cursor.fetchall())
                cursor.execute(f"""
                    SELECT d.dirid, d.parentdir, d.hostid, d.combined_sha256, d.path
                    FROM {self.TABLE_DIRECTORIES} d
                    JOIN (
                        SELECT hostid, left_dirid AS dirid
                        FROM {self.TABLE_DOUBLE_JOBS}
                        WHERE jobid IN ({placeholders}) AND status = 'equal'
                        UNION
                        SELECT hostidB AS hostid, right_dirid AS dirid
                        FROM {self.TABLE_DOUBLE_JOBS}
                        WHERE jobid IN ({placeholders}) AND status = 'equal'
                    ) verified ON verified.hostid = d.hostid AND verified.dirid = d.dirid
                    WHERE d.zero_content_only = FALSE
                    ORDER BY d.hostid, d.dirid
                """, jobids + jobids)
                nodes = cursor.fetchall()
                if persist:
                    # Only positively verified directories enter the partial analysis.
                    sqlstructure.SQLStructure.create_deep_double_candidates_table(cursor)
                    candidates = [row[:4] for row in nodes if row[3] is not None
                                  and (row[2], row[3]) not in final_hashes]
                    if candidates:
                        cursor.executemany("""
                            INSERT INTO tmp_deepdoubles_candidates
                            (dirid, parentdirid, hostid, combined_sha256)
                            VALUES (%s, %s, %s, %s)
                        """, candidates)
                result = self._deep_double_levels(nodes, final_hashes)
                connection.rollback()  # Release the read snapshot before saving results.
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

        if persist:
            self._store_deep_double_results(min(jobids), nodes, result)
        return result

    def _store_deep_double_results(self, batch_jobid, nodes, pairs):
        """Atomically replace one batch's per-directory levels; retain other batches."""
        by_id = {(row[2], row[0]): row for row in nodes}
        levels = {}
        for left_host, left_id, right_host, right_id, level, *_paths in pairs:
            for key in ((left_host, left_id), (right_host, right_id)):
                levels[key] = max(levels.get(key, 0), level)
        rows = [(batch_jobid, hostid, dirid, by_id[(hostid, dirid)][3], level)
                for (hostid, dirid), level in sorted(levels.items())]
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.create_deep_double_results_table(cursor)
                # Serialize report rebuilds for this batch, including an empty result.
                cursor.execute(f"SELECT jobid FROM {self.TABLE_JOBS} WHERE jobid = %s FOR UPDATE",
                               (batch_jobid,))
                if cursor.fetchone() is None:
                    raise RuntimeError("Binarycheck-Batch wurde inzwischen gelöscht.")
                cursor.execute("DELETE FROM tmp_deepdoubles_result WHERE jobid = %s",
                               (batch_jobid,))
                if rows:
                    cursor.executemany("""
                        INSERT INTO tmp_deepdoubles_result
                        (jobid, hostid, dirid, combined_sha256, level)
                        VALUES (%s, %s, %s, %s, %s)
                    """, rows)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

    def list_doubles_tree_snapshot_rows(self, jobid):
        pairs = (self.list_deep_double_levels(persist=False, all_jobs=True) if jobid is None
                 else self.list_deep_double_levels(jobid, persist=False))
        keys = {(host, dirid) for left_host, left, right_host, right, level, *_ in pairs
                if level > 0 for host, dirid in ((left_host, left), (right_host, right))}
        if not keys:
            # Remote verification can prove a root without persisting every child.
            # Keep those confirmed cross-host roots visible in the snapshot.
            batch = self.get_double_job_batch(jobid)
            return [row for batch_jobid in ([item[0] for item in batch] or [jobid])
                    for row in self.list_verified_double_pairs(batch_jobid)]
        ids = sorted({dirid for _, dirid in keys})
        result = []
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                for offset in range(0, len(ids), 1000):
                    batch = ids[offset:offset + 1000]
                    placeholders = ','.join(['%s'] * len(batch))
                    cursor.execute(
                        "SELECT dirid, path, combined_sha256, subtree_structure, subtree_size_bytes, "
                        f"localfilecount, subdircount, hostid FROM {self.TABLE_DIRECTORIES} "
                        f"WHERE dirid IN ({placeholders}) AND zero_content_only = FALSE "
                        "AND combined_sha256 IS NOT NULL ORDER BY hostid, dirid", tuple(batch))
                    for dirid, path, hash_, structure, size, files, dirs, host in cursor.fetchall():
                        if (host, dirid) in keys:
                            result.append((dirid, path, hash_, structure, dirid, path, size, size,
                                           files, dirs, files, dirs, host))
                batch = self.get_double_job_batch(jobid)
                verified = [row for batch_jobid in ([item[0] for item in batch] or [jobid])
                            for row in self.list_verified_double_pairs(batch_jobid)]
                return result + verified
            finally:
                cursor.close()

    def list_deep_double_tree_rows(self, jobid):
        """Build results and return every directory with a positive parent level.

        Each directory occupies both sides of the existing pair-row format;
        renderers deduplicate these sides and group all members by their hash.
        """
        self.list_deep_double_levels(jobid)
        batch = self.get_double_job_batch(jobid)
        batch_jobid = min(row[0] for row in batch) if batch else jobid
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(f"""
                    SELECT d.dirid, d.path, r.combined_sha256, d.subtree_structure,
                           d.dirid, d.path, d.subtree_size_bytes, d.subtree_size_bytes,
                           d.localfilecount, d.subdircount, d.localfilecount, d.subdircount,
                           d.hostid
                    FROM tmp_deepdoubles_result r
                    JOIN {self.TABLE_DIRECTORIES} d
                      ON d.hostid = r.hostid AND d.dirid = r.dirid
                     AND d.combined_sha256 = r.combined_sha256
                    WHERE r.jobid = %s AND r.level > 0 AND d.zero_content_only = FALSE
                    ORDER BY r.combined_sha256, d.dirid
                """, (batch_jobid,))
                return cursor.fetchall()
            finally:
                cursor.close()


    def list_double_jobs(self):
        """Return the stored state of bytewise duplicate comparisons."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.select_table_count(cursor, self.database, "tmp_doubles_letchecked")
                if cursor.fetchone()[0] == 0:
                    return None
                cursor.execute(f"""
                    SELECT jobid, hostid, left_dirid, right_dirid, status, verification_method,
                           source_left_dirid, source_right_dirid, finished_at, error_message,
                           runid, is_requested, hostidB
                    FROM {self.TABLE_DOUBLE_JOBS}
                    ORDER BY jobid, hostid, left_dirid, right_dirid
                """)
                rows = cursor.fetchall()
                empty = self.zero_content_directory_ids() if rows else set()
                return [row for row in rows if (row[1], row[2]) not in empty and (row[12], row[3]) not in empty]
            finally:
                cursor.close()


    def get_double_job_progress(self, jobid=None, hostid=None):
        """Return the newest matching job or a specified job scoped to a host."""
        if hostid is not None:
            hostid = int(hostid)
            if not 0 <= hostid <= 32767:
                raise ValueError('HOSTID muss zwischen 0 und 32767 liegen.')
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                job_filter = " AND jobid = %s" if jobid is not None else ""
                params = (jobid,) if jobid is not None else ()
                host_filter = " AND hostid = %s" if hostid is not None else ""
                if hostid is not None:
                    params += (hostid,)
                cursor.execute(
                    f"SELECT jobid, status FROM {self.TABLE_JOBS} "
                    f"WHERE task = 'binarycheck'{job_filter}{host_filter} "
                    "ORDER BY jobid DESC LIMIT 1",
                    params,
                )
                job = cursor.fetchone()
                if job is None:
                    return None
                jobid, status = job
                cursor.execute(f"""
                    SELECT COUNT(*), COALESCE(SUM(status IN ('equal', 'different', 'skipped_empty')), 0)
                    FROM {self.TABLE_DOUBLE_JOBS} WHERE jobid = %s AND is_requested = TRUE
                    {host_filter}
                """, (jobid,) + ((hostid,) if hostid is not None else ()))
                total, checked = cursor.fetchone()
                return jobid, status, total, checked
            finally:
                cursor.close()


    def get_double_job_batch(self, jobid):
        """Return binarycheck jobs created together, partitioned by hostid."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                if jobid is None:
                    cursor.execute(f"SELECT jobid, hostid FROM {self.TABLE_JOBS} "
                                   "WHERE task = 'binarycheck' ORDER BY jobid")
                    return cursor.fetchall()
                cursor.execute(
                    f"SELECT timecreated, hostid FROM {self.TABLE_JOBS} "
                    "WHERE jobid = %s AND task = 'binarycheck'",
                    (jobid,),
                )
                created = cursor.fetchone()
                if created is None:
                    return []
                cursor.execute(
                    f"""SELECT j.jobid, j.hostid
                        FROM {self.TABLE_JOBS} j
                        WHERE j.task = 'binarycheck' AND j.timecreated = %s
                        GROUP BY j.jobid, j.hostid
                        ORDER BY j.hostid, j.jobid""",
                    (created[0],),
                )
                jobs = cursor.fetchall()
                return jobs or [(jobid, int(created[1]))]
            finally:
                cursor.close()


    def list_verified_double_pairs(self, jobid):
        """Return checked pairs that were confirmed byte-for-byte equal."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(f"""
                    SELECT dj.left_dirid, left_dir.path, left_dir.combined_sha256,
                           left_dir.subtree_structure, dj.right_dirid,
                           right_dir.path, left_dir.subtree_size_bytes,
                           right_dir.subtree_size_bytes,
                           left_dir.localfilecount, left_dir.subdircount,
                           right_dir.localfilecount, right_dir.subdircount,
                           dj.hostid, dj.hostidB
                    FROM {self.TABLE_DOUBLE_JOBS} dj
                    JOIN {self.TABLE_DIRECTORIES} left_dir
                      ON left_dir.hostid = dj.hostid AND left_dir.dirid = dj.left_dirid
                    JOIN {self.TABLE_DIRECTORIES} right_dir
                      ON right_dir.hostid = dj.hostidB
                     AND right_dir.dirid = dj.right_dirid
                    WHERE dj.jobid = %s AND dj.status = 'equal'
                      AND left_dir.zero_content_only = FALSE
                      AND right_dir.zero_content_only = FALSE
                      AND dj.is_requested = TRUE
                    ORDER BY left_dir.combined_sha256, dj.left_dirid, dj.right_dirid
                """, (jobid,))
                return cursor.fetchall()
            finally:
                cursor.close()


    def count_directories_for_runid(self, runid):
        """Anzahl der Zeilen, in denen runid in einem der beiden Felder steht."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {self.TABLE_DIRECTORIES} "
                    "WHERE runid = %s OR finished = %s",
                    (runid, runid),
                )
                return cursor.fetchone()[0]
            finally:
                cursor.close()


    def delete_directories_for_runid(self, runid):
        """Runid-markierte Zeilen löschen und die verbleibende Tabellenzahl liefern."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"DELETE FROM {self.TABLE_DIRECTORIES} "
                    "WHERE runid = %s OR finished = %s",
                    (runid, runid),
                )
                cursor.execute(f"SELECT COUNT(*) FROM {self.TABLE_DIRECTORIES}")
                remaining = cursor.fetchone()[0]
                connection.commit()
                return remaining
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()


    def insert_job(self, path, skipdirs="", scanall=0, hostid=0, userhost=None):
        """Verzeichnis als neuen Scan-Job mit Status insert eintragen."""
        skipdirs = ",".join(
            dict.fromkeys(part.strip() for part in (skipdirs or "").split(",") if part.strip())
        )
        now = self.utc_now
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"""
                    INSERT INTO {DatabaseConnection.TABLE_JOBS} (
                        userhost, hostid, startdirname, task, status, stage, skipdirs, scanall,
                        timecreated, timeupdated, last_activity
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (userhost or socket.gethostname(), int(hostid), path, "scan", "insert", "coarse", skipdirs, int(bool(scanall)),
                     now, now, now),
                )
                jobid = cursor.lastrowid
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()
            print(f"Verzeichnis wurde als Job {jobid} eingetragen: {path}")
            return jobid


    def list_jobs(self, active_only=False, status=None, reset_error_display=False):
        """Alle Jobs oder Jobs mit einem bestimmten Status anzeigen."""
        if reset_error_display:
            columns = [
                "jobid", "hostid", "userhost", "startdirname", "task", "status",
                "stage", "scanall", "runid", "timecreated", "timestart", "timeend",
                "timeupdated", "last_activity", "errmsg", "usererror",
            ]
        else:
            columns = [
                "jobid", "userhost", "startdirname", "startdirid", "task", "status",
                "stage", "skipdirs", "scanall", "runid", "timecreated", "timestart",
                "timeend", "timeupdated", "last_activity", "errmsg", "usererror",
            ]
        with self.connect() as connection:
            cursor = connection.cursor(dictionary=True)
            try:
                sql = (
                    f"SELECT {', '.join(columns)} "
                    f"FROM {DatabaseConnection.TABLE_JOBS}"
                )
                if status is not None:
                    sql += " WHERE status = %s"
                    params = (status,)
                elif active_only:
                    sql += " WHERE status = %s"
                    params = ("active",)
                else:
                    params = ()
                sql += " ORDER BY jobid"
                cursor.execute(sql, params)
                rows = cursor.fetchall()
            finally:
                cursor.close()

            if not rows:
                print("Keine passenden Jobs vorhanden.")
                return []

            widths = {
                "jobid": 6, "hostid": 7, "userhost": 24, "startdirname": 32,
                "startdirid": 10, "task": 8, "status": 10, "stage": 10, "skipdirs": 28,
                "scanall": 7, "runid": 9,
                "timecreated": 21, "timestart": 21,
                "timeend": 21, "timeupdated": 21, "last_activity": 21,
                "errmsg": 36, "usererror": 56,
            }

            def format_value(value, width):
                if value is None:
                    value = "-"
                elif hasattr(value, "strftime"):
                    value = value.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    value = str(value)
                if len(value) > width:
                    value = value[:width - 3] + "..."
                return value.ljust(width)

            header = " | ".join(format_value(col, widths[col]) for col in columns)
            print(header)
            print("-" * len(header))
            for row in rows:
                print(" | ".join(format_value(row[col], widths[col]) for col in columns))
            print()
            print(f"Anzahl Jobs: {len(rows)}")
            return rows


    def change_job_status(self, jobid, action):
        """stop jobs; restart stopped jobs in a new row and resume paused rows."""
        if action == "start":
            new_job = self.restart_stopped_job(jobid)
            if new_job is not None:
                print(
                    f"Job {jobid} bleibt unverändert; neuer Job {new_job['jobid']} "
                    f"({new_job['task']}, {new_job['status']}) wurde angelegt."
                )
                return

        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                if action == "stop":
                    sql = f"""
                        UPDATE {DatabaseConnection.TABLE_JOBS} SET status = 'stopped', timeupdated = %s
                        WHERE jobid = %s AND status IN ('insert', 'pending', 'active', 'paused')
                    """
                    params = (self.utc_now, jobid)
                elif action == "start":
                    sql = f"""
                        UPDATE {DatabaseConnection.TABLE_JOBS}
                        SET status = IF(stage = 'coarse', 'insert', 'pending'),
                            timeupdated = %s, timeend = NULL, errmsg = NULL
                        WHERE jobid = %s AND status = 'paused'
                    """
                    params = (self.utc_now, jobid)
                else:
                    raise ValueError(f"Unbekannte Aktion: {action}")
                cursor.execute(sql, params)
                updated = cursor.rowcount
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

            if updated:
                if action == "stop":
                    print(f"Job {jobid} wurde gestoppt.")
                else:
                    print(f"Job {jobid} wurde zur Fortsetzung freigegeben (pending).")
            else:
                print(
                    f"Job {jobid} wurde nicht geändert. Prüfe, ob er existiert "
                    "und den passenden Ausgangsstatus hat."
                )


    def reset_error_job(self, jobid, hostid=None):
        """Reset an error job atomically; optionally restrict it to a positive host."""
        if hostid is not None and int(hostid) <= 0:
            raise ValueError('Für den lokalen Reset ist eine positive Host-ID erforderlich.')
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                now = self.utc_now
                cursor.execute(
                    f"""UPDATE {DatabaseConnection.TABLE_JOBS}
                        SET status = CASE
                                WHEN task = 'binarycheck' OR stage = 'coarse' THEN 'insert'
                                ELSE 'pending'
                            END,
                            stage = CASE WHEN task = 'binarycheck' THEN 'coarse' ELSE stage END,
                            timestart = NULL, timeend = NULL,
                            timeupdated = %s, last_activity = %s, errmsg = NULL,
                            usererror = NULL
                        WHERE jobid = %s AND status = 'error'"""
                    + (" AND hostid = %s" if hostid is not None else ""),
                    (now, now, jobid) + ((int(hostid),) if hostid is not None else ()),
                )
                changed = cursor.rowcount == 1
                if changed:
                    cursor.execute(f"SELECT hostid FROM {DatabaseConnection.TABLE_JOBS} WHERE jobid = %s", (jobid,))
                    hostid = int(cursor.fetchone()[0])
                    cursor.execute(
                        f"UPDATE {self.work_table_name(hostid)} SET state = 'pending' "
                        "WHERE jobid = %s AND state = 'active'",
                        (jobid,),
                    )
                connection.commit()
                return changed
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

