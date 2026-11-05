#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# version 0.25.2

"""Central database schema, DDL, structural validation and SQL timeout checks.

SQLStructure defines table names, expected columns and server-safe schema helpers.
SQLStructureCLI provides the existing init/schema API through DataBaseCLI; it uses
that object's fresh connections and DB_HOST/PORT/NAME/USER/PASSWORD configuration.
Commands remain in version25.py: init, serverinit, --init-jobs and check.
No database is opened on import. MariaDB SQL uses the provider's parameterized
connector. Transport/socket timeouts stay with connection management.
Run offline checks with .venv/bin/python -m pytest -q test_sqlstructure.py
and make test-dryrun; live database checks remain explicitly opt-in.
"""

__version__ = "0.25.2"

import re

class SQLStructure:
    """Table definitions and helpers usable without loading client modules."""

    TABLE_HOSTS = "hosts"

    TABLE_JOBS = "jobs"

    TABLE_MODIFIED = "tmp_modified"
    TABLE_DIRECTORIES = "directories"

    TABLE_JOB_WORK = "tmp0_job_work"

    TABLE_JOBS_RUNID = "jobsrunid"

    TABLE_EXCLUDED_NAMES = "excluded_names"

    TABLE_LIVE_EXCLUDED_ABSDIRS = "live_excluded_absdirs"

    TABLE_DOUBLE_JOBS = "`tmp_doubles_letchecked`"

    TABLE_REMOTE_DOUBLES = "tmp_remotedoubles"

    EXPECTED_TABLE_SHAPES = {'jobs': {'columns': {'jobid': ('bigint unsigned', 'NO', None, None),
                          'userhost': ('varchar(255)', 'NO', None, None),
                          'hostid': ('smallint', 'NO', '0', None),
                          'startdirname': ('text', 'NO', None, None),
                          'startdirid': ('bigint unsigned', 'YES', None, None),
                          'task': ('varchar(32)', 'NO', None, None),
                          'source_jobid': ('bigint unsigned', 'YES', None, None),
                          'status': ('varchar(16)', 'NO', 'pending', None),
                          'stage': ('varchar(16)', 'NO', 'coarse', None),
                          'skipdirs': ('varchar(4096)', 'NO', '', None),
                          'scanall': ('tinyint unsigned', 'NO', '0', None),
                          'runid': ('bigint unsigned', 'NO', '0', None),
                          'timecreated': ('datetime(6)', 'NO', None, None),
                          'timestart': ('datetime(6)', 'YES', None, None),
                          'timeend': ('datetime(6)', 'YES', None, None),
                          'timeupdated': ('datetime(6)', 'NO', None, None),
                          'last_activity': ('datetime(6)', 'YES', None, None),
                          'errmsg': ('text', 'YES', None, None),
                          'usererror': ('text', 'YES', None, None)},
              'primary': ('jobid',)},
     'excluded_names': {'columns': {'name': ('varchar(255)', 'NO', None, 'utf8mb4_bin')},
                        'primary': ('name',)},
     'live_excluded_absdirs': {'columns': {'hostid': ('smallint', 'NO', '0', None),
                                           'path_hash': ('char(16)', 'NO', None, 'ascii_bin'),
                                           'path': ('text', 'NO', None, 'utf8mb4_bin')},
                               'primary': ('hostid', 'path_hash')},
     'jobsrunid': {'columns': {'runid': ('bigint unsigned', 'NO', None, None),
                               'jobid': ('bigint unsigned', 'NO', None, None),
                               'task': ('varchar(32)', 'NO', None, None),
                               'status': ('varchar(16)', 'NO', None, None),
                               'stage': ('varchar(16)', 'NO', None, None),
                               'timecreated': ('datetime(6)', 'NO', None, None),
                               'pid': ('bigint unsigned', 'YES', None, None),
                               'process_host': ('varchar(255)', 'YES', None, None),
                               'process_start': ('varchar(128)', 'YES', None, None),
                               'remotehost': ('smallint', 'YES', None, None)},
                   'primary': ('runid',)},
     'directories': {'columns': {'dirid': ('bigint unsigned', 'NO', None, None),
                                 'path_hash': ('char(16)', 'NO', None, None),
                                 'root_path': ('text', 'NO', None, 'utf8mb4_bin'),
                                 'runid': ('bigint unsigned', 'NO', '0', None),
                                 'finished': ('bigint unsigned', 'NO', '0', None),
                                 'hostid': ('smallint', 'NO', '0', None),
                                 'path': ('text', 'NO', None, None),
                                 'name': ('text', 'NO', None, None),
                                 'parentdir': ('bigint unsigned', 'NO', '0', None),
                                 'file_count': ('bigint unsigned', 'YES', None, None),
                                 'subdirectory_count': ('bigint unsigned', 'YES', None, None),
                                 'size_bytes': ('bigint unsigned', 'YES', None, None),
                                 'file_list_sha256': ('char(64)', 'YES', None, None),
                                 'files_sha256': ('char(64)', 'YES', None, None),
                                 'subdirs_sha256': ('char(64)', 'YES', None, None),
                                 'combined_sha256': ('char(64)', 'YES', None, None),
                                 'localfilesize': ('bigint unsigned', 'YES', None, None),
                                 'localfilecount': ('bigint unsigned', 'YES', None, None),
                                 'emptyfilecount': ('bigint unsigned', 'YES', None, None),
                                 'subdircount': ('bigint unsigned', 'YES', None, None),
                                 'otherentrycount': ('bigint unsigned', 'YES', None, None),
                                 'nopermdirs': ('bigint unsigned', 'NO', '0', None),
                                 'zero_content_only': ('tinyint(1)', 'NO', '0', None),
                                 'rescan_from_here': ('tinyint(1)', 'NO', '0', None),
                                 'read_failures': ('bigint unsigned', 'NO', '0', None),
                                 'subdirectory_hash': ('char(64)', 'YES', None, None),
                                 'subtree_size_bytes': ('bigint unsigned', 'YES', None, None),
                                 'subtree_structure': ('text', 'YES', None, None),
                                 'files_lastwrite': ('datetime(6)', 'YES', None, None),
                                 'time_inode_changed': ('datetime(6)', 'YES', None, None),
                                 'hashed_at': ('datetime(6)', 'YES', None, None),
                                 'scanned_at': ('datetime(6)', 'YES', None, None)},
                     'primary': ('dirid',)},
     'tmp0_job_work': {'columns': {'jobid': ('bigint unsigned', 'NO', None, None),
                                   'stage': ('varchar(16)', 'NO', None, None),
                                   'hostid': ('smallint', 'NO', '0', None),
                                   'path_hash': ('char(16)', 'NO', None, None),
                                   'path': ('text', 'NO', None, 'utf8mb4_bin'),
                                   'parent_path': ('text', 'YES', None, 'utf8mb4_bin'),
                                   'child_count': ('bigint unsigned', 'NO', '0', None),
                                   'state': ('varchar(16)', 'NO', 'pending', None)},
                       'primary': ('jobid', 'stage', 'hostid', 'path_hash')},
     'hosts': {'columns': {'hostid': ('smallint', 'NO', None, None),
                           'user': ('varchar(255)', 'NO', None, None),
                           'homepath': ('text', 'NO', None, 'utf8mb4_bin'),
                           'homedirid': ('bigint unsigned', 'YES', None, None),
                           'system': ('varchar(255)', 'NO', None, None),
                           'machine': ('varchar(255)', 'NO', None, None),
                           'tabellenname': ('varchar(64)', 'NO', None, None),
                           'timecreated': ('datetime(6)', 'NO', None, None)},
               'primary': ('hostid',)},
     'tmp_doubles_letchecked': {'columns': {
         'jobid': ('bigint unsigned', 'NO', None, None),
         'hostid': ('smallint', 'NO', '0', None),
         'hostidB': ('smallint', 'NO', '0', None),
         'left_dirid': ('bigint unsigned', 'NO', None, None),
         'right_dirid': ('bigint unsigned', 'NO', None, None),
         'status': ('varchar(16)', 'NO', 'pending', None),
         'verification_method': ('varchar(16)', 'YES', None, None),
         'source_left_dirid': ('bigint unsigned', 'YES', None, None),
         'source_right_dirid': ('bigint unsigned', 'YES', None, None),
         'finished_at': ('datetime(6)', 'YES', None, None),
         'error_message': ('text', 'YES', None, None),
         'is_requested': ('tinyint(1)', 'NO', '1', None),
         'runid': ('bigint unsigned', 'NO', '0', None)},
         'primary': ('jobid', 'hostid', 'hostidB', 'left_dirid', 'right_dirid')}}


    EXPECTED_TABLE_SHAPES['tmp_remotedoubles'] = {
        'columns': {
            'runid': ('bigint unsigned', 'NO', None, None),
            'jobid': ('bigint unsigned', 'NO', None, None),
            'listener_runid': ('bigint unsigned', 'YES', None, None),
            'hostid': ('smallint', 'NO', None, None),
            'dirid': ('bigint unsigned', 'NO', None, None),
            'algorithm': ('varchar(32)', 'NO', 'sha256-tree-v1', None),
            'status': ('varchar(16)', 'NO', 'pending', None),
            'checksum': ('char(64)', 'YES', None, None),
            'error_message': ('text', 'YES', None, None),
            'claim_token': ('char(32)', 'YES', None, None),
            'timeupdated': ('datetime(6)', 'NO', None, None),
        },
        'primary': ('runid', 'hostid', 'dirid'),
    }


    EXPECTED_TABLE_SHAPES['tmp_modified'] = {
        'columns': {
            'batch_jobid': ('bigint unsigned', 'NO', '0', None),
            'hostid': ('smallint', 'NO', '0', None),
            'dirid': ('bigint unsigned', 'NO', None, None),
            'runid': ('bigint unsigned', 'NO', None, None),
            'newtimestamp': ('datetime(6)', 'YES', None, None),
            'subfolders_excluded': ('tinyint(1)', 'NO', '0', None),
            'unreadable': ('tinyint(1)', 'NO', '0', None),
            'path': ('text', 'YES', None, None),
        },
        'primary': ('batch_jobid', 'hostid', 'dirid'),
    }

    REQUIRED_COLUMNS = {
        TABLE_HOSTS: frozenset({"hostid", "user", "homepath", "homedirid", "system", "machine", "timecreated", "tabellenname"}),
        TABLE_JOBS: frozenset({
            "jobid", "userhost", "startdirname", "startdirid", "task", "status",
            "stage", "skipdirs", "scanall", "runid", "hostid", "timecreated", "timestart", "timeend", "timeupdated",
            "last_activity", "errmsg", "usererror",
        }),
        TABLE_JOB_WORK: frozenset({
            "jobid", "stage", "hostid", "path_hash", "path", "parent_path", "child_count", "state",
        }),
        TABLE_JOBS_RUNID: frozenset({
            "runid", "jobid", "task", "status", "stage", "timecreated",
            "pid", "process_host", "process_start", "remotehost",
        }),
        TABLE_EXCLUDED_NAMES: frozenset({"name"}),
        TABLE_LIVE_EXCLUDED_ABSDIRS: frozenset({"hostid", "path_hash", "path"}),
        TABLE_DIRECTORIES: frozenset({
            "dirid", "path_hash", "root_path", "path", "name", "parentdir",
            "file_count", "subdirectory_count", "size_bytes", "file_list_sha256",
            "files_sha256", "subdirs_sha256", "combined_sha256",
            "localfilesize", "localfilecount", "emptyfilecount",
            "subdircount", "otherentrycount",
            "nopermdirs", "subdirectory_hash", "subtree_size_bytes",
            "subtree_structure", "scanned_at", "hashed_at", "files_lastwrite",
            "runid", "hostid",
            "finished", "rescan_from_here", "read_failures", "time_inode_changed",
        }),
    }


    @staticmethod
    def work_table_name(hostid):
        hostid = int(hostid)
        if not 0 <= hostid <= 32767:
            raise ValueError("Host-ID muss zwischen 0 und 32767 liegen.")
        return f"tmp{hostid}_job_work"



    def create_remote_doubles_table(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tmp_remotedoubles (
                runid BIGINT UNSIGNED NOT NULL,
                jobid BIGINT UNSIGNED NOT NULL,
                listener_runid BIGINT UNSIGNED NULL,
                hostid SMALLINT NOT NULL,
                dirid BIGINT UNSIGNED NOT NULL,
                algorithm VARCHAR(32) NOT NULL DEFAULT 'sha256-tree-v1',
                status VARCHAR(16) NOT NULL DEFAULT 'pending',
                checksum CHAR(64) CHARACTER SET ascii NULL,
                error_message TEXT NULL,
                claim_token CHAR(32) CHARACTER SET ascii NULL,
                timeupdated DATETIME(6) NOT NULL,
                PRIMARY KEY (runid, hostid, dirid),
                KEY idx_remote_pending (hostid, status, timeupdated),
                CONSTRAINT fk_remote_job FOREIGN KEY (jobid)
                    REFERENCES jobs(jobid) ON DELETE CASCADE,
                CONSTRAINT fk_remote_run FOREIGN KEY (runid)
                    REFERENCES jobsrunid(runid) ON DELETE CASCADE,
                CONSTRAINT chk_remote_status CHECK (status IN ('pending','active','done','error'))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)


    @staticmethod
    def statement_timeout_message(error):
        """Recognize MariaDB 1969, including cleanup causes and priority IPC tracebacks.

        The error identifies max_statement_time, but does not report its configured
        value. Do not mistake a killed connection or lock timeout for this event.
        """
        seen = set()
        pending = [error]
        while pending:
            current = pending.pop()
            if current is None or id(current) in seen:
                continue
            seen.add(id(current))
            args = getattr(current, 'args', ())
            code = getattr(current, 'errno', None)
            if code is None and args and isinstance(args[0], int):
                code = args[0]
            # Priority errors cross the pipe as formatted tracebacks, not exceptions.
            text = str(current)
            if code == 1969 or ('max_statement_time exceeded' in text
                                and re.search(r'\b1969\b', text)):
                return ('MariaDB hat die SQL-Abfrage wegen Überschreitung von '
                        'max_statement_time abgebrochen (Fehler 1969). '
                        'Vorgesehene Konfiguration: max_statement_time = 360 Sekunden. '
                        'Die tatsächlich geltende Grenze wird im Fehler nicht mitgeliefert.')
            pending.extend((getattr(current, '__cause__', None),
                            getattr(current, '__context__', None)))
        return None


    @staticmethod
    def select_table_names(cursor, database, ordered=False):
        """Select tables visible in the configured database."""
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
                       + (" ORDER BY table_name" if ordered else ""), (database,))


    @staticmethod
    def select_table_count(cursor, database, table):
        """Check existence without interpolating a table name into SQL."""
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables "
                       "WHERE table_schema = %s AND table_name = %s", (database, table))


    @staticmethod
    def select_named_table(cursor, database, table):
        """Look up a table before a data preview."""
        cursor.execute("SELECT table_name FROM information_schema.tables "
                       "WHERE table_schema = %s AND table_name = %s", (database, table))


    @staticmethod
    def select_foreign_key_checks(cursor):
        """Read the current session setting before destructive maintenance."""
        cursor.execute("SELECT @@SESSION.FOREIGN_KEY_CHECKS")


    @staticmethod
    def set_foreign_key_checks(cursor, enabled):
        """Restore or disable the session's foreign-key validation."""
        cursor.execute("SET SESSION FOREIGN_KEY_CHECKS = %s", (int(enabled),))


    @staticmethod
    def set_statement_timeout_multiplier(cursor, multiplier):
        """Execute schema SQL used by connect."""
        return cursor.execute('SET SESSION max_statement_time = @@SESSION.max_statement_time * %s',
                                   (multiplier,))


    def create_excluded_names_table(self, cursor):
        """Execute schema SQL used by add_excluded_names."""
        return cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {self.TABLE_EXCLUDED_NAMES} (
                            name VARCHAR(255) CHARACTER SET utf8mb4
                                COLLATE utf8mb4_bin NOT NULL,
                            PRIMARY KEY (name)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                          COLLATE=utf8mb4_bin
                    """)


    def create_live_excluded_absdirs_table(self, cursor):
        """Execute schema SQL used by add_live_excluded_absdirs."""
        return cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {self.TABLE_LIVE_EXCLUDED_ABSDIRS} (
                            hostid SMALLINT NOT NULL DEFAULT 0,
                            path_hash CHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                            path TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                            PRIMARY KEY (hostid, path_hash)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                    """)


    @staticmethod
    def create_deep_double_candidates_table(cursor):
        """Execute schema SQL used by list_deep_double_levels."""
        return cursor.execute("""
                            CREATE TEMPORARY TABLE tmp_deepdoubles_candidates (
                                dirid BIGINT UNSIGNED PRIMARY KEY,
                                parentdirid BIGINT UNSIGNED NULL,
                                hostid SMALLINT NOT NULL,
                                combined_sha256 CHAR(64) NOT NULL,
                                status SMALLINT NOT NULL DEFAULT 1
                            ) ENGINE=InnoDB
                        """)


    @staticmethod
    def create_deep_double_results_table(cursor):
        """Execute schema SQL used by _store_deep_double_results."""
        return cursor.execute("""
                        CREATE TABLE IF NOT EXISTS tmp_deepdoubles_result (
                            jobid BIGINT UNSIGNED NOT NULL,
                            hostid SMALLINT NOT NULL,
                            dirid BIGINT UNSIGNED NOT NULL,
                            combined_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                            level BIGINT UNSIGNED NOT NULL DEFAULT 0,
                            PRIMARY KEY (jobid, hostid, dirid),
                            KEY idx_deepdoubles_group (jobid, combined_sha256, level)
                        ) ENGINE=InnoDB
                    """)


    def create_modified_table(self, cursor):
        """Create persistent host/job-scoped change batches for dependent workers."""
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {self.TABLE_MODIFIED} (
            batch_jobid BIGINT UNSIGNED NOT NULL DEFAULT 0,
            hostid SMALLINT NOT NULL DEFAULT 0,
            dirid BIGINT UNSIGNED NOT NULL,
            runid BIGINT UNSIGNED NOT NULL,
            newtimestamp DATETIME(6) NULL,
            subfolders_excluded BOOLEAN NOT NULL DEFAULT FALSE,
            unreadable BOOLEAN NOT NULL DEFAULT FALSE,
            path TEXT NULL,
            PRIMARY KEY (batch_jobid, hostid, dirid)
        ) ENGINE=InnoDB""")

    @staticmethod
    def select_priority_columns(cursor):
        """Execute schema SQL used by _refresh_priority."""
        return cursor.execute("SHOW COLUMNS FROM `tmp_highpriority`")



class SQLStructureCLI(SQLStructure):
    """Schema administration through the existing injected CLI connection."""

    def init_database(self):
        """Create the current job schema without migrating existing tables."""
        with self.connect_server() as connection:
            cursor = connection.cursor()
            try:
                db_name = self.database.replace("`", "``")
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.TABLE_JOBS} (
                        jobid BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                        userhost VARCHAR(255) NOT NULL,
                        hostid SMALLINT NOT NULL DEFAULT 0,
                        startdirname TEXT NOT NULL,
                        startdirid BIGINT UNSIGNED NULL,
                        task VARCHAR(32) NOT NULL,
                        source_jobid BIGINT UNSIGNED NULL,
                        status VARCHAR(16) NOT NULL DEFAULT 'pending',
                        stage VARCHAR(16) NOT NULL DEFAULT 'coarse',
                        skipdirs VARCHAR(4096) NOT NULL DEFAULT '',
                        scanall TINYINT UNSIGNED NOT NULL DEFAULT 0,
                        runid BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        timecreated DATETIME(6) NOT NULL,
                        timestart DATETIME(6) NULL,
                        timeend DATETIME(6) NULL,
                        timeupdated DATETIME(6) NOT NULL,
                        last_activity DATETIME(6) NULL,
                        errmsg TEXT NULL,
                        usererror TEXT NULL,
                        PRIMARY KEY (jobid),
                        KEY idx_status (status),
                        KEY idx_task_status (task, status),
                        KEY idx_jobs_source (hostid, source_jobid, task, status),
                        KEY idx_timecreated (timecreated),
                        CONSTRAINT chk_job_status CHECK (
                            status IN ('insert', 'pending', 'active', 'paused', 'stopped', 'closed', 'error', 'stale')
                        ),
                        CONSTRAINT chk_job_task CHECK (
                            task IN ('scan', 'rescan', 'check-modifications', 'rescan-after-days', 'check', 'verify', 'binarycheck')
                        )
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                      COLLATE=utf8mb4_unicode_ci
                """)
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.TABLE_EXCLUDED_NAMES} (
                        name VARCHAR(255) CHARACTER SET utf8mb4
                            COLLATE utf8mb4_bin NOT NULL,
                        PRIMARY KEY (name)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                      COLLATE=utf8mb4_bin
                """)
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.TABLE_LIVE_EXCLUDED_ABSDIRS} (
                        hostid SMALLINT NOT NULL DEFAULT 0,
                        path_hash CHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                        path TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                        PRIMARY KEY (hostid, path_hash)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """)
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.TABLE_JOBS_RUNID} (
                        runid BIGINT UNSIGNED NOT NULL,
                        jobid BIGINT UNSIGNED NOT NULL,
                        task VARCHAR(32) NOT NULL,
                        status VARCHAR(16) NOT NULL,
                        stage VARCHAR(16) NOT NULL,
                        timecreated DATETIME(6) NOT NULL,
                        pid BIGINT UNSIGNED NULL,
                        process_host VARCHAR(255) NULL,
                        process_start VARCHAR(128) NULL,
                        remotehost SMALLINT NULL,
                        PRIMARY KEY (runid),
                        KEY idx_jobsrunid_jobid (jobid),
                        KEY idx_jobsrunid_timecreated (timecreated)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                      COLLATE=utf8mb4_unicode_ci
                """)
                self.create_double_jobs_table(cursor)
                self.create_remote_doubles_table(cursor)
                self.create_modified_table(cursor)
                self.create_priority_table(cursor)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()
        print(f"Datenbank: {self.database}")
        print(
            f"Tabellen {self.TABLE_JOBS}, {self.TABLE_DOUBLE_JOBS}, "
            f"{self.TABLE_JOBS_RUNID} und {self.TABLE_EXCLUDED_NAMES} sind eingerichtet; "
            "bestehende Tabellen wurden nicht migriert."
        )

    def init_alldirs(self, reset=None):
        """Create directory schema and report whether a new inventory replaced the old one."""
        answer = "y" if reset else "n"
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = %s",
                    (self.database, self.TABLE_DIRECTORIES),
                )
                existed = bool(cursor.fetchone()[0])
                if existed and reset is None:
                    while True:
                        answer = input(
                            "Wollen sie die Tabelle directories wirklich löschen und neu anlegen? [y/n] "
                        ).strip().lower()
                        if answer in ("y", "n"):
                            break
                        print("Bitte y oder n eingeben.")
                if existed and answer == "y":
                    cursor.execute(f"DROP TABLE IF EXISTS {self.TABLE_DIRECTORIES}")
                # dirid is globally unique across hosts. hostid scopes paths only.
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.TABLE_DIRECTORIES} (
                        dirid BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                        path_hash CHAR(16) NOT NULL,
                        root_path TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                        runid BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        finished BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        hostid SMALLINT NOT NULL DEFAULT 0,
                        path TEXT NOT NULL,
                        name TEXT NOT NULL,
                        parentdir BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        file_count BIGINT UNSIGNED NULL,
                        subdirectory_count BIGINT UNSIGNED NULL,
                        size_bytes BIGINT UNSIGNED NULL,
                        file_list_sha256 CHAR(64) NULL,
                        files_sha256 CHAR(64) NULL,
                        subdirs_sha256 CHAR(64) NULL,
                        combined_sha256 CHAR(64) NULL,
                        localfilesize BIGINT UNSIGNED NULL,
                        localfilecount BIGINT UNSIGNED NULL,
                        emptyfilecount BIGINT UNSIGNED NULL,
                        subdircount BIGINT UNSIGNED NULL,
                        otherentrycount BIGINT UNSIGNED NULL,
                        nopermdirs BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        zero_content_only BOOLEAN NOT NULL DEFAULT FALSE,
                        rescan_from_here BOOLEAN NOT NULL DEFAULT FALSE,
                        read_failures BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        subdirectory_hash CHAR(64) NULL,
                        subtree_size_bytes BIGINT UNSIGNED NULL,
                        subtree_structure TEXT NULL,
                        files_lastwrite DATETIME(6) NULL,
                        time_inode_changed DATETIME(6) NULL,
                        hashed_at DATETIME(6) NULL,
                        scanned_at DATETIME(6) NULL,
                        PRIMARY KEY (dirid),
                        UNIQUE KEY uq_directories_host_path_hash (hostid, path_hash),
                        KEY idx_directories_root (root_path(191)),
                        KEY idx_directories_parent (parentdir),
                        KEY idx_directories_host_dirid (hostid, dirid),
                        KEY idx_directories_host_candidates (hostid, zero_content_only, combined_sha256),
                        KEY idx_directories_double_filter (hostid, zero_content_only)
                    ) ENGINE=InnoDB AUTO_INCREMENT=1
                      DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()
        suffix = " Bestehendes Schema wurde nicht migriert." if existed and answer == "n" else ""
        print(
            f"Tabelle {self.TABLE_DIRECTORIES} ist in {self.database} eingerichtet."
            f"{suffix}"
        )
        return not existed or answer == "y"

    def _work_tables(self, cursor):
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            (self.database,),
        )
        return sorted(row[0] for row in cursor.fetchall()
                      if re.fullmatch(r"tmp[0-9]+_job_work", row[0]))

    def create_job_work_table(self, hostid, cursor=None, migrate_legacy=True):
        """Create an independent queue and move this host's legacy checkpoints."""
        if cursor is None:
            with self.connect() as connection:
                own_cursor = connection.cursor()
                try:
                    self.create_job_work_table(hostid, own_cursor, migrate_legacy=migrate_legacy)
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    own_cursor.close()
            return self.work_table_name(hostid)
        table = self.work_table_name(hostid)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                jobid BIGINT UNSIGNED NOT NULL,
                stage VARCHAR(16) NOT NULL,
                hostid SMALLINT NOT NULL DEFAULT 0,
                path_hash CHAR(16) NOT NULL,
                path TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                parent_path TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
                child_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                state VARCHAR(16) NOT NULL DEFAULT 'pending',
                PRIMARY KEY (jobid, stage, hostid, path_hash),
                KEY idx_job_work_state (jobid, stage, state)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COLLATE=utf8mb4_unicode_ci
        """)
        if migrate_legacy:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = 'job_work'", (self.database,),
            )
            if cursor.fetchone()[0]:
                fields = "jobid, stage, hostid, path_hash, path, parent_path, child_count, state"
                cursor.execute(
                    f"INSERT IGNORE INTO {table} ({fields}) "
                    f"SELECT {fields} FROM job_work WHERE hostid = %s", (int(hostid),),
                )
                cursor.execute("DELETE FROM job_work WHERE hostid = %s", (int(hostid),))
        return table

    def create_hosts_table(self, cursor=None):
        """Create hosts if absent, optionally using the caller's connection."""
        if cursor is None:
            with self.connect() as connection:
                own_cursor = connection.cursor()
                try:
                    self.create_hosts_table(own_cursor)
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    own_cursor.close()
            return
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_HOSTS} (
                hostid SMALLINT NOT NULL,
                `user` VARCHAR(255) NOT NULL,
                homepath TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                homedirid BIGINT UNSIGNED NULL,
                `system` VARCHAR(255) NOT NULL,
                machine VARCHAR(255) NOT NULL,
                tabellenname VARCHAR(64) NOT NULL,
                timecreated DATETIME(6) NOT NULL,
                PRIMARY KEY (hostid),
                CONSTRAINT chk_hosts_hostid CHECK (hostid >= 0)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
        """)

        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = 'tabellenname'",
            (self.database, self.TABLE_HOSTS),
        )
        if not cursor.fetchone()[0]:
            cursor.execute(f"ALTER TABLE {self.TABLE_HOSTS} ADD COLUMN tabellenname VARCHAR(64) NULL")
            cursor.execute(f"UPDATE {self.TABLE_HOSTS} SET tabellenname = CONCAT('tmp', hostid, '_job_work')")
            cursor.execute(f"ALTER TABLE {self.TABLE_HOSTS} MODIFY COLUMN tabellenname VARCHAR(64) NOT NULL")

    def ensure_jobs_constraints(self):
        """Install current task/status checks on an existing jobs table atomically."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM information_schema.tables "
                               "WHERE table_schema = %s AND table_name = %s",
                               (self.database, self.TABLE_JOBS))
                if cursor.fetchone()[0] == 0:
                    return
                # One ALTER keeps the previous checks if existing rows fail validation.
                cursor.execute(f"""ALTER TABLE {self.TABLE_JOBS}
                    DROP CONSTRAINT IF EXISTS chk_job_task,
                    DROP CONSTRAINT IF EXISTS chk_job_status,
                    ADD CONSTRAINT chk_job_task CHECK (
                        task IN ('scan', 'rescan', 'check-modifications', 'rescan-after-days', 'check', 'verify', 'binarycheck')
                    ),
                    ADD CONSTRAINT chk_job_status CHECK (
                        status IN ('insert', 'pending', 'active', 'paused', 'stopped', 'closed', 'error', 'stale')
                    )""")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

    def _show_statement_timeout_hint(self):
        """Read normal system variables; advise only if either value differs from 360."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute('SELECT @@GLOBAL.max_statement_time, @@SESSION.max_statement_time')
                global_limit, session_limit = cursor.fetchone()
            finally:
                cursor.close()
        if global_limit == 360 and session_limit == 360:
            return
        print(f"MariaDB-Abfragezeitlimit: global={global_limit} s, Sitzung={session_limit} s; "
              "erwartet: jeweils 360 s.\n"
              "Dauerhaft: sudo vi /var/packages/MariaDB10/etc/my.cnf; "
              "unter [mariadb] max_statement_time = 360 setzen und MariaDB neu starten.\n"
              "Alternativ als Administrator im laufenden System: "
              "SET GLOBAL max_statement_time = 360;\n"
              "Die globale Änderung gilt für neue Verbindungen; "
              "dauerhaft zusätzlich die Konfigurationsdatei ändern.", flush=True)

    @staticmethod
    def create_priority_table(cursor):
        """Create the shared, host-scoped priority hints used by workers."""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tmp_highpriority (
                hostid SMALLINT NOT NULL,
                dirid BIGINT UNSIGNED NOT NULL,
                PRIMARY KEY (hostid, dirid)
            ) ENGINE=InnoDB
        """)

    def check_schema(self):
        """Validate every managed table, every numeric queue and the server entry."""
        problems = []
        shapes = dict(self.EXPECTED_TABLE_SHAPES)
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT table_name, column_name, column_type, is_nullable, "
                    "column_default, collation_name FROM information_schema.columns "
                    "WHERE table_schema = %s", (self.database,),
                )
                columns = {}
                for table, column, typ, nullable, default, collation in cursor.fetchall():
                    columns.setdefault(table, {})[column] = (str(typ).lower(), nullable, default, collation)
                hosts = columns.get(self.TABLE_HOSTS, {})
                host_rows = []
                if {"hostid", "tabellenname"} <= set(hosts):
                    cursor.execute(f"SELECT hostid, tabellenname FROM {self.TABLE_HOSTS} ORDER BY hostid")
                    host_rows = cursor.fetchall()
                if not any(str(hostid) == '0' and name == "tmp0_job_work" for hostid, name in host_rows):
                    problems.append("hosts benötigt hostid=0 mit tabellenname='tmp0_job_work'.")
                for hostid, table in host_rows:
                    if table is None or not str(table).strip():
                        problems.append(f"hosts.hostid={hostid}: tabellenname ist leer; Arbeitstabelle nicht angegeben.")
                        continue
                    try:
                        expected_name = self.work_table_name(hostid)
                    except (ValueError, TypeError):
                        problems.append(f"Ungültige Host-ID in hosts: {hostid!r}.")
                        continue
                    if table != expected_name:
                        problems.append(f"hosts.hostid={hostid}: tabellenname muss {expected_name} sein.")
                    shapes[expected_name] = self.EXPECTED_TABLE_SHAPES[self.TABLE_JOB_WORK]
                for table in columns:
                    if re.fullmatch(r"tmp[0-9]+_job_work", table):
                        shapes[table] = self.EXPECTED_TABLE_SHAPES[self.TABLE_JOB_WORK]
                cursor.execute(
                    "SELECT table_name, column_name, seq_in_index FROM information_schema.statistics "
                    "WHERE table_schema = %s AND index_name = 'PRIMARY' ORDER BY table_name, seq_in_index",
                    (self.database,),
                )
                primary_keys = {}
                for table, column, position in cursor.fetchall():
                    primary_keys.setdefault(table, []).append(column)
                def normalize_type(value):
                    # MySQL and MariaDB differ in integer display widths.
                    return re.sub(r"\b(tinyint|smallint|bigint)\(\d+\)", r"\1", value.lower())
                for table, shape in shapes.items():
                    actual = columns.get(table)
                    if actual is None:
                        problems.append(f"Tabelle {table} fehlt.")
                        continue
                    for column, wanted in shape["columns"].items():
                        if column not in actual:
                            problems.append(f"{table}.{column} fehlt.")
                            continue
                        typ, nullable, default, collation = actual[column]
                        want_type, want_null, want_default, want_collation = wanted
                        if normalize_type(typ) != normalize_type(want_type) or nullable != want_null:
                            problems.append(f"{table}.{column}: erwartet {want_type.upper()} "
                                            + ("NOT NULL." if want_null == "NO" else "NULL."))
                        default_text = None if default is None else str(default).strip("'")
                        if default_text is not None and default_text.upper() == "NULL":
                            default_text = None
                        if default_text != want_default:
                            problems.append(f"{table}.{column}: falscher Standardwert {default!r}; erwartet {want_default!r}.")
                        if want_collation and collation != want_collation:
                            problems.append(f"{table}.{column}: erwartet Collation {want_collation}.")
                    if tuple(primary_keys.get(table, ())) != shape["primary"]:
                        problems.append(f"{table}: falscher Primärschlüssel; erwartet {shape['primary']}.")
                if ({'root_path', 'runid'} <= set(columns.get(self.TABLE_DIRECTORIES, {}))
                        and {'runid', 'jobid'} <= set(columns.get(self.TABLE_JOBS_RUNID, {}))):
                    for conflict in self.list_root_path_job_conflicts(cursor=cursor):
                        jobs = ', '.join(map(str, conflict['jobids']))
                        problems.append(f"root_path={conflict['root_path']} hat unterschiedliche Job-IDs: {jobs}.")
            finally:
                cursor.close()
        if problems:
            for problem in problems:
                print(f"FEHLER: {problem}")
            print("Schema-Prüfung fehlgeschlagen.")
            return False
        print("Schema-Prüfung erfolgreich: alle regulären Tabellen und tmp-Tabellen korrekt; "
              "hosts.hostid=0 verweist auf tmp0_job_work.")
        return True

    def create_double_jobs_table(self, cursor):
        """Create pair results without altering existing tables."""
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_DOUBLE_JOBS} (
                jobid BIGINT UNSIGNED NOT NULL,
                hostid SMALLINT NOT NULL DEFAULT 0,
                hostidB SMALLINT NOT NULL DEFAULT 0,
                left_dirid BIGINT UNSIGNED NOT NULL,
                right_dirid BIGINT UNSIGNED NOT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'pending',
                verification_method VARCHAR(16) NULL,
                source_left_dirid BIGINT UNSIGNED NULL,
                source_right_dirid BIGINT UNSIGNED NULL,
                finished_at DATETIME(6) NULL,
                error_message TEXT NULL,
                is_requested BOOLEAN NOT NULL DEFAULT TRUE,
                runid BIGINT UNSIGNED NOT NULL DEFAULT 0,
                PRIMARY KEY (jobid, hostid, hostidB, left_dirid, right_dirid),
                KEY idx_double_jobs_pending (jobid, hostid, status, is_requested),
                CONSTRAINT chk_double_jobs_pair CHECK (hostid <> hostidB OR left_dirid < right_dirid),
                CONSTRAINT chk_double_jobs_status CHECK (
                    status IN ('pending', 'equal', 'different', 'unreadable', 'skipped_empty')
                ),
                CONSTRAINT chk_double_jobs_method CHECK (
                    verification_method IS NULL OR verification_method IN ('direct', 'inherited')
                ),
                CONSTRAINT fk_double_jobs_job FOREIGN KEY (jobid)
                    REFERENCES {self.TABLE_JOBS}(jobid) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COLLATE=utf8mb4_unicode_ci
        """)


# Compatibility for callers importing the historical timeout helper.
statement_timeout_message = SQLStructure.statement_timeout_message
