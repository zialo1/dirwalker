# version 0.25.2

"""Datenbanktreiber, Verbindungsaufbau und gemeinsame Verbindungskonfiguration."""

__version__ = "0.25.2"

import os
import tempfile
import sqlstructure
from sqlstructure import SQLStructure, statement_timeout_message
import hashlib
import stat
import uuid
import logging
import sys
import socket
import subprocess
import json
import re
import time
import threading
import weakref
from collections import deque
from pathlib import Path
from datetime import datetime, timezone

try:
    import mysql.connector

    _database_connect = mysql.connector.connect
    Error = mysql.connector.Error
    DATABASE_DRIVER = "MySQL Connector/Python"
except ImportError:
    try:
        import mariadb

        _database_connect = mariadb.connect
        Error = mariadb.Error
        DATABASE_DRIVER = "MariaDB Connector/Python"
    except ImportError as exc:
        raise ImportError(
            "Datenbanktreiber fehlt. Installiere mit "
            "'pip install mysql-connector-python==8.4.0'."
        ) from exc


DB_IO_TIMEOUT = int(os.environ.get("DB_IO_TIMEOUT", "30"))
if DB_IO_TIMEOUT <= 0:
    raise ValueError("DB_IO_TIMEOUT muss positiv sein.")


def is_network_error(error):
    """Recognize transport failures even when rollback masked the first error."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if getattr(error, 'errno', None) in (2002, 2003, 2005, 2006, 2013, 2055):
            return True
        if isinstance(error, (ConnectionError, TimeoutError, socket.gaierror)):
            return True
        error = error.__cause__ or error.__context__
    return False


def _process_start_token(pid):
    """Identify a live local process, distinguishing reused PIDs and zombies."""
    pid = int(pid)
    if pid <= 0:
        raise ValueError('Eine positive PID ist erforderlich.')
    if Path('/proc/self/stat').exists():
        try:
            # comm can contain spaces and parentheses; fields after its last
            # closing parenthesis begin with state (field 3), then starttime 22.
            fields = Path(f'/proc/{pid}/stat').read_text().rpartition(') ')[2].split()
        except FileNotFoundError:
            return None
        if len(fields) < 20:
            raise RuntimeError(f'Startkennung für PID {pid} nicht lesbar.')
        if fields[0] in ('Z', 'X'):
            return None
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        return 'proc:' + boot + ':' + fields[19]
    environment = dict(os.environ, LC_ALL='C', TZ='UTC')
    try:
        result = subprocess.run(
            ['ps', '-p', str(pid), '-o', 'stat=', '-o', 'lstart='],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
            env=environment, timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f'Prozessprüfung für PID {pid} fehlgeschlagen: {exc}') from exc
    if result.returncode == 1 and not result.stdout.strip():
        return None
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f'Prozessprüfung für PID {pid} fehlgeschlagen.')
    state, started = result.stdout.strip().split(None, 1)
    return None if state.startswith(('Z', 'X')) else 'ps:' + started.strip()




class _TaggedCursor:
    """Keep ownership visible in SHOW FULL PROCESSLIST without changing SQL semantics."""
    def __init__(self, cursor, tag):
        self._cursor, self._tag = cursor, tag

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def execute(self, operation, *args, **kwargs):
        return self._cursor.execute(self._tag + operation, *args, **kwargs)

    def executemany(self, operation, *args, **kwargs):
        return self._cursor.executemany(self._tag + operation, *args, **kwargs)

    def __iter__(self):
        return iter(self._cursor)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._cursor.close()


class _TaggedConnection:
    def __init__(self, connection, tag):
        self._connection, self._tag = connection, tag

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def cursor(self, *args, **kwargs):
        return _TaggedCursor(self._connection.cursor(*args, **kwargs), self._tag)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        DataBaseConnectionCore._close_preserving_error(self._connection)


def exclusion_matcher(skipdirs):
    """Case-insensitive name exclusions at every depth; absolute paths retain case."""
    values = [part.strip() for part in (skipdirs or '').split(',') if part.strip()]
    names = {value.casefold() for value in values if not os.path.isabs(value)}
    paths = [os.path.realpath(value) for value in values if os.path.isabs(value)]
    def excluded(path):
        if any(part.casefold() in names for part in Path(os.path.normpath(path)).parts):
            return True
        candidate = os.path.realpath(path) if paths else ''
        for parent in paths:
            try:
                if os.path.commonpath((parent, candidate)) == parent:
                    return True
            except ValueError:
                pass
        return False
    return excluded


def directory_content_checksum(root_path, progress=None, is_excluded=None):
    """SHA-256 over a canonical tree manifest including actual file bytes.

    Root names, mtimes and ownership are excluded. Names, entry types, empty
    directories and symlink targets are included. Never return a digest after
    a read error or a detected change during traversal.
    """
    is_excluded = is_excluded or (lambda path: False)
    root_path = os.path.abspath(root_path)
    if not stat.S_ISDIR(os.lstat(root_path).st_mode):
        raise NotADirectoryError(root_path)
    digest = hashlib.sha256(b'dirwalker-content-tree-v2\0')

    def frame(value):
        digest.update(len(value).to_bytes(8, 'big'))
        digest.update(value)

    def identity(info):
        return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
                info.st_mtime_ns, info.st_ctime_ns)

    stack = [(root_path, '', None)]
    while stack:
        path, relative, before = stack.pop()
        if progress:
            progress()
        if before is not None:
            if identity(os.lstat(path)) != identity(before):
                raise OSError('Directory changed during checksum: ' + path)
            continue
        before = os.lstat(path)
        frame(relative.encode('utf-8', 'surrogateescape'))
        if stat.S_ISDIR(before.st_mode):
            frame(b'D')
            with os.scandir(path) as entries:
                children = sorted((entry for entry in entries if not is_excluded(entry.path)), key=lambda entry: entry.name)
            stack.append((path, relative, before))
            for entry in reversed(children):
                child_relative = relative + '/' + entry.name if relative else entry.name
                stack.append((entry.path, child_relative, None))
        elif stat.S_ISREG(before.st_mode):
            frame(b'F')
            file_digest = hashlib.sha256()
            flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
            fd = os.open(path, flags)
            with os.fdopen(fd, 'rb') as handle:
                if identity(os.fstat(handle.fileno())) != identity(before):
                    raise OSError('File changed before checksum: ' + path)
                while True:
                    if progress:
                        progress()
                    block = handle.read(1024 * 1024)
                    if not block:
                        break
                    file_digest.update(block)
                if identity(os.fstat(handle.fileno())) != identity(before):
                    raise OSError('File changed during checksum: ' + path)
            if identity(os.lstat(path)) != identity(before):
                raise OSError('File replaced during checksum: ' + path)
            frame(file_digest.digest())
        elif stat.S_ISLNK(before.st_mode):
            frame(b'L')
            frame(os.readlink(path).encode('utf-8', 'surrogateescape'))
            if identity(os.lstat(path)) != identity(before):
                raise OSError('Symlink changed during checksum: ' + path)
        else:
            # Devices, sockets and FIFOs cannot establish byte equality.
            raise OSError('Unsupported file type for checksum: ' + path)
    return digest.hexdigest()

class DataBaseConnectionCore(SQLStructure):
    """Open independent MariaDB/MySQL connections from environment settings."""

    def reset_server_locks(self, hostid=None, directory=None):
        """Delete matching local lock files, assuming all servers have been stopped."""
        hosts = {0} | {int(row[0]) for row in self.list_hosts()} if hostid is None else {int(hostid)}
        if any(value < 0 or value > 32767 for value in hosts):
            raise ValueError('Host-ID muss zwischen 0 und 32767 liegen.')
        folder = Path(tempfile.gettempdir()) if directory is None else Path(directory)
        cleared = []
        for selected in sorted(hosts):
            scope = repr((str(self.host).casefold(), int(self.port), str(self.database), selected))
            digest = hashlib.sha256(scope.encode('utf-8')).hexdigest()
            path = folder / f'dirwalker-server-{digest}.lock'
            try:
                descriptor = os.open(str(path), os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
                with os.fdopen(descriptor, 'r', encoding='utf-8', errors='replace') as handle:
                    marker = handle.read(128).strip()
                pid = marker if marker.isdecimal() else 'leer' if not marker else 'ungültiger PID-Marker'
            except FileNotFoundError:
                continue
            except OSError as error:
                pid = f'nicht lesbar ({error})'
            print(f'Lockdatei: {path}\n'
                  f'  Zweck: exklusive Serverinstanz (server/serverplus/localserver) für Host-ID {selected}\n'
                  f'  Datenbank: {self.database}; DB-Server: {self.host}:{self.port}\n'
                  f'  Gespeicherte PID: {pid} (historischer Marker; Laufzustand nicht geprüft)',flush=True)
            try:
                path.unlink()
            except FileNotFoundError:
                print('  Ergebnis: bereits entfernt.',flush=True)
                continue
            except OSError:
                print('  Ergebnis: Löschen fehlgeschlagen.',flush=True)
                raise
            print('  Ergebnis: Datei gelöscht; alle Server werden als gestoppt vorausgesetzt.',flush=True)
            cleared.append(selected)
        return cleared

    def run_age_rescan_job(self, jobid, hostid=0, note=None):
        """Execute an age-rescan task in its server-spawned worker process."""
        return AgeRescanWorkflow(self, note=note).run(jobid, hostid)

    def ensure_combined_hash_readiness(self, hostid=0, cursor=None):
        """Clear aggregate hashes whose local or child scan metadata is unfinished.

        One grouped read checks child counts and existing child hashes. Propagate
        incompleteness to ancestors before writing, so missing child hashes cannot
        be mistaken for completed empty directories. This never creates a hash.
        """
        if cursor is None:
            with self.connect() as connection:
                writer = connection.cursor()
                try:
                    count = self.ensure_combined_hash_readiness(hostid, cursor=writer)
                    connection.commit()
                    return count
                except Exception:
                    self._rollback_preserving_error(connection)
                    raise
                finally:
                    self._close_preserving_error(writer)
        cursor.execute(f"""SELECT dirid, parentdir, files_sha256, localfilesize,
                                  subdircount, combined_sha256
            FROM {self.TABLE_DIRECTORIES} WHERE hostid = %s""", (int(hostid),))
        rows = {row[0]: row for row in cursor.fetchall()}
        children = {}
        for row in rows.values():
            children.setdefault(row[1], []).append(row)
        incomplete = set()
        for dirid, row in rows.items():
            direct = children.get(dirid, [])
            if (row[2] is None or row[3] is None or row[4] != len(direct)
                    or any(child[5] is None for child in direct)):
                incomplete.add(dirid)
        pending = list(incomplete)
        while pending:
            parent = rows[pending.pop()][1]
            if parent in rows and parent not in incomplete:
                incomplete.add(parent)
                pending.append(parent)
        invalid = [(dirid, int(hostid)) for dirid in sorted(incomplete)
                   if rows[dirid][5] is not None]
        if invalid:
            cursor.executemany(f"""UPDATE {self.TABLE_DIRECTORIES}
                SET subdirs_sha256 = NULL, combined_sha256 = NULL,
                    subdirectory_hash = NULL, subtree_size_bytes = NULL,
                    subtree_structure = NULL, zero_content_only = FALSE
                WHERE dirid = %s AND hostid = %s""", invalid)
        return len(invalid)

    def read_directory_families(self, works, hostid=0, cursor=None):
        """Read complete directory rows and their immediate children in groups.

        Results are keyed by work path_hash; each contains directory and children.
        Restrict both sides of the join to the host and verify the exact anchor
        path, so a hash collision never supplies another directory's metadata.
        The snapshot is batch-local and is not cached across scan commits.
        """
        expected = {work['path_hash']: work['path'] for work in works}
        families = {key: dict(directory=None, children=[]) for key in expected}
        if not expected:
            return families
        if cursor is None:
            with self.connect() as connection:
                reader = connection.cursor(dictionary=True)
                try:
                    return self.read_directory_families(works, hostid, cursor=reader)
                finally:
                    self._close_preserving_error(reader)
        hashes = list(expected)
        for offset in range(0, len(hashes), 200):
            group = hashes[offset:offset + 200]
            placeholders = ','.join(['%s'] * len(group))
            cursor.execute(f"""SELECT d.*, p.dirid AS _family_dirid,
                                      p.path_hash AS _family_hash, p.path AS _family_path
                FROM {self.TABLE_DIRECTORIES} p
                JOIN {self.TABLE_DIRECTORIES} d ON d.hostid = p.hostid
                    AND (d.dirid = p.dirid OR d.parentdir = p.dirid)
                WHERE p.hostid = %s AND p.path_hash IN ({placeholders})
                ORDER BY p.dirid, d.dirid""", (int(hostid),) + tuple(group))
            for result in cursor.fetchall():
                row = dict(result)
                parent_id = row.pop('_family_dirid')
                key = row.pop('_family_hash')
                anchor_path = row.pop('_family_path')
                if expected.get(key) != anchor_path:
                    continue
                if row['dirid'] == parent_id:
                    families[key]['directory'] = row
                else:
                    families[key]['children'].append(row)
        return families

    def mark_binarycheck_hashed(self, cursor, jobid, directories, timestamp):
        """Stamp completed comparisons only when no newer metadata scan intervened."""
        for hostid,dirid in sorted(set(directories)):
            cursor.execute(f"""UPDATE {self.TABLE_DIRECTORIES} d
                JOIN {self.TABLE_JOBS} j ON j.jobid=%s
                SET d.hashed_at=GREATEST(COALESCE(d.hashed_at,%s),%s)
                WHERE d.hostid=%s AND d.dirid=%s AND j.status='active'
                  AND d.scanned_at IS NOT NULL AND j.timestart >= d.scanned_at""",
                (jobid,timestamp,timestamp,hostid,dirid))

    def list_unused_jobs(self, hostid=None, cursor=None, for_update=False):
        """List terminal jobs with no inventory references or runnable dependants."""
        if cursor is None:
            with self.connect() as connection:
                reader=connection.cursor()
                try:
                    return self.list_unused_jobs(hostid,cursor=reader,for_update=for_update)
                finally:
                    self._close_preserving_error(reader)
        statement=f"""SELECT j.jobid,j.hostid,j.task,j.status,j.startdirname,j.timeend
            FROM {self.TABLE_JOBS} j
            WHERE j.status IN ('closed','error','stopped','stale')
              AND NOT EXISTS (SELECT 1 FROM {self.TABLE_JOBS_RUNID} r
                JOIN {self.TABLE_DIRECTORIES} d ON d.runid=r.runid OR d.finished=r.runid
                WHERE r.jobid=j.jobid)
              AND NOT EXISTS (SELECT 1 FROM {self.TABLE_JOBS} dependent
                WHERE dependent.source_jobid=j.jobid
                  AND dependent.status IN ('insert','pending','active','paused'))"""
        params=()
        if hostid is not None:
            statement+=' AND j.hostid=%s'
            params=(int(hostid),)
        cursor.execute(statement+' ORDER BY j.hostid,j.jobid'+(' FOR UPDATE' if for_update else ''),params)
        fields=('jobid','hostid','task','status','startdirname','timeend')
        return [dict(zip(fields,row)) for row in cursor.fetchall()]

    def list_root_path_job_conflicts(self, cursor=None):
        """Find exact root paths owned by distinct known jobs across all hosts.

        Multiple run IDs of one job are normal. Unmapped run IDs alone cannot
        prove a conflict and are not counted as a second job.
        """
        if cursor is None:
            with self.connect() as connection:
                reader = connection.cursor()
                try:
                    return self.list_root_path_job_conflicts(cursor=reader)
                finally:
                    self._close_preserving_error(reader)
        cursor.execute(f"""SELECT d.root_path, r.jobid, COUNT(*)
            FROM {self.TABLE_DIRECTORIES} d
            LEFT JOIN {self.TABLE_JOBS_RUNID} r ON r.runid = d.runid
            GROUP BY BINARY d.root_path, d.root_path, r.jobid
            ORDER BY BINARY d.root_path, r.jobid""")
        groups = {}
        for path, jobid, count in cursor.fetchall():
            record = groups.setdefault(path, dict(root_path=path, jobids=set(), directory_count=0))
            if jobid is not None:
                record['jobids'].add(int(jobid))
            record['directory_count'] += int(count)
        conflicts = []
        for record in groups.values():
            if len(record['jobids']) > 1:
                record['jobids'] = sorted(record['jobids'])
                conflicts.append(record)
        return conflicts

    def plan_root_job_cleanup(self, roots, cursor=None):
        """Propose older-job entries, but never select a currently active job."""
        roots = sorted(set(roots))
        if not roots:
            return []
        if cursor is None:
            with self.connect() as connection:
                reader = connection.cursor()
                try:
                    return self.plan_root_job_cleanup(roots, cursor=reader)
                finally:
                    self._close_preserving_error(reader)
        placeholders = ','.join(['%s'] * len(roots))
        cursor.execute(f"""SELECT d.root_path, d.hostid, r.jobid, j.timeend, COUNT(*), j.status
            FROM {self.TABLE_DIRECTORIES} d
            LEFT JOIN {self.TABLE_JOBS_RUNID} r ON r.runid = d.runid
            LEFT JOIN {self.TABLE_JOBS} j ON j.jobid = r.jobid AND j.hostid = d.hostid
            WHERE BINARY d.root_path IN ({placeholders})
            GROUP BY BINARY d.root_path, d.root_path, d.hostid, r.jobid, j.timeend, j.status
            ORDER BY BINARY d.root_path, d.hostid, r.jobid""", tuple(roots))
        groups = {}
        for path, hostid, jobid, ended, count, status in cursor.fetchall():
            groups.setdefault(path, []).append(dict(root_path=path, hostid=int(hostid),
                jobid=int(jobid) if jobid is not None else None,
                timeend=ended, directory_count=int(count), status=status))
        candidates = []
        for records in groups.values():
            completed = [row for row in records if row['timeend'] is not None and row['jobid'] is not None]
            keep = max(completed, key=lambda row: (row['timeend'], row['jobid']))['jobid'] if completed else None
            candidates.extend(row for row in records if row['status'] != 'active'
                              and (row['timeend'] is None or row['jobid'] != keep))
        return candidates

    def delete_root_job_candidates(self, candidates):
        """Recheck the approved plan and delete only its root/host/job inventory rows."""
        if not candidates:
            return 0
        roots = [row['root_path'] for row in candidates]
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                current = self.plan_root_job_cleanup(roots, cursor=cursor)
                def signature(rows):
                    return {(row['root_path'], row['hostid'], row['jobid'],
                             row['timeend'], row['directory_count'], row.get('status')) for row in rows}
                if signature(current) != signature(candidates):
                    raise RuntimeError('Reparaturvorschlag hat sich geändert; nichts gelöscht. Erneut starten.')
                deleted = 0
                for row in candidates:
                    job_condition = 'r.jobid IS NULL' if row['jobid'] is None else 'r.jobid = %s'
                    params = (row['hostid'], row['root_path'])
                    if row['jobid'] is not None:
                        params += (row['jobid'],)
                    cursor.execute(f"""DELETE d FROM {self.TABLE_DIRECTORIES} d
                        LEFT JOIN {self.TABLE_JOBS_RUNID} r ON r.runid = d.runid
                        WHERE d.hostid = %s AND BINARY d.root_path = BINARY %s
                          AND {job_condition}""", params)
                    deleted += cursor.rowcount
                connection.commit()
                return deleted
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                self._close_preserving_error(cursor)

    def list_directory_hostids(self, cursor=None, include_jobs=True):
        """List inventory hosts with the jobs referenced by directories.runid.

        By default return one dictionary per (hostid, jobid), containing hostid,
        jobid, sorted runids/root_paths, directory_count, and timeend (UTC or None).
        Sort in this core function by descending directory_count, then host/job.
        Preserve orphaned runs with jobid=None rather than hiding their host.
        include_jobs=False returns only distinct host IDs for host-level work.
        An existing cursor keeps reads on an administrative timeout-adjusted handle.
        """
        if cursor is not None:
            if not include_jobs:
                cursor.execute(f"SELECT DISTINCT hostid FROM {self.TABLE_DIRECTORIES} ORDER BY hostid")
                return [int(row[0]) for row in cursor.fetchall()]
            cursor.execute(f"""SELECT d.hostid, r.jobid, d.runid, j.timeend, d.root_path, COUNT(*)
                FROM {self.TABLE_DIRECTORIES} d
                LEFT JOIN {self.TABLE_JOBS_RUNID} r ON r.runid = d.runid
                LEFT JOIN {self.TABLE_JOBS} j ON j.jobid = r.jobid AND j.hostid = d.hostid
                GROUP BY d.hostid, r.jobid, d.runid, j.timeend, d.root_path
                ORDER BY d.hostid, r.jobid, d.runid, d.root_path""")
            groups = {}
            for hostid, jobid, runid, timeend, root_path, count in cursor.fetchall():
                hostid = int(hostid)
                jobid = int(jobid) if jobid is not None else None
                record = groups.setdefault((hostid, jobid), dict(
                    hostid=hostid, jobid=jobid, runids=set(), timeend=timeend,
                    root_paths=set(), directory_count=0))
                record['directory_count'] += int(count)
                if root_path is not None:
                    record['root_paths'].add(root_path)
                if runid is not None:
                    record['runids'].add(int(runid))
            for record in groups.values():
                record['runids'] = sorted(record['runids'])
                record['root_paths'] = sorted(record['root_paths'])
            return sorted(groups.values(), key=lambda record: (
                -record['directory_count'], record['hostid'],
                record['jobid'] is None, record['jobid'] or 0))
        with self.connect() as connection:
            reader = connection.cursor()
            try:
                return self.list_directory_hostids(cursor=reader, include_jobs=include_jobs)
            finally:
                self._close_preserving_error(reader)

    def check_modifications(self, hostid=0):
        """Recreate the change report and flag changed local directory timestamps.

        First compare the directory st_mtime against time_inode_changed. Changed
        directories are recorded with subfolders_excluded without opening contents.
        Otherwise compare direct, included regular-file UTC modification times.
        No eligible files produce NULL. Missing directories mark the nearest
        existing inventoried parent. Permission errors still abort the check.
        A database lock serializes report rebuilds.
        """
        hostid = int(hostid)
        if not 0 <= hostid <= 32767:
            raise ValueError("Host-ID muss zwischen 0 und 32767 liegen.")
        absolute = self.list_live_excluded_absdirs(hostid)
        with self.connect() as connection:
            cursor = connection.cursor()
            locked = False
            try:
                while not locked:
                    cursor.execute("SELECT GET_LOCK(%s, 0)", ("dirwalker:tmp_modified",))
                    lock_row = cursor.fetchone()
                    if lock_row is None or lock_row[0] is None:
                        raise RuntimeError("Sperre für tmp_modified konnte nicht angelegt werden.")
                    locked = lock_row[0] == 1
                    if not locked:
                        time.sleep(0.5)
                self.create_modified_table(cursor)
                cursor.execute(f"DELETE FROM {self.TABLE_MODIFIED} WHERE batch_jobid=0 AND hostid=%s", (hostid,))
                cursor.execute(f"""SELECT d.dirid, d.runid, d.path, d.files_lastwrite,
                                          j.skipdirs, d.time_inode_changed
                    FROM {self.TABLE_DIRECTORIES} d
                    LEFT JOIN {self.TABLE_JOBS_RUNID} r
                      ON r.runid = COALESCE(NULLIF(d.finished, 0), d.runid)
                    LEFT JOIN {self.TABLE_JOBS} j ON j.jobid = r.jobid AND j.hostid = d.hostid
                    WHERE d.hostid = %s ORDER BY d.dirid""", (hostid,))
                rows = cursor.fetchall()
                by_path = {os.path.normpath(row[2]): row for row in rows}
                changes = {}
                for dirid, runid, path, previous, skipdirs, inode_previous in rows:
                    if skipdirs is None:
                        raise RuntimeError(
                            f"Scan-Ausschlüsse fehlen für Verzeichnis {dirid} (runid={runid}).")
                    excluded = exclusion_matcher(','.join([skipdirs, *absolute]))
                    if excluded(path):
                        continue
                    try:
                        info = os.stat(path, follow_symlinks=False)
                        if not stat.S_ISDIR(info.st_mode):
                            raise NotADirectoryError(path)
                        inode_current = datetime.fromtimestamp(
                            info.st_mtime, timezone.utc).replace(tzinfo=None)
                        if inode_current != inode_previous:
                            # Record a directory-only change without enumerating its contents.
                            changes[dirid] = (runid, inode_current, True)
                            continue
                        latest, vanished = self._modification_timestamp(path, excluded)
                    except (FileNotFoundError, NotADirectoryError) as missing:
                        parent = os.path.dirname(os.path.normpath(path))
                        while True:
                            parent_row = by_path.get(parent)
                            if parent_row is not None:
                                parent_id, parent_runid, _, _, parent_skip, _ = parent_row
                                if parent_skip is None:
                                    raise RuntimeError(
                                        f"Scan-Ausschlüsse fehlen für Verzeichnis {parent_id}.")
                                parent_excluded = exclusion_matcher(','.join([parent_skip, *absolute]))
                                if not parent_excluded(parent):
                                    try:
                                        timestamp, _ = self._modification_timestamp(parent, parent_excluded)
                                    except (FileNotFoundError, NotADirectoryError):
                                        pass
                                    else:
                                        changes[parent_id] = (parent_runid, timestamp, False)
                                        print(f"Verzeichnis fehlt oder ist kein Verzeichnis mehr: {path}; "
                                              f"Elternverzeichnis für Rescan markiert: {parent}", flush=True)
                                        break
                            ancestor = os.path.dirname(parent)
                            if ancestor == parent:
                                raise RuntimeError(
                                    f"Kein vorhandenes inventarisiertes Elternverzeichnis für {path}") from missing
                            parent = ancestor
                        continue
                    if latest != previous or vanished:
                        changes[dirid] = (runid, latest, False)
                for dirid, (runid, latest, local_only) in sorted(changes.items()):
                    cursor.execute(
                        f"INSERT INTO {self.TABLE_MODIFIED} (hostid, dirid, runid, newtimestamp, subfolders_excluded) "
                        "VALUES (%s, %s, %s, %s, %s)", (hostid, dirid, runid, latest, local_only))
                cursor.execute(f"""UPDATE {self.TABLE_DIRECTORIES} d
                    JOIN {self.TABLE_MODIFIED} m ON m.dirid = d.dirid AND m.runid = d.runid
                        AND m.hostid = d.hostid AND m.batch_jobid = 0
                    SET d.rescan_from_here = TRUE
                    WHERE d.hostid = %s""", (hostid,))
                connection.commit()
                return len(changes)
            except Exception:
                connection.rollback()
                raise
            finally:
                try:
                    if locked:
                        cursor.execute("SELECT RELEASE_LOCK(%s)", ("dirwalker:tmp_modified",))
                        cursor.fetchone()
                finally:
                    self._close_preserving_error(cursor)

    @staticmethod
    def _modification_timestamp(path, excluded):
        """Read eligible file mtimes; entry churn requests a rescan of this directory."""
        latest = None
        vanished = False
        with os.scandir(path) as entries:
            for entry in entries:
                if excluded(entry.path):
                    continue
                try:
                    info = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    vanished = True
                    continue
                if stat.S_ISREG(info.st_mode):
                    modified = datetime.fromtimestamp(
                        info.st_mtime, timezone.utc).replace(tzinfo=None)
                    if latest is None or modified > latest:
                        latest = modified
        return latest, vanished

    def directories_rescan_marked(self, hostid=0):
        """Coarsely rescan topmost marked trees and queue affected inventory roots.

        Preserve hashes of unchanged branches. Clear local hashes on timestamp or
        child-list changes and aggregate hashes up to the inventory root. Changes,
        detailed jobs and consumed marks commit together. Unreadable branches
        retain inventory for two failures, then prune descendants on the third;
        successful reads reset the consecutive counter. Other branches continue. Use current
        global exclusions alongside the stored job exclusions. Run without scanners.
        """
        hostid = int(hostid)
        if not 0 <= hostid <= 32767:
            raise ValueError("Host-ID muss zwischen 0 und 32767 liegen.")
        absolute = self.list_live_excluded_absdirs(hostid)
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                self.select_table_count(cursor, self.database, self.TABLE_EXCLUDED_NAMES)
                global_names = []
                if cursor.fetchone()[0]:
                    cursor.execute(f"SELECT name FROM {self.TABLE_EXCLUDED_NAMES} ORDER BY BINARY name")
                    global_names = [row[0] for row in cursor.fetchall()]
                cursor.execute(f"""SELECT d.dirid, d.parentdir, d.path, d.root_path,
                                          d.runid, d.files_lastwrite, d.rescan_from_here,
                                          j.skipdirs, d.read_failures
                    FROM {self.TABLE_DIRECTORIES} d
                    LEFT JOIN {self.TABLE_JOBS_RUNID} r
                      ON r.runid = COALESCE(NULLIF(d.finished, 0), d.runid)
                    LEFT JOIN {self.TABLE_JOBS} j ON j.jobid = r.jobid AND j.hostid = d.hostid
                    WHERE d.hostid = %s ORDER BY d.dirid""", (hostid,))
                rows = {os.path.normpath(row[2]): row for row in cursor.fetchall()}
                marked = {path for path, row in rows.items() if row[6]}
                children_by_id = {}
                for path, row in rows.items():
                    children_by_id.setdefault(row[1], set()).add(path)
                roots = []
                for path in sorted(marked):
                    parent = os.path.dirname(path)
                    while parent not in marked and parent != os.path.dirname(parent):
                        parent = os.path.dirname(parent)
                    if parent not in marked or path == parent:
                        roots.append(path)
                changed = set()
                removed = set()
                visited = set()
                blocked = set()
                jobs = {}
                for root in roots:
                    root_row = rows[root]
                    if root_row[7] is None:
                        raise RuntimeError(f"Scan-Ausschlüsse fehlen für Verzeichnis {root_row[0]}.")
                    excluded = exclusion_matcher(','.join([root_row[7], *global_names, *absolute]))
                    if excluded(root):
                        continue
                    pending = [root]
                    seen = set()
                    while pending:
                        path = pending.pop()
                        old = rows.get(path)
                        latest = None
                        children = []
                        try:
                            with os.scandir(path) as entries:
                                inode_time = datetime.fromtimestamp(
                                    os.stat(path, follow_symlinks=False).st_mtime, timezone.utc
                                ).replace(tzinfo=None)
                                for entry in entries:
                                    if excluded(entry.path):
                                        continue
                                    info = entry.stat(follow_symlinks=False)
                                    if stat.S_ISDIR(info.st_mode):
                                        children.append(os.path.normpath(entry.path))
                                    elif stat.S_ISREG(info.st_mode):
                                        modified = datetime.fromtimestamp(
                                            info.st_mtime, timezone.utc).replace(tzinfo=None)
                                        if latest is None or modified > latest:
                                            latest = modified
                        except PermissionError:
                            blocked.add(path)
                            if old is None:
                                parent = rows[os.path.dirname(path)]
                                cursor.execute(f"""INSERT INTO {self.TABLE_DIRECTORIES}
                                    (hostid, path_hash, root_path, runid, path, name,
                                     parentdir, read_failures, rescan_from_here)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, 1, TRUE)""",
                                    (hostid, hashlib.blake2b(path.encode(
                                        "utf-8", "surrogateescape"), digest_size=8).hexdigest(),
                                     root_row[3], root_row[4], path, os.path.basename(path), parent[0]))
                                rows[path] = (cursor.lastrowid, parent[0], path, root_row[3],
                                              root_row[4], None, True, root_row[7], 1)
                                failures = 1
                            else:
                                failures = int(old[8] or 0) + 1
                                cursor.execute(f"UPDATE {self.TABLE_DIRECTORIES} "
                                               "SET read_failures = COALESCE(read_failures, 0) + 1 "
                                               "WHERE dirid = %s AND hostid = %s", (old[0], hostid))
                            if failures >= 3:
                                # Retain a retryable boundary, never fabricate a readable empty tree.
                                seen.add(path)
                                changed.add(path)
                                print(f"Verzeichnis {failures}-mal nicht lesbar; "
                                      f"Unterverzeichnisse werden aus der Datenbank entfernt: {path}", flush=True)
                            else:
                                seen.update(candidate for candidate in rows
                                            if self._rescan_path_within(path, candidate))
                                print(f"Verzeichnis nicht lesbar ({failures}/3); Teilbaum bleibt erhalten "
                                      f"und für erneuten Rescan markiert: {path}", flush=True)
                            continue
                        seen.add(path)
                        visited.add(path)
                        pending.extend(sorted(children, reverse=True))
                        old_children = children_by_id.get(old[0], set()) if old else set()
                        if old is None:
                            parent = rows[os.path.dirname(path)]
                            cursor.execute(f"""INSERT INTO {self.TABLE_DIRECTORIES}
                                (hostid, path_hash, root_path, runid, path, name,
                                 parentdir, files_lastwrite, time_inode_changed)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                (hostid, hashlib.blake2b(os.path.normpath(path).encode(
                                     "utf-8", "surrogateescape"), digest_size=8).hexdigest(), root_row[3], root_row[4], path,
                                 os.path.basename(path), parent[0], latest, inode_time))
                            rows[path] = (cursor.lastrowid, parent[0], path, root_row[3],
                                          root_row[4], latest, False, root_row[7], 0)
                        else:
                            cursor.execute(f"UPDATE {self.TABLE_DIRECTORIES} "
                                           "SET files_lastwrite = %s, time_inode_changed = %s, read_failures = 0 WHERE dirid = %s AND hostid = %s",
                                           (latest, inode_time, old[0], hostid))
                        if old is None or latest != old[5] or set(children) != old_children:
                            changed.add(path)
                    removed.update(path for path in rows
                                   if self._rescan_path_within(root, path) and path not in seen)
                # A removed descendant invalidates its surviving ancestors too.
                changed.update(removed)
                aggregate = set()
                for candidate in changed:
                    path = candidate
                    while True:
                        if path in rows:
                            aggregate.add(path)
                        parent = os.path.dirname(path)
                        if parent == path:
                            break
                        path = parent
                for path in sorted(removed, key=len, reverse=True):
                    cursor.execute(f"DELETE FROM {self.TABLE_DIRECTORIES} "
                                   "WHERE dirid = %s AND hostid = %s", (rows[path][0], hostid))
                for path in sorted(aggregate - removed):
                    row = rows[path]
                    # Ancestors retain valid local file hashes; only their subtree changed.
                    local = ("file_list_sha256 = NULL, files_sha256 = NULL, "
                             "localfilesize = NULL, localfilecount = NULL, "
                             "emptyfilecount = NULL, subdircount = NULL, "
                             "otherentrycount = NULL, " if path in changed else "")
                    cursor.execute(f"""UPDATE {self.TABLE_DIRECTORIES} SET {local}
                        subdirs_sha256 = NULL, combined_sha256 = NULL,
                        subdirectory_hash = NULL, subtree_size_bytes = NULL,
                        subtree_structure = NULL, finished = 0, zero_content_only = FALSE
                        WHERE dirid = %s AND hostid = %s""", (row[0], hostid))
                    owner = os.path.normpath(row[3])
                    owner_row = rows.get(owner)
                    if owner_row is None or owner_row[7] is None:
                        raise RuntimeError(f"Scan-Wurzel oder Ausschlüsse fehlen: {owner}")
                    jobs[owner] = ','.join([owner_row[7], *global_names])
                now = self.utc_now
                for root, skipdirs in sorted(jobs.items()):
                    cursor.execute(f"""INSERT INTO {self.TABLE_JOBS}
                        (userhost, hostid, startdirname, task, status, stage, skipdirs,
                         scanall, timecreated, timeupdated, last_activity)
                        VALUES (%s, %s, %s, 'scan', 'insert', 'detailed', %s, 1, %s, %s, %s)""",
                        (socket.gethostname(), hostid, root, skipdirs, now, now, now))
                retry = {path for path in rows if any(
                    self._rescan_path_within(path, denied) for denied in blocked)}
                for path in sorted(retry - removed):
                    cursor.execute(f"UPDATE {self.TABLE_DIRECTORIES} "
                                   "SET rescan_from_here = TRUE WHERE dirid = %s AND hostid = %s",
                                   (rows[path][0], hostid))
                for path in sorted(visited - removed - retry):
                    cursor.execute(f"UPDATE {self.TABLE_DIRECTORIES} "
                                   "SET rescan_from_here = FALSE WHERE dirid = %s AND hostid = %s",
                                   (rows[path][0], hostid))
                self.ensure_combined_hash_readiness(hostid, cursor=cursor)
                completed = (visited | removed) - retry
                self.select_table_count(cursor, self.database, self.TABLE_MODIFIED)
                if cursor.fetchone()[0]:
                    ids = [rows[path][0] for path in sorted(completed)]
                    for offset in range(0,len(ids),200):
                        group=ids[offset:offset+200]
                        placeholders=','.join(['%s']*len(group))
                        cursor.execute(f"DELETE FROM {self.TABLE_MODIFIED} "
                                       f"WHERE batch_jobid=0 AND hostid=%s AND dirid IN ({placeholders})",
                                       (hostid,)+tuple(group))
                connection.commit()
                return len(roots), len(jobs)
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

    @staticmethod
    def _rescan_path_within(root, path):
        """Match path components rather than string prefixes."""
        try:
            return os.path.commonpath((root, path)) == root
        except ValueError:
            return False

    def __init__(self):
        self.host = os.environ.get("DB_HOST", "localhost")
        self.port = int(os.environ.get("DB_PORT", "3306"))
        self.database = os.environ.get("DB_NAME", "filestatus")
        self.user = os.environ.get("DB_USER")
        self.password = os.environ.get("DB_PASSWORD")
        # Probe at initialization; unavailable DBs should not prevent --help or init-jobs.
        try:
            with self.connect():
                pass
        except Exception:
            pass


    @property
    def utc_now(self):
        """Aktuelle UTC-Zeit als naive DATETIME-kompatible Zeit."""
        return datetime.now(timezone.utc).replace(tzinfo=None)


    def write_run_record(
        self, cursor, runid, jobid, task, status, stage, *, timecreated=None,
        pid=None, process_host=None, process_start=None, remotehost=None,
    ):
        """Write every jobsrunid entry with a complete process identity.

        Default to the calling process; a parent registering its child supplies
        the child's identity explicitly. The caller owns allocation, transaction
        commit/rollback and retry, so history and the jobs update remain atomic.
        """
        pid = os.getpid() if pid is None else int(pid)
        if pid <= 0:
            raise ValueError('Eine positive PID ist für jobsrunid erforderlich.')
        process_host = socket.gethostname() if process_host is None else process_host
        process_start = _process_start_token(pid) if process_start is None else process_start
        if not process_host or not process_start:
            raise RuntimeError(f'Unvollständige Prozesskennung für PID {pid}; jobsrunid nicht geschrieben.')
        stamp = self.utc_now if timecreated is None else timecreated
        cursor.execute(
            f"INSERT INTO {self.TABLE_JOBS_RUNID} "
            "(runid, jobid, task, status, stage, timecreated, pid, process_host, process_start, remotehost) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (runid, jobid, task, status, stage, stamp, pid, process_host, process_start, remotehost),
        )


    @staticmethod
    def _rollback_preserving_error(connection):
        """A lost connection must not hide the operation's original exception."""
        try:
            connection.rollback()
        except Exception as error:
            logging.warning('Rollback fehlgeschlagen; ursprünglicher Fehler bleibt erhalten: %s', error)

    @staticmethod
    def _close_preserving_error(resource):
        """Report cleanup failures without replacing an exception already in flight."""
        original_error = sys.exc_info()[0] is not None
        try:
            resource.close()
        except Exception as error:
            if not original_error:
                raise
            logging.warning('Schließen fehlgeschlagen; ursprünglicher Fehler bleibt erhalten: %s', error)

    @staticmethod
    def _zero_content_tree_ids(rows):
        """Prove readable zero-content trees bottom-up, preserving unknown/special entries.

        Rows: dirid, parentdir, hostid, localfilecount, subdircount, nopermdirs,
        emptyfilecount, otherentrycount, localfilesize. All regular files must
        be readable zero-byte files; all recorded children must also qualify.
        """
        nodes = {(row[2], row[0]): row for row in rows}
        children = {key: 0 for key in nodes}
        for key, row in nodes.items():
            parent = (row[2], row[1])
            if parent in children:
                children[parent] += 1
        empty = {key: row[3] is not None and row[6] == row[3]
                 and row[4] == children[key] and row[5] == 0
                 and row[7] == 0 and row[8] == 0
                 for key, row in nodes.items()}
        remaining = dict(children)
        ready = deque(key for key, count in remaining.items() if count == 0)
        result = set()
        while ready:
            key = ready.popleft()
            if empty[key]:
                result.add(key)
            row = nodes[key]
            parent = (row[2], row[1])
            if parent in nodes:
                empty[parent] = empty[parent] and empty[key]
                remaining[parent] -= 1
                if remaining[parent] == 0:
                    ready.append(parent)
        return result

    def zero_content_directory_ids(self, hostid=None):
        """Read the persisted exclusion flag without loading paths or tree metadata."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(f'SELECT hostid, dirid FROM {self.TABLE_DIRECTORIES} '
                               'WHERE zero_content_only = TRUE'
                               + (' AND hostid = %s' if hostid is not None else ''),
                               (hostid,) if hostid is not None else ())
                return set(cursor.fetchall())
            finally:
                cursor.close()

    def invalidate_zero_content_flags(self, hostid):
        """Keep results visible while a host's scan metadata is being changed."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(f'UPDATE {self.TABLE_DIRECTORIES} SET zero_content_only = FALSE '
                               'WHERE hostid = %s AND zero_content_only = TRUE', (hostid,))
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

    def refresh_zero_content_flags(self, hostid=None):
        """Backfill derived flags in short batches; unknown data remains visible."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f'SELECT dirid, parentdir, hostid, localfilecount, subdircount, nopermdirs, '
                    f'emptyfilecount, otherentrycount, localfilesize, zero_content_only '
                    f'FROM {self.TABLE_DIRECTORIES}'
                    + (' WHERE hostid = %s' if hostid is not None else ''),
                    (hostid,) if hostid is not None else ())
                rows = cursor.fetchall()
                excluded = self._zero_content_tree_ids(rows)
                changes = [(int((row[2], row[0]) in excluded), row[2], row[0])
                           for row in rows if bool(row[9]) != ((row[2], row[0]) in excluded)]
                connection.commit()
                for offset in range(0, len(changes), 200):
                    cursor.executemany(f'UPDATE {self.TABLE_DIRECTORIES} SET zero_content_only = %s '
                                       'WHERE hostid = %s AND dirid = %s', changes[offset:offset+200])
                    connection.commit()
                return len(changes)
            except Exception:
                connection.rollback()
                raise
            finally:
                cursor.close()

    def _repair_directory_parents(self, connection, cursor, hostid, root_path, root_id, batch_size=200):
        """Restore parents in O(n) path lookups and short primary-key write batches.

        Read only this host/root once, preserve binary path matching, and update
        only changed rows. A failed batch can be rerun: earlier committed repairs
        remain valid and queue creation does not start until repair completes.
        """
        if batch_size < 1:
            raise ValueError('batch_size muss positiv sein.')
        cursor.execute(
            f"SELECT dirid, path, parentdir FROM {self.TABLE_DIRECTORIES} "
            "WHERE hostid = %s AND root_path = %s ORDER BY dirid",
            (hostid, root_path),
        )
        rows = cursor.fetchall()
        by_path = {path: dirid for dirid, path, _parent in rows}
        changed = []
        for dirid, path, old_parent in rows:
            parent = 0 if dirid == root_id else by_path.get(os.path.dirname(path), 0)
            if old_parent != parent:
                changed.append((parent, dirid, hostid, root_path))
        # Release the read transaction and any preceding ownership changes before
        # the first small repair transaction; no long self-join update is used.
        connection.commit()
        for offset in range(0, len(changed), batch_size):
            cursor.executemany(
                f"UPDATE {self.TABLE_DIRECTORIES} SET parentdir = %s "
                "WHERE dirid = %s AND hostid = %s AND root_path = %s",
                changed[offset:offset + batch_size],
            )
            connection.commit()
        return len(changed)

    def local_start_jobs(self, hostids):
        """Read unfinished/error jobs for positive local host IDs without changing them."""
        hostids = tuple(sorted(set(int(value) for value in hostids)))
        if not hostids or any(not 1 <= value <= 32767 for value in hostids):
            raise ValueError('Positive Host-IDs sind für die lokale Startprüfung erforderlich.')
        fields = ('jobid', 'hostid', 'status', 'stage', 'startdirname', 'errmsg', 'usererror')
        placeholders = ','.join(['%s'] * len(hostids))
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT {', '.join(fields)} FROM {self.TABLE_JOBS} "
                    f"WHERE hostid IN ({placeholders}) "
                    "AND status IN ('error', 'stale', 'active', 'paused', 'insert', 'pending') "
                    "ORDER BY jobid", hostids,
                )
                return [dict(zip(fields, row)) for row in cursor.fetchall()]
            finally:
                cursor.close()

    def _tag_connection(self, connection):
        scope = os.environ.get('DIRWALKER_SERVER_SCOPE')
        if not scope:
            return connection
        pid = os.getpid()
        cached = getattr(self, '_sql_tag_cache', None)
        if cached is not None and cached[:2] == (scope, pid):
            return _TaggedConnection(connection, cached[2])
        identity = json.loads(scope)
        identity.update(pid=pid, start=_process_start_token(pid))
        tag = '/*dirwalker:' + json.dumps(identity, separators=(',', ':')).encode().hex() + '*/ '
        self._sql_tag_cache = (scope, pid, tag)
        return _TaggedConnection(connection, tag)

    def cleanup_server_queries(self, hostid, group=None, timeout=20, confirm=False):
        """Kill only identified local sessions; disappearance confirms rollback completion.

        A dead client can leave a running query behind. Legacy untagged queries
        cannot safely be attributed to a host ID and are reported, never guessed.
        KILL CONNECTION also rolls back its transaction, unlike KILL QUERY.
        """
        deadline = time.monotonic() + timeout
        # Bypass tagging: this control connection must never select itself.
        connection = self._control_connection()
        cursor = connection.cursor()
        pending = set()
        unassigned = []
        owned = []
        try:
            cursor.execute('SELECT CONNECTION_ID()')
            own_id = int(cursor.fetchone()[0])
            cursor.execute('SHOW FULL PROCESSLIST')
            rows = cursor.fetchall()
            for row in rows:
                connection_id, user, _host, database, command, _elapsed, _state, info = row[:8]
                if int(connection_id) == own_id or database != self.database or user != self.user:
                    continue
                match = re.match(r'/\*dirwalker:([0-9a-f]+)\*/', info or '')
                if not match:
                    if group is None and command == 'Query' and info:
                        unassigned.append(int(connection_id))
                    continue
                try:
                    identity = json.loads(bytes.fromhex(match.group(1)).decode())
                    if identity['hostid'] != hostid or identity['machine'] != socket.gethostname():
                        continue
                    if group is not None:
                        if identity['group'] != group:
                            continue
                    elif not confirm and _process_start_token(identity['pid']) == identity['start']:
                        raise RuntimeError('DB-Abfrage {} gehört noch zu einem lebenden Prozess (PID {}).'.format(
                            connection_id, identity['pid']))
                except (KeyError, ValueError, TypeError):
                    if group is None:
                        unassigned.append(int(connection_id))
                    continue
                owned.append((int(connection_id), identity))
            if unassigned:
                for connection_id in unassigned:
                    print('KILL CONNECTION {};'.format(connection_id), flush=True)
                return False
            for connection_id, identity in owned:
                if confirm:
                    try:
                        answer = input('DB-Verbindung {} gehört zu Host-ID {} auf diesem Rechner '
                                       '(PID {}). Beenden und Rollback abwarten? [yN] '.format(
                                           connection_id, hostid, identity['pid'])).strip().lower()
                    except EOFError:
                        answer = ''
                    if answer != 'y':
                        return False
            for connection_id, _identity in owned:
                print('Beende DB-Verbindung {}; warte auf Rollback.'.format(connection_id), flush=True)
                try:
                    cursor.execute('KILL CONNECTION {}'.format(int(connection_id)))
                except Error as error:
                    if getattr(error, 'errno', None) != 1094:
                        raise
                pending.add(int(connection_id))
            deadline = time.monotonic() + timeout
            while pending:
                cursor.execute('SHOW FULL PROCESSLIST')
                pending.intersection_update(int(row[0]) for row in cursor.fetchall())
                if not pending:
                    print('DB-Verbindungen geschlossen; Rollback abgeschlossen.', flush=True)
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError('Rollback nach {}s noch nicht abgeschlossen: DB-Verbindungen {}. '
                                       'Kein neuer Scan gestartet.'.format(timeout, sorted(pending)))
                time.sleep(min(0.2, max(0, deadline - time.monotonic())))
            return True
        finally:
            cursor.close()
            connection.close()

    def _control_connection(self):
        self.validate_config()
        options = dict(host=self.host, port=self.port, database=self.database,
                       user=self.user, password=self.password, autocommit=True,
                       connect_timeout=5)
        return self._open_connection(options)

    def _open_connection(self, options, timeout_multiplier=1):
        """Fresh transport every time, with bounded reads/writes on Python 3.8."""
        options = dict(options)
        options['connect_timeout'] = options.get('connect_timeout', 10) * timeout_multiplier
        io_timeout = DB_IO_TIMEOUT * timeout_multiplier
        if DATABASE_DRIVER == 'MySQL Connector/Python':
            # 8.4 has no read_timeout/write_timeout options. Pin its Python
            # implementation so its actual socket can retain an I/O timeout.
            options['use_pure'] = True
            options['charset'] = 'utf8mb4'
        else:
            options.update(read_timeout=io_timeout, write_timeout=io_timeout)
        connection = _database_connect(**options)
        if DATABASE_DRIVER == 'MySQL Connector/Python':
            try:
                transport = connection._socket.sock
                transport.settimeout(io_timeout)
                self._track_socket(transport)
            except Exception:
                connection.close()
                raise
        return connection

    def _track_socket(self, transport):
        if not hasattr(self, '_network_sockets'):
            self._network_sockets = weakref.WeakSet()
            self._network_lock = threading.Lock()
        with self._network_lock:
            self._network_sockets.add(transport)

    def _discard_network_sockets(self):
        """Unblock old transports without issuing SQL on sleeping connections."""
        if not hasattr(self, '_network_sockets'):
            return
        with self._network_lock:
            sockets = list(self._network_sockets)
        for transport in sockets:
            try:
                transport.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            # The owning worker closes the descriptor during normal cleanup.
            # Closing it in this thread races with connector reads/rollback.

    def start_wake_monitor(self, on_wake=None):
        """Detect the first scheduled tick after suspend or a long process pause."""
        if getattr(self, '_wake_thread', None) is not None:
            return
        self._wake_event = threading.Event()
        self._wake_stop = threading.Event()
        def monitor():
            previous = time.time()
            while not self._wake_stop.wait(0.5):
                now = time.time()
                gap = now - previous
                previous = now
                if gap > 5:
                    self._discard_network_sockets()
                    self._wake_event.set()
                    if on_wake is not None:
                        on_wake(gap)
        self._wake_thread = threading.Thread(target=monitor, daemon=True, name='database-wake-monitor')
        self._wake_thread.start()

    def stop_wake_monitor(self):
        if getattr(self, '_wake_thread', None) is not None:
            self._wake_stop.set()
            self._wake_thread.join(timeout=1)
            self._wake_thread = None

    def consume_wake_event(self):
        event = getattr(self, '_wake_event', None)
        if event is None or not event.is_set():
            return False
        event.clear()
        return True

    def validate_config(self):
        if not self.user or self.password is None:
            raise RuntimeError("DB_USER und DB_PASSWORD müssen gesetzt sein.")


    def connect_server(self):
        """Eine neue Verbindung ohne ausgewählte Datenbank öffnen."""
        self.validate_config()
        options = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "autocommit": False,
            "connect_timeout": 10,
        }
        return self._open_connection(options)


    def connect(self, timeout_multiplier=1):
        """Für jeden Aufruf einen neuen, mit ``with`` verwendbaren Handle liefern."""
        self.validate_config()
        options = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "autocommit": False,
            "connect_timeout": 10,
        }
        return self._tag_connection(self._open_connection(options, timeout_multiplier=timeout_multiplier))


    def connect_database(self):
        """Alias für ``connect``; jeder Aufruf öffnet eine neue Verbindung."""
        return self.connect()

    def binarycheck_exclusion_matcher(self, jobid, hostid):
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(f"SELECT skipdirs FROM {self.TABLE_JOBS} WHERE jobid = %s", (jobid,))
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError('Binarycheck job no longer exists.')
                skipdirs = row[0] or ''
                cursor.execute(f"SELECT path FROM {self.TABLE_LIVE_EXCLUDED_ABSDIRS} WHERE hostid = %s", (hostid,))
                paths = [value[0] for value in cursor.fetchall()]
                return exclusion_matcher(','.join([skipdirs, *paths]))
            finally:
                self._close_preserving_error(cursor)


    def remote_binarycheck_runs(self, hostid):
        """Only current active runs authorize remote filesystem work."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT r.runid, j.jobid, j.hostid FROM {self.TABLE_JOBS_RUNID} r "
                    f"JOIN {self.TABLE_JOBS} j ON j.jobid = r.jobid AND j.runid = r.runid "
                    "WHERE r.remotehost = %s AND r.stage = 'listening' AND j.hostid <> %s "
                    "AND j.status = 'active' AND j.task = 'binarycheck'",
                    (hostid, hostid),
                )
                return cursor.fetchall()
            finally:
                self._close_preserving_error(cursor)

    def register_remote_listener_run(self, hostid):
        """One audit run for the entire listener PID, never one per request."""
        with self.connect() as connection:
            cursor = connection.cursor()
            lock_name = None
            try:
                lock_name, runid = self._acquire_runid(cursor)
                cursor.execute(
                    f"SELECT j.jobid, j.hostid FROM {self.TABLE_JOBS} j "
                    f"JOIN {self.TABLE_JOBS_RUNID} r ON r.runid = j.runid "
                    "WHERE r.remotehost = %s AND r.stage = 'listening' AND j.status = 'active' AND j.task = 'binarycheck' "
                    "AND j.hostid <> %s ORDER BY j.jobid LIMIT 1",
                    (hostid, hostid),
                )
                row = cursor.fetchone()
                if row is None:
                    connection.rollback()
                    return None
                self.write_run_record(cursor, runid, row[0], 'binarycheck', 'active', 'listener',
                                      remotehost=row[1])
                connection.commit()
                return runid
            except BaseException:
                self._rollback_preserving_error(connection)
                raise
            finally:
                if lock_name is not None:
                    try:
                        cursor.execute('SELECT RELEASE_LOCK(%s)', (lock_name,))
                        cursor.fetchone()
                    except Exception:
                        pass
                self._close_preserving_error(cursor)

    def finish_remote_listener_run(self, runid):
        """Close the same listener audit record without allocating another run."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"UPDATE {self.TABLE_JOBS_RUNID} SET status = 'closed' "
                    "WHERE runid = %s AND stage = 'listener' AND pid = %s",
                    (runid, os.getpid()),
                )
                connection.commit()
            finally:
                self._close_preserving_error(cursor)

    def register_remote_binarycheck_run(self, jobid, hostid, remotehost):
        """Allocate a fresh advertised run and atomically attach it to the job."""
        with self.connect() as connection:
            cursor = connection.cursor()
            lock_name = None
            try:
                lock_name, runid = self._acquire_runid(cursor)
                cursor.execute(
                    f"SELECT runid FROM {self.TABLE_JOBS} WHERE jobid = %s "
                    "AND hostid = %s AND task = 'binarycheck' AND status = 'active' FOR UPDATE",
                    (jobid, hostid),
                )
                if cursor.fetchone() is None:
                    raise RuntimeError('Remote binarycheck job is no longer active.')
                self.write_run_record(cursor, runid, jobid, 'binarycheck', 'active', 'listening',
                                      remotehost=int(remotehost))
                cursor.execute(
                    f"UPDATE {self.TABLE_JOBS} SET runid = %s, last_activity = %s, timeupdated = %s "
                    "WHERE jobid = %s AND hostid = %s AND status = 'active'",
                    (runid, self.utc_now, self.utc_now, jobid, hostid),
                )
                connection.commit()
                return runid
            except BaseException:
                self._rollback_preserving_error(connection)
                raise
            finally:
                if lock_name is not None:
                    try:
                        cursor.execute('SELECT RELEASE_LOCK(%s)', (lock_name,))
                        cursor.fetchone()
                    except Exception:
                        pass
                self._close_preserving_error(cursor)

    def _remote_heartbeat(self, jobid, runid, hostid):
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT 1 FROM {self.TABLE_JOBS} WHERE jobid = %s AND runid = %s "
                    "AND hostid = %s AND status = 'active' AND task = 'binarycheck'",
                    (jobid, runid, hostid),
                )
                if cursor.fetchone() is None:
                    raise InterruptedError('Remote binarycheck was stopped or superseded.')
                cursor.execute(
                    f"UPDATE {self.TABLE_JOBS} SET last_activity = %s WHERE jobid = %s AND runid = %s "
                    "AND hostid = %s AND status = 'active'",
                    (self.utc_now, jobid, runid, hostid),
                )
                connection.commit()
            finally:
                self._close_preserving_error(cursor)

    def _remote_request(self, jobid, runid, hostid, dirid):
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "INSERT INTO tmp_remotedoubles (jobid, runid, hostid, dirid, timeupdated, algorithm) "
                    "VALUES (%s, %s, %s, %s, %s, 'sha256-tree-v2') "
                    "ON DUPLICATE KEY UPDATE checksum = NULL, error_message = NULL, algorithm = VALUES(algorithm), "
                    "status = 'pending', claim_token = NULL, timeupdated = VALUES(timeupdated)",
                    (jobid, runid, hostid, dirid, self.utc_now),
                )
                connection.commit()
            finally:
                self._close_preserving_error(cursor)

    def _remote_response(self, runid, hostid, dirid):
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT status, checksum, error_message, algorithm FROM tmp_remotedoubles "
                    "WHERE runid = %s AND hostid = %s AND dirid = %s",
                    (runid, hostid, dirid),
                )
                return cursor.fetchone()
            finally:
                self._close_preserving_error(cursor)

    def _save_remote_result(self, jobid, runid, hostid, remotehost, left, right, status, error=None):
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"UPDATE {self.TABLE_DOUBLE_JOBS} p JOIN {self.TABLE_JOBS} j ON j.jobid = p.jobid "
                    "SET p.status = %s, p.verification_method = 'direct', p.finished_at = %s, "
                    "p.error_message = %s, p.runid = %s "
                    "WHERE p.jobid = %s AND p.hostid = %s AND p.hostidB = %s "
                    "AND p.left_dirid = %s AND p.right_dirid = %s AND j.runid = %s "
                    "AND j.status = 'active' AND p.status IN ('pending', 'unreadable')",
                    (status, self.utc_now, error, runid, jobid, hostid, remotehost, left, right, runid),
                )
                if status in ('equal','different') and cursor.rowcount:
                    self.mark_binarycheck_hashed(cursor,jobid,[(hostid,left),(remotehost,right)],self.utc_now)
                connection.commit()
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                self._close_preserving_error(cursor)

    def run_remote_binarycheck(self, jobid, hostid=None):
        """Handle cross-host jobs; return False for an ordinary same-host job."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT p.hostid, p.hostidB, p.left_dirid, p.right_dirid, d.path "
                    f"FROM {self.TABLE_DOUBLE_JOBS} p LEFT JOIN {self.TABLE_DIRECTORIES} d "
                    "ON d.hostid = p.hostid AND d.dirid = p.left_dirid "
                    f"JOIN {self.TABLE_JOBS} j ON j.jobid = p.jobid "
                    "WHERE p.jobid = %s AND p.hostid <> p.hostidB AND j.status = 'active' "
                    "AND p.is_requested = TRUE AND p.status IN ('pending', 'unreadable') ORDER BY p.hostidB, p.left_dirid, p.right_dirid",
                    (jobid,),
                )
                pairs = cursor.fetchall()
            finally:
                self._close_preserving_error(cursor)
        if not pairs:
            return False
        local_host, remote_host = int(pairs[0][0]), int(pairs[0][1])
        if (local_host <= 0 or hostid != local_host
                or any(int(pair[0]) != local_host for pair in pairs)):
            raise RuntimeError('Cross-host binarycheck requires its owning localserver.')
        is_excluded = self.binarycheck_exclusion_matcher(jobid, local_host)
        runid = self.register_remote_binarycheck_run(jobid, local_host, remote_host)
        next_heartbeat = [0.0]

        def progress():
            now = time.monotonic()
            if now >= next_heartbeat[0]:
                self._remote_heartbeat(jobid, runid, local_host)
                next_heartbeat[0] = now + 5

        wait_seconds = max(30, int(os.environ.get('DIRWALKER_REMOTE_TIMEOUT', '3600')))
        for _, pair_remote, left, right, path in pairs:
            if int(pair_remote) != remote_host:
                remote_host = int(pair_remote)
                runid = self.register_remote_binarycheck_run(jobid, local_host, remote_host)
                next_heartbeat[0] = 0.0
            progress()
            self._remote_request(jobid, runid, remote_host, right)
            status, error = 'unreadable', None
            try:
                if path is None:
                    raise OSError('Local directory record is missing.')
                local_digest = directory_content_checksum(path, progress=progress, is_excluded=is_excluded)
                deadline = time.monotonic() + wait_seconds
                while True:
                    progress()
                    response = self._remote_response(runid, remote_host, right)
                    if response and response[0] == 'error':
                        raise OSError(response[2] or 'Remote checksum failed.')
                    if response and response[0] == 'done':
                        if response[3] != 'sha256-tree-v2' or not re.fullmatch('[0-9a-f]{64}', response[1] or ''):
                            raise OSError('Invalid remote checksum or algorithm.')
                        status = 'equal' if local_digest == response[1] else 'different'
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError('Timed out waiting for remote checksum.')
                    time.sleep(1)
            except InterruptedError:
                raise
            except (OSError, ValueError) as exc:
                if is_network_error(exc) and not isinstance(exc, TimeoutError):
                    raise
                error = str(exc)
            self._save_remote_result(jobid, runid, local_host, remote_host, left, right, status, error)
            print(f'Job {jobid}: Host {local_host}:{left} <-> Host {remote_host}:{right}: {status}', flush=True)
        return True

    def serve_remote_checksum(self, hostid, listener_runid):
        """Claim one request with a renewable lease, hash locally, publish by token."""
        token = uuid.uuid4().hex
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT q.runid, q.jobid, q.dirid, d.path FROM tmp_remotedoubles q "
                    f"JOIN {self.TABLE_JOBS_RUNID} r ON r.runid = q.runid AND r.remotehost = q.hostid AND r.stage = 'listening' "
                    f"JOIN {self.TABLE_JOBS} j ON j.jobid = q.jobid AND j.runid = q.runid "
                    f"LEFT JOIN {self.TABLE_DIRECTORIES} d ON d.hostid = q.hostid AND d.dirid = q.dirid "
                    "WHERE q.hostid = %s AND j.status = 'active' AND j.task = 'binarycheck' "
                    "AND j.hostid <> q.hostid AND q.algorithm = 'sha256-tree-v2' "
                    "AND (q.status = 'pending' OR (q.status = 'active' "
                    "AND q.timeupdated < UTC_TIMESTAMP(6) - INTERVAL 60 SECOND)) "
                    "AND EXISTS (SELECT 1 FROM " + self.TABLE_DOUBLE_JOBS + " p "
                    "WHERE p.jobid = q.jobid AND p.hostid = j.hostid AND p.hostidB = q.hostid "
                    "AND p.right_dirid = q.dirid AND p.is_requested = TRUE) "
                    "ORDER BY q.runid, q.dirid LIMIT 1 FOR UPDATE",
                    (hostid,),
                )
                request = cursor.fetchone()
                if request is None:
                    connection.commit()
                    return False
                runid, jobid, dirid, path = request
                cursor.execute(
                    "UPDATE tmp_remotedoubles SET status = 'active', claim_token = %s, listener_runid = %s, "
                    "checksum = NULL, error_message = NULL, timeupdated = %s "
                    "WHERE runid = %s AND hostid = %s AND dirid = %s",
                    (token, listener_runid, self.utc_now, runid, hostid, dirid),
                )
                connection.commit()
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                self._close_preserving_error(cursor)
        next_renewal = [0.0]

        def renew():
            now = time.monotonic()
            if now < next_renewal[0]:
                return
            with self.connect() as connection:
                cursor = connection.cursor()
                try:
                    cursor.execute(
                        "UPDATE tmp_remotedoubles q JOIN " + self.TABLE_JOBS + " j "
                        "ON j.jobid = q.jobid AND j.runid = q.runid "
                        "SET q.timeupdated = %s WHERE q.runid = %s AND q.hostid = %s "
                        "AND q.dirid = %s AND q.claim_token = %s AND q.status = 'active' "
                        "AND j.status = 'active'",
                        (self.utc_now, runid, hostid, dirid, token),
                    )
                    if cursor.rowcount != 1:
                        raise InterruptedError('Remote request stopped, reassigned or superseded.')
                    connection.commit()
                finally:
                    self._close_preserving_error(cursor)
            next_renewal[0] = now + 5
        checksum, error = None, None
        try:
            if path is None:
                raise OSError('Remote directory record is missing.')
            checksum = directory_content_checksum(path, progress=renew,
                                                  is_excluded=self.binarycheck_exclusion_matcher(jobid, hostid))
        except InterruptedError:
            return True
        except OSError as exc:
            error = str(exc)
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "UPDATE tmp_remotedoubles q JOIN " + self.TABLE_JOBS + " j "
                    "ON j.jobid = q.jobid AND j.runid = q.runid "
                    "SET q.status = %s, q.checksum = %s, q.error_message = %s, q.timeupdated = %s "
                    "WHERE q.runid = %s AND q.hostid = %s AND q.dirid = %s "
                    "AND q.claim_token = %s AND q.status = 'active' AND j.status = 'active'",
                    ('error' if error else 'done', checksum, error, self.utc_now,
                     runid, hostid, dirid, token),
                )
                connection.commit()
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                self._close_preserving_error(cursor)
        return True


class AgeRescanWorkflow:
    """Spawn-worker stages firstscan, fixunreadable and workmodified.

    Every report row belongs to this job and host. The server advances the same
    job through firstscan, fixunreadable and workmodified; no child jobs are created.
    Only workmodified changes hashes, scan ownership and coarse timestamps.
    """

    TASKS = ('rescan-after-days',)

    def __init__(self, database, note=None):
        self.db = database
        self.note = note or (lambda message: print(message,flush=True))
        self.notes = []

    def run(self, jobid, hostid):
        with self.db.connect() as connection:
            cursor = connection.cursor(dictionary=True)
            try:
                cursor.execute(f"SELECT * FROM {self.db.TABLE_JOBS} WHERE jobid=%s AND hostid=%s",
                               (int(jobid),int(hostid)))
                self.connection = connection
                self.cursor = cursor
                self.job = cursor.fetchone()
                if not self.job or self.job['task'] not in self.TASKS or self.job['status'] != 'active':
                    raise RuntimeError(f'Wartungsjob {jobid} ist nicht aktiv oder gehört einem anderen Host.')
                self.next_progress = time.monotonic() + 5
                self.hostid = int(hostid)
                self.batch = int(jobid)
                self.source = self.job.get('source_jobid')
                if self.source is None:
                    raise RuntimeError('Quelljob der Schnellprüfung fehlt.')
                cursor.execute(f"SELECT skipdirs FROM {self.db.TABLE_JOBS} WHERE jobid = %s", (self.source,))
                source_job = cursor.fetchone()
                if source_job is None:
                    raise RuntimeError(f'Quelljob {self.source} fehlt.')
                names = []
                self.db.select_table_count(cursor, self.db.database, self.db.TABLE_EXCLUDED_NAMES)
                if next(iter(cursor.fetchone().values())):
                    cursor.execute(f"SELECT name FROM {self.db.TABLE_EXCLUDED_NAMES}")
                    names = [row['name'] for row in cursor.fetchall()]
                paths = self.db.list_live_excluded_absdirs(self.hostid)
                self.skipdirs = ','.join(dict.fromkeys(
                    part.strip() for value in [source_job['skipdirs'] or '', *names, *paths]
                    for part in value.split(',') if part.strip()))
                self.excluded = exclusion_matcher(self.skipdirs)
                cursor.execute(f"SELECT dirid,parentdir,path,root_path,runid,time_inode_changed,read_failures "
                               f"FROM {self.db.TABLE_DIRECTORIES} WHERE hostid = %s", (self.hostid,))
                self.rows = {os.path.normpath(row['path']): row for row in cursor.fetchall()}
                retry_fix = False
                if self.job['stage'] == 'firstscan':
                    self._fastscan(cursor)
                elif self.job['stage'] == 'fixunreadable':
                    self._fixunreadable(cursor)
                elif self.job['stage'] == 'workmodified':
                    retry_fix = self._workmodified(cursor)
                    if not retry_fix:
                        cursor.execute(f"DELETE FROM {self.db.TABLE_MODIFIED} WHERE batch_jobid=%s AND hostid=%s",
                                       (self.batch,self.hostid))
                else:
                    raise RuntimeError('Unbekannte Schnellprüf-Stufe: ' + self.job['stage'])
                connection.commit()
                self._flush_notes()
                return 'fixunreadable' if retry_fix else None
            except Exception:
                self.db._rollback_preserving_error(connection)
                raise
            finally:
                self.db._close_preserving_error(cursor)

    def _flush_notes(self):
        """Announce repairs only after their database transaction committed."""
        notes,self.notes=self.notes,[]
        for message in notes:
            self.note(message)

    def _heartbeat(self,cursor):
        """Persist progress often enough for the server's normal stall watchdog."""
        if time.monotonic() < self.next_progress:
            return
        if self.job['stage'] != 'firstscan':
            self._guard(cursor)
        now=self.db.utc_now
        cursor.execute(f"UPDATE {self.db.TABLE_JOBS} SET last_activity=%s,timeupdated=%s "
                       "WHERE jobid=%s AND hostid=%s AND status='active'",
                       (now,now,self.job['jobid'],self.hostid))
        self.connection.commit()
        self._flush_notes()
        self.next_progress=time.monotonic()+5

    @staticmethod
    def _utc(timestamp):
        return datetime.fromtimestamp(timestamp, timezone.utc).replace(tzinfo=None)

    def _inspect(self, path):
        """Read one level of ordinary files/directories without following symlinks."""
        info = os.stat(path, follow_symlinks=False)
        if not stat.S_ISDIR(info.st_mode):
            raise NotADirectoryError(path)
        latest = None
        children = []
        with os.scandir(path) as entries:
            for index,entry in enumerate(entries):
                if index % 1000 == 0:
                    self._heartbeat(self.cursor)
                if self.excluded(entry.path):
                    continue
                try:
                    item = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    # Directory mtime will identify the churn on the next pass.
                    continue
                if stat.S_ISDIR(item.st_mode):
                    children.append(os.path.normpath(entry.path))
                elif stat.S_ISREG(item.st_mode):
                    modified = self._utc(item.st_mtime)
                    if latest is None or modified > latest:
                        latest = modified
        return self._utc(info.st_mtime), latest, children

    def _record(self, cursor, row, timestamp, unreadable=False):
        cursor.execute(f"""INSERT INTO {self.db.TABLE_MODIFIED}
            (batch_jobid,hostid,dirid,runid,newtimestamp,subfolders_excluded,unreadable,path)
            VALUES (%s,%s,%s,%s,%s,TRUE,%s,%s)
            ON DUPLICATE KEY UPDATE newtimestamp=VALUES(newtimestamp),
                unreadable=VALUES(unreadable), path=VALUES(path), subfolders_excluded=TRUE""",
            (self.batch,self.hostid,row['dirid'],row['runid'],timestamp,bool(unreadable),row['path']))

    def _fastscan(self, cursor):
        cursor.execute(f"""SELECT DISTINCT d.root_path, d.path FROM {self.db.TABLE_DIRECTORIES} d
            JOIN {self.db.TABLE_JOBS_RUNID} r ON r.runid=d.runid
            WHERE d.hostid=%s AND r.jobid=%s ORDER BY d.root_path, d.path""", (self.hostid,self.source))
        inventory = {item['root_path'] for item in cursor.fetchall()}
        roots = []
        for label in sorted(inventory):
            root = os.path.normpath(label)
            if root in self.rows:
                roots.append(root)
                continue
            cursor.execute(f"DELETE FROM {self.db.TABLE_DIRECTORIES} "
                           "WHERE hostid=%s AND BINARY root_path=BINARY %s",(self.hostid,label))
            deleted=cursor.rowcount
            self.rows={path:row for path,row in self.rows.items() if row['root_path'] != label}
            cursor.execute(f"SELECT jobid FROM {self.db.TABLE_JOBS} WHERE hostid=%s "
                           "AND BINARY startdirname=BINARY %s AND task='scan' "
                           "AND status IN ('insert','pending','active','paused') ORDER BY jobid LIMIT 1",
                           (self.hostid,root))
            existing=cursor.fetchone()
            if existing:
                scan_job=existing['jobid']
            else:
                now=self.db.utc_now
                cursor.execute(f"""INSERT INTO {self.db.TABLE_JOBS}
                    (userhost,hostid,startdirname,task,status,stage,skipdirs,scanall,
                     timecreated,timeupdated,last_activity)
                    VALUES (%s,%s,%s,'scan','insert','coarse',%s,0,%s,%s,%s)""",
                    (self.job['userhost'],self.hostid,root,self.skipdirs,now,now,now))
                scan_job=cursor.lastrowid
            self.notes.append(f'Job {self.job["jobid"]}, Host {self.hostid}: '
                              f'Inventarwurzel ohne eigenen Datensatz: {label}; {deleted} Directory-Einträge '
                              f'gelöscht; normaler Scan-Job {scan_job} eingereiht.')
        # Retain other batches and make a restarted fast scan replace only its own report.
        cursor.execute(f"DELETE FROM {self.db.TABLE_MODIFIED} WHERE batch_jobid=%s AND hostid=%s",
                       (self.batch,self.hostid))
        pending = sorted(set(roots), reverse=True)
        seen = set()
        while pending:
            path = pending.pop()
            if path in seen or self.excluded(path):
                continue
            seen.add(path)
            row = self.rows.get(path)
            anchor = path
            while row is None and os.path.dirname(anchor) != anchor:
                anchor = os.path.dirname(anchor)
                row = self.rows.get(anchor)
            try:
                inode, latest, children = self._inspect(path)
            except (PermissionError, FileNotFoundError, NotADirectoryError):
                if row is None:
                    raise RuntimeError(f'Kein inventarisierter Rescan-Einstieg für {path}')
                if path in self.rows:
                    cursor.execute(f"UPDATE {self.db.TABLE_DIRECTORIES} SET read_failures="
                                   "COALESCE(read_failures,0)+1 WHERE dirid=%s AND hostid=%s",
                                   (row['dirid'],self.hostid))
                    self._record(cursor,row,None,True)
                else:
                    self._record(cursor,row,None)
                self._heartbeat(cursor)
                continue
            pending.extend(sorted(children,reverse=True))
            if row is None:
                raise RuntimeError(f'Kein inventarisierter Rescan-Einstieg für {path}')
            baseline = row['time_inode_changed']
            if int(row.get('read_failures') or 0) > 0:
                self._record(cursor,row,inode,True)
            elif (path not in self.rows or baseline is None or inode > baseline
                    or (latest is not None and latest > baseline)):
                self._record(cursor,row,inode)
            self._heartbeat(cursor)
        print(f"Job {self.job['jobid']}: {len(seen)} Verzeichnisse schnell geprüft.",flush=True)

    def _report(self,cursor,unreadable=None):
        cursor.execute(f"SELECT * FROM {self.db.TABLE_MODIFIED} WHERE batch_jobid=%s AND hostid=%s"
                       + (' AND unreadable=TRUE' if unreadable else ''), (self.batch,self.hostid))
        ids = {row['dirid'] for row in cursor.fetchall()}
        return [row for row in self.rows.values() if row['dirid'] in ids]

    def _delete_tree(self,cursor,path,preserve_unreadable=False):
        removed = [row for candidate,row in self.rows.items() if self.db._rescan_path_within(path,candidate)]
        for row in sorted(removed,key=lambda row:len(row['path']),reverse=True):
            cursor.execute(f"DELETE FROM {self.db.TABLE_DIRECTORIES} WHERE dirid=%s AND hostid=%s",
                           (row['dirid'],self.hostid))
            if not preserve_unreadable:
                cursor.execute(f"DELETE FROM {self.db.TABLE_MODIFIED} "
                               "WHERE batch_jobid=%s AND hostid=%s AND dirid=%s",
                               (self.batch,self.hostid,row['dirid']))
            self.rows.pop(os.path.normpath(row['path']),None)

    def _fixunreadable(self,cursor):
        for row in self._report(cursor,True):
            if os.path.normpath(row['path']) not in self.rows:
                continue
            failures=int(row.get('read_failures') or 0)
            readable=False
            while failures<3:
                try:
                    self._inspect(row['path'])
                except (PermissionError,FileNotFoundError,NotADirectoryError):
                    failures+=1
                    cursor.execute(f"UPDATE {self.db.TABLE_DIRECTORIES} SET read_failures=%s "
                                   "WHERE dirid=%s AND hostid=%s",(failures,row['dirid'],self.hostid))
                else:
                    readable=True
                    cursor.execute(f"UPDATE {self.db.TABLE_DIRECTORIES} SET read_failures=0 "
                                   "WHERE dirid=%s AND hostid=%s",(row['dirid'],self.hostid))
                    cursor.execute(f"UPDATE {self.db.TABLE_MODIFIED} SET unreadable=FALSE "
                                   "WHERE batch_jobid=%s AND hostid=%s AND dirid=%s",
                                   (self.batch,self.hostid,row['dirid']))
                    break
            if not readable:
                self._delete_tree(cursor,os.path.normpath(row['path']),preserve_unreadable=True)
                print(f"Job {self.job['jobid']}: nach {failures} Lesefehlern Inventar-Teilbaum entfernt: {row['path']}",flush=True)
            self._heartbeat(cursor)
        self._guard(cursor)

    def _guard(self,cursor):
        # Readiness uses a positional cursor; stay on this worker's transaction.
        guard=self.connection.cursor()
        try:
            self.db.ensure_combined_hash_readiness(self.hostid,cursor=guard)
        finally:
            self.db._close_preserving_error(guard)

    def _workmodified(self,cursor):
        retry_fix=False
        pending=[os.path.normpath(row['path']) for row in self._report(cursor)]
        cursor.execute(f"SELECT path FROM {self.db.TABLE_MODIFIED} "
                       "WHERE batch_jobid=%s AND hostid=%s AND unreadable=TRUE",
                       (self.batch,self.hostid))
        blocked=[row['path'] for row in cursor.fetchall() if row['path']]
        changed=set()
        cursor.execute(f"""SELECT DISTINCT d.root_path FROM {self.db.TABLE_DIRECTORIES} d
            JOIN {self.db.TABLE_JOBS_RUNID} r ON r.runid=d.runid
            WHERE d.hostid=%s AND r.jobid=%s""", (self.hostid,self.source))
        roots={row['root_path'] for row in cursor.fetchall() if not self.excluded(row['root_path'])}
        # Invalidate known changes before a progress commit can expose refreshed
        # ownership. Transfer each root together, avoiding artificial job conflicts
        # when the worker is paused midway through its directory list.
        for path in pending:
            row=self.rows[path]
            cursor.execute(f"""UPDATE {self.db.TABLE_DIRECTORIES}
                SET file_list_sha256=NULL,files_sha256=NULL,subdirs_sha256=NULL,
                    combined_sha256=NULL,subdirectory_hash=NULL,subtree_size_bytes=NULL,
                    subtree_structure=NULL,localfilesize=NULL,localfilecount=NULL,
                    emptyfilecount=NULL,finished=0,zero_content_only=FALSE
                WHERE dirid=%s AND hostid=%s""",(row['dirid'],self.hostid))
        for root in roots:
            cursor.execute(f"UPDATE {self.db.TABLE_DIRECTORIES} SET runid=%s WHERE hostid=%s AND root_path=%s",
                           (self.job['runid'],self.hostid,root))
        visited=set()
        while pending:
            path=pending.pop()
            if path in visited or path not in self.rows or self.excluded(path):
                continue
            visited.add(path)
            row=self.rows[path]
            try:
                inode,latest,children=self._inspect(path)
            except (PermissionError,FileNotFoundError,NotADirectoryError):
                cursor.execute(f"UPDATE {self.db.TABLE_DIRECTORIES} SET read_failures="
                               "COALESCE(read_failures,0)+1 WHERE dirid=%s AND hostid=%s",
                               (row['dirid'],self.hostid))
                self._record(cursor,row,None,True)
                # Re-enter the fix stage if access changes after its earlier check.
                retry_fix=True
                self._heartbeat(cursor)
                continue
            roots.add(row['root_path'])
            children=[child for child in children if not any(
                self.db._rescan_path_within(denied,child) for denied in blocked)]
            old_children=[child for child in list(self.rows.values()) if child['parentdir']==row['dirid']]
            for child in old_children:
                if os.path.normpath(child['path']) not in children:
                    changed.add(os.path.normpath(child['path']))
                    self._delete_tree(cursor,os.path.normpath(child['path']))
            for child in children:
                if child not in self.rows:
                    cursor.execute(f"""INSERT INTO {self.db.TABLE_DIRECTORIES}
                        (hostid,path_hash,root_path,runid,path,name,parentdir,read_failures)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,0)""",
                        (self.hostid,hashlib.blake2b(child.encode('utf-8','surrogateescape'),digest_size=8).hexdigest(),
                         row['root_path'],self.job['runid'],child,os.path.basename(child),row['dirid']))
                    self.rows[child]=dict(dirid=cursor.lastrowid,parentdir=row['dirid'],path=child,
                        root_path=row['root_path'],runid=self.job['runid'],read_failures=0,time_inode_changed=None)
                    pending.append(child)
            cursor.execute(f"""UPDATE {self.db.TABLE_DIRECTORIES}
                SET runid=%s, files_lastwrite=%s,time_inode_changed=%s,read_failures=0,
                    subdirectory_count=%s,subdircount=%s,file_count=NULL,size_bytes=NULL,
                    localfilesize=NULL,localfilecount=NULL,emptyfilecount=NULL,otherentrycount=NULL,
                    file_list_sha256=NULL,files_sha256=NULL,subdirs_sha256=NULL,combined_sha256=NULL,
                    subdirectory_hash=NULL,subtree_size_bytes=NULL,subtree_structure=NULL,
                    finished=0,zero_content_only=FALSE,rescan_from_here=FALSE
                WHERE dirid=%s AND hostid=%s""",
                (self.job['runid'],latest,inode,len(children),len(children),row['dirid'],self.hostid))
            changed.add(path)
            cursor.execute(f"DELETE FROM {self.db.TABLE_MODIFIED} WHERE batch_jobid=%s AND hostid=%s AND dirid=%s",
                           (self.batch,self.hostid,row['dirid']))
            self._heartbeat(cursor)
        # One owner job per refreshed root; unchanged descendants retain their valid hashes.
        for root in roots:
            cursor.execute(f"UPDATE {self.db.TABLE_DIRECTORIES} SET runid=%s WHERE hostid=%s AND root_path=%s",
                           (self.job['runid'],self.hostid,root))
        self._guard(cursor)
        cursor.execute(f"DELETE FROM {self.db.TABLE_MODIFIED} WHERE batch_jobid=%s AND hostid=%s "
                       f"AND dirid NOT IN (SELECT dirid FROM {self.db.TABLE_DIRECTORIES} WHERE hostid=%s)",
                       (self.batch,self.hostid,self.hostid))
        print(f"Job {self.job['jobid']}: {len(changed)} Änderungen grob verarbeitet.",flush=True)
        return retry_fix
