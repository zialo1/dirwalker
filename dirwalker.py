#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# version 0.25.2

"""Dateisystem-Inventar mit Jobverwaltung und Scan-Server.

Das Programm verwaltet Scan-Aufträge in einer MariaDB/MySQL-Datenbank und
speichert Verzeichnisse in der Tabelle ``directories``. Es bietet zwei
Betriebsbereiche: Client-Befehle zum Einrichten und Verwalten von Jobs sowie
einen dauerhaft laufenden Server, der Aufträge abarbeitet.

Befehle:
    Unterbefehle mit Bindestrich sind auch mit führendem -- verfügbar,
    beispielsweise --jobs-insert und --init-jobs.
    show [NAME]           Ohne NAME verfügbare Tabellen auflisten.
                           hosts, jobs, excluded, listrunid, dirs-listrunid: formatierte Anzeigen.
                           Andere Namen: maximal 40 Tabellenzeilen ohne Header, danach Gesamtzahl.
    --setmyhostid          Eigenen Benutzer und Rechner mit positiver Host-ID registrieren.
    --insert-priority HOSTID DIRID [DIRID ...]
                           Verzeichnis-IDs dieses Hosts für Binarycheck priorisieren.
    serverinit             hosts und Host-0-Arbeitsliste bei Bedarf anlegen;
                           aktuellen Rechner als Host 0 registrieren, bestehende Daten behalten.
    server                 Scan-Server starten; Binarycheck-Jobs bleiben liegen.
    serverplus             Alle Jobs einschließlich Binarycheck abarbeiten.
    localserver --hostid HOSTID [--coarse-glob]
                           Jobs des registrierten lokalen Hosts abarbeiten.
                           Optional im Grobscan per glob nach Verzeichnissen suchen.
    init                   jobs und directories einrichten und Schema prüfen.
    --init-jobs            Datenbank, Jobtabelle, Runidhistorie und Arbeitswarteschlange einrichten.
    --drop-directories    ``directories`` löschen und frisch neu anlegen.
    --init-alldirs        Weiterer Aufruf für --drop-directories.
    --jobs-insert PFAD [SKIPDIRS]
                           Einen Verzeichnis-Scan einreihen; SKIPDIRS ist eine
                           kommaseparierte Liste auszuschließender Verzeichnisnamen.
    --jobs-insert-hostid HOSTID PFAD [SKIPDIRS]
                           Scan für die angegebene Host-ID einreihen.
    --jobs-insert-scanall PFAD [SKIPDIRS]
                           Scan mit scanall=1 einreihen; fertige Teilbäume nicht auslassen.
    --jobs-list-active     Aktive Jobs anzeigen.
    --jobs-stop JOBID      Einen laufenden oder wartenden Job stoppen.
    --jobs-start JOBID     Pausierten Job fortsetzen oder gestoppten exakt neu einreihen.
    --jobs-deleteid JOBID [-Y] Einen einzelnen Job nach Rückfrage löschen.
    --jobs-remove-unused   Nicht mehr im Inventar referenzierte abgeschlossene Jobs nach [Yn] löschen.
    --jobs-deleteall       Alle Jobdatensätze samt Checkpoints löschen.
    --droptmp                tmp<hostid>_job_work, tmp_deepdoubles_candidates, tmp_deepdoubles_result, tmp_remotedoubles und tmp_doubles_letchecked löschen.
    --dropall                Einmalige [yN]-Frage zu directories/jobs/hosts/jobsrunid;
                           übrige Anwendungstabellen zurücksetzen, job_work erhalten.
                           Tabellen ``directories``, ``job_work``, ``excluded_names``, ``live_excluded_absdirs``, ``tmp_doubles_letchecked``, ``jobsrunid`` und ``jobs`` löschen.
    check                  Vorhandensein und Schema der Tabellen prüfen.
    --directories-rescan-marked
                           Job für Server/localserver einreihen: markierte Teilbäume
                           grob prüfen und bei Änderungen Detailjobs einreihen.
    --resetlocks           Lokale Lockdateien zurücksetzen; Server vorher stoppen.
    --rescan-after-days TAGE
                           Schnellprüfung alter Inventar-Jobs; Stufen firstscan,
                           fixunreadable und workmodified im selben Job.
    --check-modifications  Prüfjobs für --hostid einreihen, ohne --hostid für alle
                           registrierten Hosts; Server/localserver prüft Zeitstempel
                           und setzt rescan_from_here über tmp_modified.
    --jobs-reset-on-error      Fehlerjobs anzeigen und einen Job zurücksetzen.
    --dropdir [-Y]           Einen ausgewählten root_path samt Einträgen löschen.
    --dirs-purge-from-exclusion-lists
                           Directory-Datensätze aus beiden Ausschlusslisten samt
                           Unterverzeichnissen per SQL löschen; optional --hostid.
    --dirs-summary-data Runids mit Stufe/Status und Anzahl je Runid/Host anzeigen.
    --dirs-droprunid RUNID [-Y] Directory-Einträge mit der Runid in runid oder finished löschen.
    --doubles-list         Tiefste vollständig doppelte Unterbäume auflisten.
    --doubles-letcheck        Doppelte Unterbäume für Binarycheck einreihen.
    --doubles-list-binary   Status der Binarycheck-Vergleiche anzeigen.
    --doubles-dirids-to-dirnames
                           Eingefügte dirid-Liste in Verzeichnisnamen umwandeln.
    --doubles-progress [JOBID] Fortschritt anzeigen und nach Abschluss Dateien
                           aus bestätigten Duplikatgruppen auflisten.
    --doubles-progress-html [JOBID]
                           Fortschritt als automatisch aktualisierte HTML-Datei schreiben.
    --doubles-tree [JOBID] Vorhandene Ergebnisse als HTML-Snapshot ohne Serverarbeit anzeigen.
    --doubles-tree-html [JOBID]
                           HTML-Baumbericht mit allen dirids je Hash-Gruppe mit level > 0.
    --excluded-dirnames CSV Globale Ausschlussnamen speichern.
    --excluded-absolute HOSTID PFAD
                           Absoluten Live-Ausschlusspfad für einen Host speichern.
    --excluded-absolute-delete HOSTID
                           Absolute Ausschlüsse anzeigen und interaktiv löschen.
    --erase-excluded       Namen und Live-Pfade des gewählten Hosts entfernen.

Beim Löschen eines einzelnen Jobs fragt das Programm standardmäßig nach einer
Bestätigung mit y oder n. Die Option -Y unterdrückt diese Rückfrage.
Fehlerhafte Jobs protokollieren ``server`` und ``serverplus`` im lokalen Syslog
auf localhost:5101. ``localserver`` zeigt Meldungen auf der Konsole und sendet
ab WARNING an denselben Syslog-Handler; INFO wird dort unterdrückt.
Coarse-Debugmeldungen werden zusätzlich lokal in ``localserver.log`` gespeichert.
Mit ``--jobs-reset-on-error`` kann ein Fehlerjob interaktiv auf
``insert`` zurückgesetzt werden.
Fehlt das Startverzeichnis oder ist es nicht lesbar, setzt der Server
``jobs.usererror`` auf ``Starting directory doesn't exist or isn't readable.``
und protokolliert den konkreten Pfadfehler im Syslog beziehungsweise bei
``localserver`` auf der Konsole.
Der Client verwaltet gemeinsame Verzeichnis-Ausschlüsse in ``excluded_names``.
Bei ``--jobs-insert`` fragt er, ob diese Namen in ``jobs.skipdirs`` übernommen
werden sollen; der Server verwendet unverändert das gespeicherte Jobfeld.
Einfache Namen gelten auf jeder Ebene. Absolute Pfade schließen gezielt das
angegebene Verzeichnis samt Unterbaum aus; diese werden nicht betreten.
``--excluded-absolute HOSTID PFAD`` speichert absolute Pfade getrennt
in ``live_excluded_absdirs`` mit ``hostid``. Diese werden
nicht in Jobs übernommen und einmal beim Serverstart für dessen Host geladen.
Alle Scan-Prozesse erhalten dieselbe Liste; Änderungen wirken nach Neustart. HOSTID 0 gilt für den Server, positive IDs für registrierte lokale Hosts. Die Auflösung
symbolischer Pfadbestandteile erfolgt erst auf dem jeweiligen Scan-Computer.
``--hostid N show excluded`` zeigt dessen Live-Pfade; ``--erase-excluded``
entfernt allgemeine Namen und die Live-Pfade des gewählten Hosts.
``--excluded-absolute-delete HOSTID`` listet ausschließlich dessen absolute
Pfade nummeriert auf. ``*`` löscht alle, eine Nummer den ausgewählten Eintrag,
``n`` oder Enter keinen. Die Änderung wirkt für Scans nach einem Server-Neustart.

Ein Job wird zunächst grob auf Verzeichnisebene erfasst und danach detailliert
untersucht. Beide Stufen verarbeiten höchstens 1000 Verzeichnisse pro Speicher-
batch. Ergebnisse und kompaktierte Checkpoints werden erst am Batch-Ende in einer
Datenbanktransaktion gespeichert. Ein `done`-Marker in `job_work` bedeutet, dass
der gesamte darunterliegende Teilbaum erledigt ist.
Jede Jobübernahme durch den Server erhält eine global aufsteigende ``runid``
(höchster bisheriger Wert plus eins). Der Wechsel zu ``pending`` behält die
aktuelle Runid bei; Terminalwechsel wie ``closed``, ``paused`` und ``error``
erhalten weiterhin eine neue. Beim Neustart verwendet die nächste Jobübernahme
den höchsten gespeicherten Wert als Ausgangspunkt und erhält dadurch eine neue
Runid. Die Nummer wird beim ersten Einfügen eines Verzeichnisses in
``directories.runid`` übernommen und bei späteren Scans dieses Verzeichnisses
nicht verändert. ``jobsrunid`` speichert jede neu vergebene Runid unveränderlich
zusammen mit Job-ID, Task, resultierendem Status, Stage und Erstellungszeit.
Für Pfadschlüssel wird ein schneller BLAKE2b-Hash mit 64 Bit (16 Hex-Zeichen)
verwendet; diese Spalte ist damit viermal kürzer als der bisherige SHA-256-Text.
Es findet keine automatische Migration alter Pfad-Hashes statt. Für ein
einheitliches neues Schema müssen die Tabellen neu angelegt werden.
Das Jobfeld ``scanall`` ist standardmäßig 0. Bei scanall=0 prüft der Server
vor dem Betreten jedes zweiten Verzeichnislevels (Tiefe 2, 4, 6 usw.), ob
``directories.finished`` auf eine der in den letzten 13 Tagen gestarteten
Laufnummern oder auf die Runid des gerade bearbeiteten Jobs verweist und für
den Kandidaten bereits lokale Detailwerte gespeichert sind. Dann
überspringt er den kompletten Teilbaum und gibt
``skipped subdirectory`` samt ``path_hash`` aus. ``--jobs-insert-scanall`` setzt
scanall auf 1 und führt den Scan vollständig aus.
Im Detaildurchlauf zählt der Server verschwundene oder durch Dateien ersetzte
Unterverzeichnisse über alle Batches. Beim 12. Fall bricht er den Detaildurchlauf
ab, verwirft die Arbeits-Checkpoints dieses Jobs und plant ihn als pending/coarse
mit allscan (scanall=1) neu ein. Die Neueinplanung wird als WARNING sowohl im
Syslog als auch auf der Konsole dokumentiert. Der Zähler beginnt bei jedem
Detaildurchlauf bei 0; ein verschwundenes Startverzeichnis bleibt ein Jobfehler.
``finished`` wird mit der
Laufnummer gesetzt, sobald ein Teilbaum im Grobscan vollständig in
``directories`` eingetragen ist; ein erfolgreicher Jobabschluss aktualisiert
die Marker auf dessen Abschluss-Laufnummer. Wenn ein übergeordneter Scan einen
fertigen Teilbaum überspringt, bleiben dessen ``root_path``-Zuordnungen erhalten,
damit bereits eingereihte Detail-Scans ihren Startknoten weiterhin finden.
Clientbefehle mit JOBID akzeptieren nur Werte ab 1.
Ein detaillierter Scan benötigt keinen Datensatz für seinen Startknoten: Wenn
der Grobscan diesen Knoten wegen eines gültigen ``finished``-Markers
übersprungen hat, wird die Detailstufe mit leerer Warteschlange erfolgreich
beendet. Die fehlende Startzeile löst dann keinen Fehler aus.
Die Detailstufe ermittelt pro Verzeichnis lokale Dateigröße, Datei- und
Leerdateizahl sowie Eintragszahlen. ``files_sha256`` hasht Namen und Größen direkter regulärer Dateien; es hasht nicht die Datei-Bytes.
``subdirs_sha256`` verbindet die Namen und kombinierten Hashes der direkten
Unterverzeichnisse. ``combined_sha256`` verbindet ``files_sha256`` und
``subdirs_sha256``. Dadurch fliesst eine Änderung in einem Kind über dessen
Hash rekursiv in alle Vorfahren ein. ``subtree_size_bytes`` summiert die
Dateigrößen des gesamten Unterbaums; ``subtree_structure`` zählt Verzeichnisse
pro relativer Tiefe und endet mit einer Null-Ebene. Die genaue Byte-Kodierung
und Zusammenführung ist in ``hashlogic.md`` beschrieben. Die Baumdaten werden
mit einem ungepufferten rekursiven Cursor in Blöcken zu 1000 Zeilen verarbeitet;
auch die Detailwarteschlange wird streamend erstellt. Die älteren Felder
``file_list_sha256`` und ``subdirectory_hash`` spiegeln ``files_sha256`` bzw.
``combined_sha256``. Symlink-Verzeichnisse werden nicht verfolgt.

Der Server startet mit ``server``. Wird das Skript ohne Argumente unter dem
Dateinamen ``fileserver.py`` ausgeführt, wird ebenfalls der Servermodus gewählt.
Scan-Stufen laufen in separaten Prozessen mit dem Startverfahren ``spawn``.
Jede Stufe arbeitet Verzeichnis für Verzeichnis und speichert erledigte Einheiten
in ``job_work``. Ein Signal pausiert den Auftrag; nach einem Neustart wird die
aktive Einheit wiederholt und der Scan in seiner gespeicherten Stufe fortgesetzt.
Die Detailstatistiken werden je Verzeichnis zusammen mit dem Fortschrittsmarker
transaktional gespeichert. Abgeleitete Teilbaum-Hashes lassen sich nach einem
Abbruch erneut aus den gespeicherten lokalen Werten berechnen.

Die Tabelle ``hosts`` enthält ``hostid``, ``user``, ``homepath``, ``homedirid``,
``system``, ``machine`` (Rechnername), ``timecreated`` (Erstellung in UTC) und
``tabellenname``. Die Arbeitswarteschlange eines Hosts liegt in dessen
``tmp<hostid>_job_work``-Tabelle. ``serverinit`` legt hosts und tmp0_job_work
bei Bedarf an und erhält alle vorhandenen Host-Registrierungen und Arbeitslisten. ``--setmyhostid`` erzeugt die jeweilige Host-Tabelle.
``--droptmp`` löscht alle numerischen Host-Arbeitstabellen. ``--dropall`` fragt einmal mit [yN], ob directories, jobs, hosts und jobsrunid
ebenfalls zurückgesetzt werden sollen. Alle übrigen Anwendungstabellen werden
zurückgesetzt; job_work bleibt erhalten. tmp-Tabellen werden nicht neu angelegt.
``--setmyhostid`` legt die Tabelle bei Bedarf an und vergibt eine Host-ID ab 1.
Bei gleichem Home-Pfad, System und Rechnernamen fragt --setmyhostid, ob eine
weitere Host-ID angelegt werden soll [yN]. Mit y wird eine neue ID vergeben;
sonst wird die vorhandene positive ID desselben Benutzers aktualisiert,
wobei die Erstellungszeit erhalten bleibt. ``homedirid``
verweist auf den passenden vorhandenen Home-Eintrag in ``directories`` desselben
Hosts, sonst ist es NULL. ``serverinit`` aktualisiert nur den Eintrag für Host-ID 0;
Jobs, Runidhistorie, Ergebnisse und Prioritätshinweise bleiben erhalten.
``init`` prüft das MariaDB-Zeitlimit. Wird directories neu erstellt, löscht es
Jobs, Runidhistorie, Arbeitslisten und davon abhängige Ergebnisse. Bestehende Jobs und
Verzeichnisse werden dadurch nicht neu zugeordnet. ``localserver --hostid HOSTID`` verlangt eine positive registrierte ID. Vor dem Start
werden Benutzer, Home-Pfad, System und Rechnername mit ``hosts`` verglichen;
bei fehlendem oder abweichendem Eintrag wird mit Fehler abgebrochen.
``show hosts`` zeigt alle Einträge. Mit ``--hostid HOSTID --jobs-insert PFAD``
lassen sich Jobs für die registrierte ID einreihen. ``check`` verlangt zusätzlich das
Host-Schema und einen Eintrag mit Host-ID 0.

Konfiguration über Umgebungsvariablen:
    DB_HOST       Datenbankserver, Standard: localhost
    DB_PORT       Port, Standard: 3306
    DB_NAME       Datenbankname, Standard: filestatus
    DB_USER       Datenbankbenutzer, erforderlich
    DB_PASSWORD   Passwort, erforderlich; ein leerer Wert ist zulässig
    POLL_SECONDS  Polling-Intervall, Standard: 120 Sekunden

Version 24: Verbindungen werden bei jedem Versuch neu aufgebaut (kein Pool und
kein reconnect auf einem alten Handle). Verbindungsaufbau: 10s; Socket-I/O:
30s, über DB_IO_TIMEOUT in Sekunden einstellbar. Für Connector/Python 8.4 wird
use_pure=True verwendet, da erst spätere Versionen read_timeout unterstützen.
Ein Wake-Monitor prüft alle 0,5s die Wanduhr. Nach einer Lücke über 5s
(Suspend, Prozesspause oder Uhrsprung) verwirft er alte Sockets und weckt das
Polling sofort. Dies erkennt den ersten geplanten Prozesslauf nach dem
Aufwachen, nicht den physischen Zeitpunkt des Einschaltens. Netzwerkfehler
werden alle 30s mit neuen Verbindungen geprüft; nach 600 aufeinanderfolgenden
fehlgeschlagenen Wiederverbindungsversuchen beendet sich der Server. Bei
Erfolg wird der Zähler zurückgesetzt. Checkpoints bleiben erhalten; bei
unerreichbarer DB kann der Jobstatus active bleiben. unterbrochene Scan-Worker
werden nach Wiederkehr der Verbindung über paused/Checkpoints fortgesetzt.
Unklare COMMIT-Ergebnisse werden nicht als Batch erneut ausgeführt.
Nach dem Aufwachen wird ein alter Worker beendet, bevor der Job auf paused
zur Checkpoint-Fortsetzung gesetzt wird. Im localserver wird nach 1200s ohne
Checkpoint (DIRWALKER_STALL_TIMEOUT, mindestens 30s) ein Worker neu gestartet.
In detailed wird vorher die Anzahl der directories mit fehlenden lokalen
Statistiken für Host-ID und Scan-Root verglichen. Sinkt sie seit der letzten
Prüfung, läuft der Worker für ein weiteres Intervall weiter. Die Zählung erfolgt
alle 20 Minuten (bei kürzerem Timeout entsprechend früher). Fehlgeschlagene
Zählungen gelten nicht als Stillstand. Die Statusprüfung erfolgt alle 5s. Nach erfolgreicher Wiederverbindung erfolgt die
Fortsetzung ohne das normale 120s-Polling abzuwarten.
Wake- und Wiederverbindungsmeldungen erscheinen auf Konsole und im Syslog.

Abhängigkeit: ``mysql-connector-python``. Die Offline-Unit-Tests werden mit
``python3 -m unittest test_version24`` ausgeführt. Sie benötigen weder eine
Datenbankverbindung noch Netzwerkzugriff.
"""

__version__ = "0.25.2"

import glob
import getpass
import platform
import re
import hashlib
import logging
from logging.handlers import SysLogHandler
import multiprocessing
import signal
import stat
import time
import traceback
import os
import sqlstructure
import socket
import sys
from pathlib import Path
from collections import deque
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone

sys.modules["version22"] = sys.modules[__name__]

from DataBaseCore import (
    DataBaseConnectionCore, Error, DATABASE_DRIVER, _database_connect,
    _process_start_token, is_network_error, exclusion_matcher,
)

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "120"))
WORK_BATCH_SIZE = 1000
LOCALSERVER_LOG_FILE = Path(__file__).with_name("localserver.log")
_shutdown = False
_current_child = None


def _append_localserver_log(message):
    """Append a localserver diagnostic without sending it to syslog/network."""
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    with open(LOCALSERVER_LOG_FILE, "a", encoding="utf-8") as log_file:
        log_file.write(f"{timestamp} {message}\n")


class ApplicationContext:
    """Shared runtime access without importing any client or server classes."""

    @staticmethod
    def runtime():
        """Return the single module owning database handles and scan state."""
        return sys.modules[__name__]

    @staticmethod
    def database():
        return database_connection

    @staticmethod
    def cli_database():
        from DataBaseCLI import DataBaseCLI
        return DataBaseCLI(database_connection)


class DataBaseConnection(DataBaseConnectionCore):
    """Database operations shared by scan workers and server execution."""

    @staticmethod
    def _local_host_values():
        """Collect the current OS user, home, system name and computer name."""
        return (getpass.getuser(), os.path.realpath(os.path.expanduser("~")),
                platform.system(), socket.gethostname())


    def list_hosts(self, hostid=None):
        """Read the host registry, optionally restricting it to one ID."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.select_table_count(cursor, self.database, self.TABLE_HOSTS)
                if not cursor.fetchone()[0]:
                    raise RuntimeError("Tabelle hosts fehlt; serverinit oder --setmyhostid ausführen.")
                cursor.execute(
                    f"SELECT hostid, `user`, homepath, homedirid, `system`, machine, timecreated, tabellenname "
                    f"FROM {self.TABLE_HOSTS}"
                    + (" WHERE hostid = %s" if hostid is not None else "")
                    + " ORDER BY hostid",
                    (int(hostid),) if hostid is not None else (),
                )
                return cursor.fetchall()
            finally:
                cursor.close()


    def validate_local_host(self, hostid):
        """Reject missing registrations and mismatched local machine/user data."""
        hostid = int(hostid)
        if not 1 <= hostid <= 32767:
            raise ValueError("localserver benötigt eine positive Host-ID (1 bis 32767).")
        rows = self.list_hosts(hostid)
        if not rows:
            raise RuntimeError(f"Host-ID {hostid} ist nicht registriert; --setmyhostid ausführen.")
        row = rows[0]
        if len(row) < 8 or row[7] != self.work_table_name(hostid):
            raise RuntimeError(f"hosts.tabellenname für Host-ID {hostid} ist ungültig.")
        actual = self._local_host_values()
        stored = (row[1], row[2], row[4], row[5])
        differences = [
            f"{field}: gespeichert={expected!r}, lokal={current!r}"
            for field, expected, current in zip(
                ("user", "homepath", "system", "machine"), stored, actual
            ) if expected != current
        ]
        if differences:
            raise RuntimeError(
                f"Host-ID {hostid} passt nicht zum lokalen Benutzer/Rechner: "
                + "; ".join(differences)
            )
        return hostid


    def get_job_hostid(self, jobid):
        """Return the filesystem host assigned to a job."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT hostid FROM {self.TABLE_JOBS} WHERE jobid = %s",
                    (jobid,),
                )
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError(f"Job {jobid} existiert nicht mehr.")
                return int(row[0])
            finally:
                cursor.close()


    def prepare_stage_work(
        self, jobid, stage, root_path, scanall=1, recent_runids=(), hostid=None
    ):
        """Create a stage queue and report whether its root is represented."""
        expected_hostid = self.get_job_hostid(jobid)
        if hostid is not None and int(hostid) != expected_hostid:
            raise RuntimeError(
                f"Job {jobid} gehört hostid={expected_hostid}, nicht hostid={hostid}."
            )
        hostid = expected_hostid
        self.invalidate_zero_content_flags(hostid)
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {self.work_table_name(hostid)} "
                    "WHERE jobid = %s AND stage = %s AND hostid = %s",
                    (jobid, stage, hostid),
                )
                is_new = cursor.fetchone()[0] == 0
                if stage == "coarse" and is_new:
                    path = os.path.realpath(os.path.abspath(root_path))
                    cursor.execute(
                        f"""INSERT INTO {self.work_table_name(hostid)}
                            (jobid, stage, hostid, path_hash, path, parent_path, state)
                            VALUES (%s, %s, %s, %s, %s, NULL, 'pending')""",
                        (jobid, stage, hostid, _path_hash(path), path),
                    )
                    cursor.execute(
                        f"UPDATE {self.TABLE_DIRECTORIES} SET nopermdirs = 0 "
                        "WHERE hostid = %s AND root_path = %s",
                        (hostid, path),
                    )
                elif stage == "detailed" and is_new:
                    root_path = os.path.realpath(os.path.abspath(root_path))
                    root_hash = _path_hash(root_path)
                    cursor.execute(
                        f"SELECT dirid, root_path FROM {self.TABLE_DIRECTORIES} "
                        "WHERE hostid = %s AND path_hash = %s",
                        (hostid, root_hash),
                    )
                    stored_root = cursor.fetchone()
                    if stored_root is None:
                        print(
                            "Startverzeichnis nicht in directories; "
                            f"Detailstufe wird übersprungen: {root_path}",
                            flush=True,
                        )
                        cursor.execute(
                            f"UPDATE {self.TABLE_JOBS} SET stage = %s "
                            "WHERE jobid = %s AND status = 'active'",
                            (stage, jobid),
                        )
                        connection.commit()
                        return False

                    stored_root_id, stored_root_path = stored_root
                    if os.path.normpath(stored_root_path) != root_path:
                        # Older server versions reassigned every row in a
                        # skipped subtree to the enclosing scan root. Restore
                        # this job's subtree ownership before detailed work.
                        cursor.execute(
                            f"SELECT path_hash, path FROM {self.TABLE_DIRECTORIES} "
                            "WHERE hostid = %s",
                            (hostid,),
                        )
                        subtree_hashes = [
                            (path_hash,)
                            for path_hash, path in cursor.fetchall()
                            if _path_is_within(root_path, os.path.normpath(path))
                        ]
                        if subtree_hashes:
                            # Move the root marker last: an interrupted migration
                            # still has an old root_path and retries on restart.
                            subtree_hashes.sort(key=lambda item: (item[0] == root_hash, item[0]))
                            for offset in range(0, len(subtree_hashes), 200):
                                cursor.executemany(
                                    f"UPDATE {self.TABLE_DIRECTORIES} SET root_path = %s "
                                    "WHERE hostid = %s AND path_hash = %s",
                                    [(root_path, hostid, path_hash)
                                     for (path_hash,) in subtree_hashes[offset:offset + 200]],
                                )
                                connection.commit()
                    # Resolve paths once in memory; write only changed parents
                    # by primary key and commit each short batch independently.
                    self._repair_directory_parents(
                        connection, cursor, hostid, root_path, stored_root_id,
                    )

                    reader = self.connect()
                    reader_cursor = None
                    try:
                        reader_cursor = reader.cursor(buffered=False)
                        reader_cursor.execute(
                            f"SELECT dirid FROM {self.TABLE_DIRECTORIES} "
                            "WHERE hostid = %s AND root_path = %s AND path_hash = %s",
                            (hostid, root_path, _path_hash(root_path)),
                        )
                        root_row = reader_cursor.fetchone()
                        if root_row is None:
                            print(
                                "Startverzeichnis nicht in directories; "
                                f"Detailstufe wird übersprungen: {root_path}",
                                flush=True,
                            )
                            cursor.execute(
                                f"UPDATE {self.TABLE_JOBS} SET stage = %s "
                                "WHERE jobid = %s AND status = 'active'",
                                (stage, jobid),
                            )
                            connection.commit()
                            return False
                        skip_condition = "1 = 0"
                        query_params = [root_row[0], hostid, root_path]
                        if not scanall and recent_runids:
                            runids = sorted(recent_runids)
                            placeholders = ", ".join("%s" for _ in runids)
                            skip_condition = (
                                "((t.tiefe + 1) >= 2 "
                                "AND MOD(t.tiefe + 1, 2) = 0 "
                                f"AND d.finished IN ({placeholders}) "
                                "AND d.localfilesize IS NOT NULL)"
                            )
                            query_params.extend(runids)
                        # The recursive member filters by hostid/root_path; the
                        # child count and parent join each add one root_path.
                        query_params.extend([hostid, root_path, root_path, root_path])

                        reader_cursor.execute(
                            f"""WITH RECURSIVE tree AS (
                                    SELECT d.hostid, d.dirid, d.parentdir, d.path, d.path_hash,
                                           d.finished, 0 AS tiefe, 0 AS skip_subtree
                                    FROM {self.TABLE_DIRECTORIES} d
                                    WHERE d.dirid = %s AND d.hostid = %s AND d.root_path = %s

                                    UNION ALL

                                    SELECT d.hostid, d.dirid, d.parentdir, d.path, d.path_hash,
                                           d.finished, t.tiefe + 1,
                                           CASE WHEN {skip_condition}
                                                     OR (t.skip_subtree = 1
                                                         AND d.localfilesize IS NOT NULL)
                                                THEN 1 ELSE 0 END
                                    FROM {self.TABLE_DIRECTORIES} d
                                    JOIN tree t ON d.parentdir = t.dirid AND d.hostid = t.hostid
                                    WHERE d.hostid = %s AND d.root_path = %s
                                )
                                SELECT t.dirid, t.path_hash, t.path, t.parentdir,
                                       p.path,
                                       (SELECT COUNT(*) FROM {self.TABLE_DIRECTORIES} c
                                        WHERE c.hostid = t.hostid AND c.parentdir = t.dirid
                                          AND c.root_path = %s),
                                       t.skip_subtree
                                FROM tree t
                                LEFT JOIN {self.TABLE_DIRECTORIES} p
                                  ON p.hostid = t.hostid AND p.dirid = t.parentdir
                                 AND p.root_path = %s""",
                            tuple(query_params),
                        )
                        insert_sql = f"""INSERT INTO {self.work_table_name(hostid)}
                            (jobid, stage, hostid, path_hash, path, parent_path,
                             child_count, state)
                            VALUES (%s, 'detailed', %s, %s, %s, %s, %s, %s)"""
                        while True:
                            rows = reader_cursor.fetchmany(1000)
                            if not rows:
                                break
                            cursor.executemany(
                                insert_sql,
                                [
                                    (
                                        jobid, hostid, path_hash, path, parent_path,
                                        child_count,
                                        "done" if skip_subtree else "pending",
                                    )
                                    for (
                                        _dirid, path_hash, path, _parent_id,
                                        parent_path, child_count, skip_subtree,
                                    ) in rows
                                ],
                            )
                        # The recursive tree can intentionally contain fewer
                        # rows than the table-wide root_path count: overlapping
                        # scans leave rows owned by another root, and stale or
                        # disconnected rows are not descendants of this root.
                    finally:
                        if reader_cursor is not None:
                            self._close_preserving_error(reader_cursor)
                        self._close_preserving_error(reader)
                cursor.execute(
                    f"UPDATE {self.work_table_name(hostid)} SET state = 'pending' "
                    "WHERE jobid = %s AND stage = %s AND hostid = %s AND state = 'active'",
                    (jobid, stage, hostid),
                )
                cursor.execute(
                    f"UPDATE {self.TABLE_JOBS} SET stage = %s "
                    "WHERE jobid = %s AND status = 'active'",
                    (stage, jobid),
                )
                connection.commit()
                return True
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                self._close_preserving_error(cursor)


    def claim_work_batch(self, jobid, stage, limit=WORK_BATCH_SIZE, hostid=None):
        """Read a batch of pending directory units without checkpointing them yet."""
        limit = max(1, min(int(limit), WORK_BATCH_SIZE))
        expected_hostid = self.get_job_hostid(jobid)
        if hostid is not None and int(hostid) != expected_hostid:
            raise RuntimeError(
                f"Job {jobid} gehört hostid={expected_hostid}, nicht hostid={hostid}."
            )
        hostid = expected_hostid
        with self.connect() as connection:
            cursor = connection.cursor(dictionary=True)
            try:
                cursor.execute(
                    f"""SELECT path_hash, path, parent_path, child_count
                        FROM {self.work_table_name(hostid)}
                        WHERE jobid = %s AND stage = %s AND hostid = %s AND state = 'pending'
                        ORDER BY path_hash LIMIT {limit}""",
                    (jobid, stage, hostid),
                )
                rows = cursor.fetchall()
                families = self.read_directory_families(rows, hostid, cursor=cursor)
                for work in rows:
                    family = families[work['path_hash']]
                    work['directory'] = family['directory']
                    work['subdirectories'] = family['children']
                connection.commit()
                return rows
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                cursor.close()


    def _compact_done_subtrees(self, cursor, jobid, stage, runid=None, hostid=0):
        """Replace completed child markers with their completed parent marker."""
        while True:
            cursor.execute(
                f"""SELECT path_hash, path, child_count
                    FROM {self.work_table_name(hostid)}
                    WHERE jobid = %s AND stage = %s AND hostid = %s AND state = 'expanded'""",
                (jobid, stage, hostid),
            )
            expanded = cursor.fetchall()
            changed = False
            for path_hash, path, child_count in expanded:
                if child_count:
                    cursor.execute(
                        f"""SELECT COUNT(*) FROM {self.work_table_name(hostid)}
                            WHERE jobid = %s AND stage = %s AND hostid = %s
                              AND parent_path = %s AND state = 'done'""",
                        (jobid, stage, hostid, path),
                    )
                    if cursor.fetchone()[0] != child_count:
                        continue
                cursor.execute(
                    f"UPDATE {self.work_table_name(hostid)} SET state = 'done' "
                    "WHERE jobid = %s AND stage = %s AND hostid = %s AND path_hash = %s",
                    (jobid, stage, hostid, path_hash),
                )
                if stage == "coarse" and runid is not None:
                    cursor.execute(
                        f"UPDATE {self.TABLE_DIRECTORIES} SET finished = %s "
                        "WHERE hostid = %s AND path_hash = %s",
                        (runid, hostid, path_hash),
                    )
                if child_count:
                    cursor.execute(
                        f"""DELETE FROM {self.work_table_name(hostid)}
                            WHERE jobid = %s AND stage = %s AND hostid = %s
                              AND parent_path = %s AND state = 'done'""",
                        (jobid, stage, hostid, path),
                    )
                changed = True
            if not changed:
                return


    def complete_coarse_batch(self, jobid, root_path, results, hostid=None):
        """Persist up to 200 coarse scans and compact completed subtrees."""
        return self._retry_on_lock_error(
            self._complete_coarse_batch_once,
            jobid, root_path, results, hostid=hostid,
        )


    def _complete_coarse_batch_once(self, jobid, root_path, results, hostid=None):
        """Persist one coarse batch in a single transaction."""
        with self.connect() as connection:
            cursor = connection.cursor()
            commit_started = False
            try:
                cursor.execute(
                    f"SELECT runid, hostid FROM {self.TABLE_JOBS} WHERE jobid = %s",
                    (jobid,),
                )
                job_row = cursor.fetchone()
                if job_row is None:
                    raise RuntimeError(f"Job {jobid} existiert nicht mehr.")
                job_runid, job_hostid = job_row
                if hostid is not None and int(hostid) != int(job_hostid):
                    raise RuntimeError("Host-ID des groben Scans stimmt nicht mit dem Job überein.")
                hostid = int(job_hostid)
                inserted_count = 0
                for result in results:
                    work = result["work"]
                    path = os.path.realpath(os.path.abspath(work["path"]))
                    parent_path = work["parent_path"]
                    if result.get("skip_subtree"):
                        # Preserve rows under their original scan root. Moving
                        # them to this enclosing root would make a previously
                        # queued detailed scan unable to find its own root.
                        cursor.execute(
                            f"""UPDATE {self.work_table_name(hostid)}
                                SET state = 'done', child_count = 0
                                WHERE jobid = %s AND stage = 'coarse' AND hostid = %s AND path_hash = %s""",
                            (jobid, hostid, work["path_hash"]),
                        )
                        continue
                    if result.get("remove_subtree"):
                        cursor.execute(
                            f"SELECT path_hash, path FROM {self.TABLE_DIRECTORIES} "
                            "WHERE hostid = %s AND root_path = %s",
                            (hostid, root_path),
                        )
                        stale_rows = cursor.fetchall()
                        stale_hashes = []
                        for stale_hash, candidate in stale_rows:
                            try:
                                inside = os.path.commonpath((path, candidate)) == path
                            except ValueError:
                                inside = False
                            if inside:
                                stale_hashes.append((stale_hash,))
                        if stale_hashes:
                            cursor.executemany(
                                f"DELETE FROM {self.TABLE_DIRECTORIES} "
                                "WHERE hostid = %s AND path_hash = %s",
                                [(hostid, stale_hash) for (stale_hash,) in stale_hashes],
                            )
                    if result["store"]:
                        # The scan root has no parent; zero is its root marker.
                        parentdir_id = 0
                        if parent_path is not None:
                            cursor.execute(
                                f"SELECT dirid FROM {self.TABLE_DIRECTORIES} "
                                "WHERE hostid = %s AND root_path = %s AND path_hash = %s",
                                (hostid, root_path, _path_hash(parent_path)),
                            )
                            parent_row = cursor.fetchone()
                            if parent_row is None:
                                raise RuntimeError(
                                    f"Elternverzeichnis wurde noch nicht angelegt: {parent_path}"
                                )
                            parentdir_id = parent_row[0]
                        cursor.execute(
                            f"""INSERT INTO {self.TABLE_DIRECTORIES}
                                (hostid, path_hash, root_path, runid, path, name, parentdir)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    root_path = VALUES(root_path),
                                    path = VALUES(path), name = VALUES(name),
                                    parentdir = VALUES(parentdir)""",
                            (
                                hostid, work["path_hash"], root_path, job_runid, path,
                                os.path.basename(path) or path, parentdir_id,
                            ),
                        )
                        if cursor.rowcount == 1:
                            inserted_count += 1
                    child_paths = sorted(set(
                        os.path.realpath(os.path.abspath(value))
                        for value in result["children"]
                    ))
                    for child_path in child_paths:
                        cursor.execute(
                            f"""INSERT IGNORE INTO {self.work_table_name(hostid)}
                                (jobid, stage, hostid, path_hash, path, parent_path,
                                 child_count, state)
                                VALUES (%s, 'coarse', %s, %s, %s, %s, 0, 'pending')""",
                            (jobid, hostid, _path_hash(child_path), child_path, path),
                        )
                    cursor.execute(
                        f"""UPDATE {self.work_table_name(hostid)}
                            SET state = 'expanded', child_count = %s
                            WHERE jobid = %s AND stage = 'coarse' AND hostid = %s AND path_hash = %s""",
                        (len(child_paths), jobid, hostid, work["path_hash"]),
                    )
                    permission_parent = result.get("permission_parent")
                    if permission_parent is not None:
                        cursor.execute(
                            f"UPDATE {self.TABLE_DIRECTORIES} "
                            "SET nopermdirs = nopermdirs + 1 "
                            "WHERE hostid = %s AND root_path = %s AND path_hash = %s",
                            (hostid, root_path, _path_hash(permission_parent)),
                        )
                self._compact_done_subtrees(cursor, jobid, "coarse", job_runid, hostid)
                now = self.utc_now
                cursor.execute(
                    f"UPDATE {self.TABLE_JOBS} SET last_activity = %s, timeupdated = %s "
                    "WHERE jobid = %s AND hostid = %s",
                    (now, now, jobid, hostid),
                )
                commit_started = True
                connection.commit()
                return inserted_count
            except Exception as error:
                # A disconnected, uncommitted transaction is rolled back by
                # the server. A lost COMMIT response has an unknown outcome.
                if not commit_started and getattr(error, "errno", None) in (2006, 2013, 2055):
                    error._dirwalker_uncommitted = True
                self._rollback_preserving_error(connection)
                raise
            finally:
                self._close_preserving_error(cursor)


    def complete_detailed_batch(self, jobid, root_path, results, hostid=None):
        """Persist local statistics for a batch and compact completed subtrees."""
        return self._retry_on_lock_error(
            self._complete_detailed_batch_once,
            jobid, root_path, results, hostid=hostid,
        )


    def _complete_detailed_batch_once(self, jobid, root_path, results, hostid=None):
        """Persist one detailed batch in a single transaction."""
        with self.connect() as connection:
            cursor = connection.cursor()
            commit_started = False
            try:
                cursor.execute(
                    f"SELECT hostid FROM {self.TABLE_JOBS} WHERE jobid = %s",
                    (jobid,),
                )
                job_row = cursor.fetchone()
                if job_row is None:
                    raise RuntimeError(f"Job {jobid} existiert nicht mehr.")
                job_hostid = int(job_row[0])
                if hostid is not None and int(hostid) != job_hostid:
                    raise RuntimeError("Host-ID des Detailscans stimmt nicht mit dem Job überein.")
                hostid = job_hostid
                now = self.utc_now
                for result in results:
                    work = result["work"]
                    if result.get("excluded"):
                        cursor.execute(
                            f"UPDATE {self.work_table_name(hostid)} SET state = 'done' "
                            "WHERE jobid = %s AND stage = 'detailed' AND hostid = %s "
                            "AND (path = %s OR LEFT(path, CHAR_LENGTH(%s)) = %s)",
                            (jobid, hostid, work["path"],
                             work["path"].rstrip("/") + "/", work["path"].rstrip("/") + "/"),
                        )
                        continue
                    stats = result["stats"]
                    cursor.execute(
                        f"""UPDATE {self.TABLE_DIRECTORIES}
                            SET file_count = %s, subdirectory_count = %s,
                                size_bytes = %s, file_list_sha256 = %s,
                                files_sha256 = %s,
                                localfilesize = %s, localfilecount = %s,
                                emptyfilecount = %s,
                                subdircount = %s, otherentrycount = %s, scanned_at = %s,
                                files_lastwrite = %s, time_inode_changed = %s
                            WHERE hostid = %s AND root_path = %s AND path_hash = %s""",
                        (
                            stats["localfilecount"], stats["subdircount"],
                            stats["localfilesize"], stats["filehash"],
                            stats["filehash"],
                            stats["localfilesize"], stats["localfilecount"],
                            stats.get("emptyfilecount", 0),
                            stats["subdircount"], stats["otherentrycount"], now,
                            stats.get("files_lastwrite"), stats.get("time_inode_changed"),
                            hostid, root_path, work["path_hash"],
                        ),
                    )
                    cursor.execute(
                        f"""UPDATE {self.work_table_name(hostid)} SET state = 'expanded'
                            WHERE jobid = %s AND stage = 'detailed' AND hostid = %s AND path_hash = %s""",
                        (jobid, hostid, work["path_hash"]),
                    )
                    permission_parent = result.get("permission_parent")
                    if permission_parent is not None:
                        cursor.execute(
                            f"UPDATE {self.TABLE_DIRECTORIES} "
                            "SET nopermdirs = nopermdirs + 1 "
                            "WHERE hostid = %s AND root_path = %s AND path_hash = %s",
                            (hostid, root_path, _path_hash(permission_parent)),
                        )
                self._compact_done_subtrees(cursor, jobid, "detailed", hostid=hostid)
                cursor.execute(
                    f"UPDATE {self.TABLE_JOBS} SET last_activity = %s, timeupdated = %s "
                    "WHERE jobid = %s AND hostid = %s",
                    (now, now, jobid, hostid),
                )
                commit_started = True
                connection.commit()
            except Exception as error:
                # A disconnected, uncommitted transaction is rolled back by
                # the server. A lost COMMIT response has an unknown outcome.
                if not commit_started and getattr(error, "errno", None) in (2006, 2013, 2055):
                    error._dirwalker_uncommitted = True
                self._rollback_preserving_error(connection)
                raise
            finally:
                self._close_preserving_error(cursor)


    def get_detailed_directory_progress(self, jobid, hostid, root_path):
        """Count pending directories against the complete scan root."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT (SELECT COUNT(*) FROM {self.TABLE_DIRECTORIES} "
                    "WHERE hostid = %s AND root_path = %s), "
                    f"(SELECT COUNT(*) FROM {self.work_table_name(hostid)} "
                    "WHERE jobid = %s AND hostid = %s AND stage = 'detailed' "
                    "AND state = 'pending')",
                    (hostid, root_path, jobid, hostid),
                )
                return cursor.fetchone()
            finally:
                self._close_preserving_error(cursor)


    def count_unfilled_detailed_directories(self, hostid, root_path):
        """Count records still missing statistics written by detailed work."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {self.TABLE_DIRECTORIES} "
                    "WHERE hostid = %s AND root_path = %s AND "
                    "(localfilesize IS NULL OR localfilecount IS NULL "
                    "OR subdircount IS NULL OR otherentrycount IS NULL)",
                    (hostid, root_path),
                )
                return cursor.fetchone()[0]
            finally:
                self._close_preserving_error(cursor)


    def get_job_idle_seconds(self, jobid, hostid):
        """Read the committed progress age using the database clock."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT TIMESTAMPDIFF(SECOND, COALESCE(last_activity, timestart, timeupdated), "
                    f"UTC_TIMESTAMP(6)) FROM {self.TABLE_JOBS} "
                    "WHERE jobid = %s AND hostid = %s AND status = 'active'",
                    (jobid, hostid),
                )
                row = cursor.fetchone()
                return row[0] if row else None
            finally:
                self._close_preserving_error(cursor)


    def get_job_status(self, jobid, hostid=None):
        """Den aktuellen Status eines Jobs für den Server abfragen."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT status FROM {self.TABLE_JOBS} WHERE jobid = %s" +
                    (" AND hostid = %s" if hostid is not None else ""),
                    (jobid, hostid) if hostid is not None else (jobid,),
                )
                row = cursor.fetchone()
                return row[0] if row else None
            finally:
                cursor.close()


    def get_recent_runids(self, days=13):
        """Return current job runids whose jobs were started within ``days``."""
        now = self.utc_now
        cutoff = now - timedelta(days=days)
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"""SELECT runid FROM {self.TABLE_JOBS_RUNID}
                        WHERE timecreated >= %s AND timecreated <= %s
                        UNION
                        SELECT runid FROM {self.TABLE_JOBS}
                        WHERE timestart >= %s AND timestart <= %s AND runid > 0""",
                    (cutoff, now, cutoff, now),
                )
                return {row[0] for row in cursor.fetchall()}
            finally:
                cursor.close()


    def is_recently_finished(self, path_hash, path, recent_runids, hostid=0):
        """Whether this exact path has a finished marker from a recent run."""
        if not recent_runids:
            return False
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT finished FROM {self.TABLE_DIRECTORIES} "
                    "WHERE hostid = %s AND path_hash = %s AND BINARY path = BINARY %s",
                    (hostid, path_hash, path),
                )
                row = cursor.fetchone()
                return bool(row and row[0] in recent_runids)
            finally:
                cursor.close()


    def rebuild_subtree_statistics(self, root_path, hostid=0):
        """Stream a directory tree in postorder and persist its derived metadata.

        The recursive result is read from an explicitly unbuffered cursor in
        batches. A separate connection writes each completed node, so Python
        never retains the full result set or a full node graph in memory.
        Children are ordered by binary directory name so the incremental
        subdirectory digest is deterministic for every parent.
        """
        root_path = os.path.realpath(os.path.abspath(root_path))
        read_connection = self.connect()
        write_connection = self.connect()
        read_cursor = None
        write_cursor = None
        try:
            read_cursor = read_connection.cursor(buffered=False)
            write_cursor = write_connection.cursor()
            read_cursor.execute(
                f"SELECT dirid FROM {self.TABLE_DIRECTORIES} "
                "WHERE hostid = %s AND root_path = %s AND path_hash = %s",
                (hostid, root_path, _path_hash(root_path)),
            )
            root_row = read_cursor.fetchone()
            if root_row is None:
                raise RuntimeError(
                    f"Wurzelverzeichnis fehlt in directories: {root_path}"
                )
            root_id = root_row[0]
            read_cursor.execute(
                f"""WITH RECURSIVE tree AS (
                        SELECT hostid, dirid AS id, parentdir AS parent_id, 0 AS tiefe
                        FROM {self.TABLE_DIRECTORIES}
                        WHERE hostid = %s AND dirid = %s AND root_path = %s

                        UNION ALL

                        SELECT d.hostid, d.dirid AS id, d.parentdir AS parent_id,
                               t.tiefe + 1 AS tiefe
                        FROM {self.TABLE_DIRECTORIES} d
                        JOIN tree t ON d.parentdir = t.id AND d.hostid = t.hostid
                        WHERE d.hostid = %s AND d.root_path = %s
                    )
                    SELECT t.id, t.parent_id, t.tiefe,
                           CASE WHEN EXISTS (
                               SELECT 1 FROM {self.TABLE_DIRECTORIES} c
                                   WHERE c.hostid = t.hostid
                                     AND c.parentdir = t.id AND c.root_path = %s
                           ) THEN 0 ELSE 1 END AS ist_blatt,
                           d.name, d.localfilesize, d.files_sha256, d.subdircount,
                           (SELECT COUNT(*) FROM {self.TABLE_DIRECTORIES} all_children
                            WHERE all_children.hostid = t.hostid AND all_children.parentdir = t.id)
                    FROM tree t
                    JOIN {self.TABLE_DIRECTORIES} d
                      ON d.hostid = t.hostid AND d.dirid = t.id
                    ORDER BY t.tiefe DESC, t.parent_id, BINARY d.name, t.id""",
                (hostid, root_id, root_path, hostid, root_path, root_path),
            )

            def empty_children():
                return {
                    "count": 0,
                    "complete": True,
                    "size": 0,
                    "levels": [],
                    "digest": hashlib.sha256(),
                }

            child_summaries = {}
            root_completed = False
            while True:
                batch = read_cursor.fetchmany(1000)
                if not batch:
                    break
                for dirid, parent_id, _depth, is_leaf, name, local_size, files_hash, expected_children, actual_children in batch:
                    children = child_summaries.pop(dirid, None) or empty_children()
                    if bool(is_leaf) != (children["count"] == 0):
                        raise RuntimeError(
                            f"Blattmarkierung stimmt für directories.dirid={dirid} nicht."
                        )

                    # Leaf first: the child's combined digest is complete before
                    # it is folded into its parent's subdirectory digest.
                    ready = (files_hash is not None and local_size is not None
                             and children['complete']
                             and expected_children == actual_children == children['count'])
                    if not ready:
                        write_cursor.execute(f"""UPDATE {self.TABLE_DIRECTORIES}
                            SET subdirs_sha256 = NULL, combined_sha256 = NULL,
                                subdirectory_hash = NULL, subtree_size_bytes = NULL,
                                subtree_structure = NULL, zero_content_only = FALSE
                            WHERE hostid = %s AND root_path = %s AND dirid = %s""",
                            (hostid, root_path, dirid))
                        if dirid == root_id:
                            root_completed = True
                        if dirid != root_id and parent_id is not None:
                            parent = child_summaries.setdefault(parent_id, empty_children())
                            parent['count'] += 1
                            parent['complete'] = False
                        continue
                    subdirs_hash = children["digest"].hexdigest()
                    combined = hashlib.sha256()
                    # Domain tags keep the direct-file and child-tree inputs distinct.
                    combined.update(b"FILES\0" + files_hash.encode("ascii") + b"\0")
                    combined.update(b"SUBDIRS\0" + subdirs_hash.encode("ascii") + b"\0")
                    subtree_size = (local_size or 0) + children["size"]
                    structure = "-".join(
                        str(value) for value in children["levels"] + [0]
                    )

                    write_cursor.execute(
                        f"""UPDATE {self.TABLE_DIRECTORIES}
                            SET file_list_sha256 = %s, files_sha256 = %s,
                                subdirs_sha256 = %s, combined_sha256 = %s,
                                subdirectory_hash = %s, subtree_size_bytes = %s,
                                subtree_structure = %s, scanned_at = %s
                            WHERE hostid = %s AND root_path = %s AND dirid = %s""",
                        (
                            files_hash, files_hash, subdirs_hash, combined.hexdigest(),
                            combined.hexdigest(), subtree_size, structure, self.utc_now,
                            hostid, root_path, dirid,
                        ),
                    )
                    if dirid == root_id:
                        root_completed = True
                    if dirid != root_id and parent_id is not None:
                        parent = child_summaries.setdefault(parent_id, empty_children())
                        parent["count"] += 1
                        parent["size"] += subtree_size
                        if not parent["levels"]:
                            parent["levels"].append(0)
                        parent["levels"][0] += 1
                        for level, count in enumerate(children["levels"], start=1):
                            while len(parent["levels"]) <= level:
                                parent["levels"].append(0)
                            parent["levels"][level] += count
                        encoded_name = name.encode("utf-8", "surrogateescape")
                        # Hash the child name and full child hash; this propagates
                        # every descendant change toward the root.
                        parent["digest"].update(
                            b"DIR\0" + encoded_name + b"\0"
                            + combined.hexdigest().encode("ascii") + b"\0"
                        )

                write_connection.commit()

            if not root_completed:
                raise RuntimeError(
                    f"Unterbaum-Berechnung hat die Wurzel {root_path} (Host {hostid}) "
                    "nicht verarbeitet; Scan darf nicht als abgeschlossen gelten."
                )
            if child_summaries:
                raise RuntimeError(
                    "Rekursiver Verzeichnisbaum enthält nicht zugeordnete "
                    f"Unterbäume: {len(child_summaries)} offene Knoten."
                )
            write_connection.commit()
        except Exception:
            write_connection.rollback()
            raise
        finally:
            if read_cursor is not None:
                read_cursor.close()
            if write_cursor is not None:
                write_cursor.close()
            read_connection.close()
            write_connection.close()
        self.refresh_zero_content_flags(hostid=hostid)


    def _acquire_runid(self, cursor):
        """Serialize global MAX(runid)+1 allocation between server processes."""
        lock_name = ("filewalker-runid:" + self.database)[:64]
        cursor.execute("SELECT GET_LOCK(%s, 30)", (lock_name,))
        row = cursor.fetchone()
        if not row or row[0] != 1:
            raise RuntimeError("Zeitüberschreitung beim Sperren der runid-Vergabe.")
        cursor.execute(
            f"""SELECT GREATEST(
                    COALESCE((SELECT MAX(runid) FROM {self.TABLE_JOBS}), 0),
                    COALESCE((SELECT MAX(runid) FROM {self.TABLE_JOBS_RUNID}), 0)
                ) + 1"""
        )
        return lock_name, cursor.fetchone()[0]


    def claim_job(self, status, include_binarycheck=False, hostid=0, userhost=None):
        """Atomically activate the oldest matching job with a new global runid."""
        with self.connect() as connection:
            cursor = connection.cursor()
            lock_name = None
            try:
                lock_name, runid = self._acquire_runid(cursor)
                cursor.execute(
                    f"""SELECT jobid, startdirname, task, stage, skipdirs, scanall,
                               hostid, userhost
                        FROM {self.TABLE_JOBS} WHERE status = %s
                          AND hostid = %s
                          {"AND userhost = %s" if userhost is not None else ""}
                        {'' if include_binarycheck else "AND task <> 'binarycheck'"}
                        ORDER BY jobid
                        LIMIT 1 FOR UPDATE""",
                    (status, hostid, userhost) if userhost is not None else (status, hostid),
                )
                row = cursor.fetchone()
                if row is None:
                    connection.commit()
                    return None
                jobid, startdirname, task, stage, skipdirs, scanall, job_hostid, job_userhost = row
                now = self.utc_now
                cursor.execute(
                    f"""UPDATE {self.TABLE_JOBS}
                        SET status = 'active', runid = %s, timestart = %s,
                            timeupdated = %s, last_activity = %s, timeend = NULL, errmsg = NULL,
                            usererror = NULL
                        WHERE jobid = %s AND status = %s AND hostid = %s""",
                    (runid, now, now, now, jobid, status, job_hostid),
                )
                if cursor.rowcount != 1:
                    self._rollback_preserving_error(connection)
                    return None
                self.write_run_record(
                    cursor, runid, jobid, task, "active", stage, timecreated=now,
                )
                connection.commit()
                return {
                    "jobid": jobid, "startdirname": startdirname, "task": task,
                    "stage": stage, "skipdirs": skipdirs, "status": status,
                    "scanall": scanall, "runid": runid,
                    "hostid": job_hostid, "userhost": job_userhost,
                }
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                if lock_name is not None:
                    try:
                        cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                        cursor.fetchone()
                    except Exception:
                        pass
                cursor.close()


    def reschedule_full_coarse(self, jobid, hostid):
        """Discard this job's checkpoints and queue a full coarse scan atomically."""
        return self._retry_on_lock_error(self._reschedule_full_coarse_once, jobid, hostid)

    def _reschedule_full_coarse_once(self, jobid, hostid):
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT jobid FROM {self.TABLE_JOBS} "
                    "WHERE jobid = %s AND hostid = %s AND status = 'active' FOR UPDATE",
                    (jobid, hostid),
                )
                if cursor.fetchone() is None:
                    return False
                cursor.execute(
                    f"DELETE FROM {self.work_table_name(hostid)} WHERE jobid = %s AND hostid = %s",
                    (jobid, hostid),
                )
                cursor.execute(
                    f"UPDATE {self.TABLE_JOBS} SET status = 'pending', stage = 'coarse', "
                    "scanall = 1, errmsg = NULL, usererror = NULL, timeend = NULL, "
                    "timeupdated = %s, last_activity = %s WHERE jobid = %s AND hostid = %s",
                    (self.utc_now, self.utc_now, jobid, hostid),
                )
                connection.commit()
                return True
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                self._close_preserving_error(cursor)


    def set_job_status(self, jobid, status, error=None, stage=None, usererror=None, hostid=None):
        """Change status, retrying transactions chosen as deadlock victims."""
        return self._retry_on_lock_error(
            self._set_job_status_once,
            jobid, status, error=error, stage=stage,
            usererror=usererror, hostid=hostid,
        )


    @staticmethod
    def _retry_on_lock_error(operation, *args, **kwargs):
        """Retry lock failures and explicitly uncommitted connection losses."""
        max_attempts = 9
        for attempt in range(max_attempts):
            try:
                return operation(*args, **kwargs)
            except Exception as exc:
                # MariaDB rolls back the deadlocked transaction. Reopen it and
                # retry the complete transaction, never just its failed query.
                error_code = getattr(exc, "errno", None)
                retryable = error_code in (1205, 1213) or (
                    error_code in (2006, 2013, 2055)
                    and getattr(exc, "_dirwalker_uncommitted", False)
                )
                if not retryable or attempt + 1 == max_attempts:
                    raise
                delay = (3 ** attempt) / 3.0
                logging.warning(
                    "%s hit retryable database error %s; retry %d/%d in %.2fs",
                    operation.__name__, error_code, attempt + 1,
                    max_attempts - 1, delay,
                )
                if error_code in (2006, 2013, 2055):
                    # Transport recovery must not inherit the lock backoff's
                    # multi-minute delays, especially after laptop suspend.
                    time.sleep(30)
                else:
                    time.sleep(delay)


    def _set_job_status_once(
        self, jobid, status, error=None, stage=None, usererror=None, hostid=None
    ):
        """Run one transaction; pending retains its current runid."""
        with self.connect() as connection:
            cursor = connection.cursor()
            lock_name = None
            try:
                if status != "pending":
                    lock_name, runid = self._acquire_runid(cursor)
                else:
                    runid = None
                now = self.utc_now
                cursor.execute(
                    f"SELECT startdirname, task, stage, runid, hostid FROM {self.TABLE_JOBS} "
                    "WHERE jobid = %s AND status = 'active' "
                    + ("AND hostid = %s " if hostid is not None else "") + "FOR UPDATE",
                    (jobid, hostid) if hostid is not None else (jobid,),
                )
                active_job = cursor.fetchone()
                if active_job is None:
                    connection.commit()
                    return None
                startdirname, task, old_stage, active_runid, job_hostid = active_job
                if hostid is not None and int(hostid) != int(job_hostid):
                    raise RuntimeError("Host-ID des Statusupdates stimmt nicht mit dem Job überein.")
                hostid = int(job_hostid)
                resulting_stage = stage or old_stage
                if status == "pending":
                    runid = active_runid
                    cursor.execute(
                        f"""UPDATE {self.TABLE_JOBS}
                            SET status = %s, stage = COALESCE(%s, stage),
                                timeupdated = %s, last_activity = %s, errmsg = %s
                            WHERE jobid = %s AND status = 'active' AND hostid = %s""",
                        (status, stage, now, now, error, jobid, hostid),
                    )
                elif status in ("closed", "paused", "error"):
                    cursor.execute(
                        f"""UPDATE {self.TABLE_JOBS}
                            SET status = %s, runid = %s,
                                stage = COALESCE(%s, stage), timeupdated = %s,
                                timeend = %s, last_activity = %s, errmsg = %s,
                                usererror = %s
                            WHERE jobid = %s AND status = 'active' AND hostid = %s""",
                        (status, runid, stage, now, now, now, error, usererror, jobid, hostid),
                    )
                else:
                    cursor.execute(
                        f"""UPDATE {self.TABLE_JOBS}
                            SET status = %s, runid = %s,
                                stage = COALESCE(%s, stage), timeupdated = %s,
                                last_activity = %s, errmsg = %s, usererror = %s
                            WHERE jobid = %s AND status = 'active' AND hostid = %s""",
                        (status, runid, stage, now, now, error, usererror, jobid, hostid),
                    )
                updated = cursor.rowcount == 1
                if updated and status != "pending":
                    self.write_run_record(
                        cursor, runid, jobid, task, status, resulting_stage,
                        timecreated=now,
                    )
                if updated and (
                    status == "closed"
                    or (status == "pending" and stage == "detailed" and old_stage == "coarse")
                ):
                    root_path = os.path.realpath(os.path.abspath(startdirname))
                    cursor.execute(
                        f"UPDATE {self.TABLE_DIRECTORIES} SET finished = %s "
                        "WHERE hostid = %s AND root_path = %s",
                        (runid, hostid, root_path),
                    )
                connection.commit()
                return runid if updated else None
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                if lock_name is not None:
                    try:
                        cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                        cursor.fetchone()
                    except Exception:
                        pass
                cursor.close()


    def list_live_excluded_absdirs(self, hostid=0):
        """Read current absolute exclusions for exactly one host."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                sqlstructure.SQLStructure.select_table_count(cursor, self.database, self.TABLE_LIVE_EXCLUDED_ABSDIRS)
                if cursor.fetchone()[0] == 0:
                    return []
                cursor.execute(
                    f"SELECT path FROM {self.TABLE_LIVE_EXCLUDED_ABSDIRS} "
                    "WHERE hostid = %s ORDER BY BINARY path", (int(hostid),),
                )
                return [row[0] for row in cursor.fetchall()]
            finally:
                cursor.close()


    def _ensure_no_priority_worker(self, cursor, hostid):
        """Check live local PIDs; history and stale/reused PIDs do not block startup."""
        cursor.execute(
            f"SELECT r.runid, r.jobid, r.pid, r.process_start "
            f"FROM {self.TABLE_JOBS_RUNID} r LEFT JOIN {self.TABLE_JOBS} j ON j.jobid = r.jobid "
            "WHERE r.task = 'binarycheck' AND r.stage = 'highpriority' "
            "AND r.pid IS NOT NULL AND r.process_host = %s "
            "AND (j.hostid = %s OR j.jobid IS NULL)",
            (socket.gethostname(), hostid),
        )
        for runid, jobid, pid, recorded_start in cursor.fetchall():
            current_start = _process_start_token(pid)
            if current_start is not None and (
                    recorded_start is None or current_start == recorded_start):
                raise RuntimeError(
                    f'Priority-Binarycheck läuft bereits: PID {pid}, Runid {runid}, '
                    f'Job {jobid}, Rechner {socket.gethostname()}; neuer Start abgebrochen.'
                )

    def ensure_no_priority_worker(self, hostid):
        """Reject startup if this database/host already has a live local priority worker."""
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                self._ensure_no_priority_worker(cursor, hostid)
            finally:
                cursor.close()

    def register_binarycheck_priority_run(self, jobid, hostid, pid):
        """Record the spawned PID with a unique run, leaving the owning jobs row intact."""
        started = _process_start_token(pid)
        if started is None:
            raise RuntimeError(f'Priority-Prozess PID {pid} ist bereits beendet.')
        return self._retry_on_lock_error(
            self._register_binarycheck_priority_run_once, jobid, hostid,
            int(pid), socket.gethostname(), started,
        )

    def _register_binarycheck_priority_run_once(self, jobid, hostid, pid, process_host, started):
        with self.connect() as connection:
            cursor = connection.cursor()
            lock_name = None
            try:
                lock_name, runid = self._acquire_runid(cursor)
                # Serialize competing launches with the existing global runid
                # allocator: only the first live PID can register and receive work.
                self._ensure_no_priority_worker(cursor, hostid)
                cursor.execute(
                    f"SELECT task FROM {self.TABLE_JOBS} "
                    "WHERE jobid = %s AND hostid = %s AND status = 'active'",
                    (jobid, hostid),
                )
                row = cursor.fetchone()
                if row is None or row[0] != 'binarycheck':
                    raise RuntimeError(f"Binarycheck-Job {jobid} ist für Host {hostid} nicht aktiv.")
                self.write_run_record(
                    cursor, runid, jobid, 'binarycheck', 'active', 'highpriority',
                    pid=pid, process_host=process_host, process_start=started,
                )
                connection.commit()
                return runid
            except BaseException:
                self._rollback_preserving_error(connection)
                raise
            finally:
                if lock_name is not None:
                    try:
                        cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                        cursor.fetchone()
                    except Exception:
                        pass
                cursor.close()

    def run_binarycheck_job(self, jobid, hostid=None):
        """Run the binary comparison worker with its existing job lifecycle."""
        self.run_remote_binarycheck(jobid, hostid=hostid)
        return Binarycheck(self).loop(jobid, hostid=hostid)

    def _save_binarycheck_result_once(
        self, jobid, hostid, left_id, right_id, runid, status,
        method=None, source=None, error_message=None, newly_verified=(),
        _connection=None, _zero_content_ids=(),
    ):
        """Persist one outcome and its successful subtrees in a short transaction.

        The caller retries the complete transaction on lock errors. Discovered
        subtree pairs do not count as requested work, unless already queued.
        """
        with (self.connect() if _connection is None else nullcontext(_connection)) as connection:
            cursor = connection.cursor()
            try:
                now = self.utc_now
                source_left, source_right = source or (None, None)
                cursor.execute(f"""
                    UPDATE {self.TABLE_DOUBLE_JOBS}
                    SET status = %s, verification_method = %s,
                        source_left_dirid = %s, source_right_dirid = %s,
                        finished_at = %s, error_message = %s, runid = %s
                    WHERE jobid = %s AND hostid = %s
                      AND left_dirid = %s AND right_dirid = %s
                      AND status IN ('pending', 'unreadable')
                """, (status, method, source_left, source_right, now, error_message,
                      runid, jobid, hostid, left_id, right_id))

                result_changed = bool(cursor.rowcount)
                if newly_verified:
                    paths = sorted({path for pair in newly_verified for path in pair})
                    by_path = {}
                    for offset in range(0, len(paths), 500):
                        hashes = sorted({_path_hash(path) for path in paths[offset:offset + 500]})
                        placeholders = ','.join(['%s'] * len(hashes))
                        cursor.execute(f"""
                            SELECT dirid, path FROM {self.TABLE_DIRECTORIES}
                            WHERE hostid = %s AND path_hash IN ({placeholders})
                        """, (hostid, *hashes))
                        for dirid, path in cursor.fetchall():
                            by_path[os.path.abspath(path)] = dirid
                    rows = []
                    for left_path, right_path in newly_verified:
                        a, b = by_path.get(left_path), by_path.get(right_path)
                        if (a is None or b is None or a == b
                                or (hostid, a) in _zero_content_ids
                                or (hostid, b) in _zero_content_ids):
                            continue
                        a, b = sorted((a, b))
                        if (a, b) == (left_id, right_id):
                            continue
                        inherited = status == 'equal'
                        rows.append((jobid, hostid, hostid, a, b,
                                     'inherited' if inherited else 'direct',
                                     left_id if inherited else None,
                                     right_id if inherited else None, now, runid))
                    if rows:
                        cursor.executemany(f"""
                            INSERT INTO {self.TABLE_DOUBLE_JOBS}
                                (jobid, hostid, hostidB, left_dirid, right_dirid, status,
                                 verification_method, source_left_dirid, source_right_dirid,
                                 finished_at, is_requested, runid)
                            VALUES (%s, %s, %s, %s, %s, 'equal', %s, %s, %s, %s, FALSE, %s)
                            ON DUPLICATE KEY UPDATE
                                verification_method = IF(status IN ('pending', 'unreadable'), VALUES(verification_method), verification_method),
                                source_left_dirid = IF(status IN ('pending', 'unreadable'), VALUES(source_left_dirid), source_left_dirid),
                                source_right_dirid = IF(status IN ('pending', 'unreadable'), VALUES(source_right_dirid), source_right_dirid),
                                finished_at = IF(status IN ('pending', 'unreadable'), VALUES(finished_at), finished_at),
                                error_message = IF(status IN ('pending', 'unreadable'), NULL, error_message),
                                runid = IF(status IN ('pending', 'unreadable'), VALUES(runid), runid),
                                status = IF(status IN ('pending', 'unreadable'), 'equal', status)
                        """, rows)
                if status in ('equal','different') and result_changed:
                    stamped={(hostid,left_id),(hostid,right_id)}
                    if newly_verified:
                        stamped.update((hostid,dirid) for row in rows for dirid in row[3:5])
                    self.mark_binarycheck_hashed(cursor,jobid,stamped,now)
                if _connection is None:
                    connection.commit()
            except Exception:
                self._rollback_preserving_error(connection)
                raise
            finally:
                cursor.close()


class Binarycheck:
    """Compare normal pairs and coordinate one priority subprocess per worker.

    The priority process owns delegated pairs. Both processes retain the existing
    result schema; tmp_highpriority remains a read-only source of hints.
    """

    PRIORITY_INTERVAL = 120.0
    WRITE_WINDOW = 10.0
    ACK_POLL_INTERVAL = 5.0
    BATCH_SIZE = 20
    BATCH_SECONDS = 600.0

    def __init__(self, database):
        self.database = database
        self._next_priority_check = 0.0
        self._priority_ids = set()
        self._priority_process = None
        self._priority_started = False
        self._priority_idle = True
        self._priority_ipc = None
        self._write_lock = None
        self._previous_sigterm = None
        self._priority_runid = None
        self._priority_verbose = False
        self._zero_content_ids = set()
        self._priority_jobid = None
        self._priority_hostid = None

    def loop(self, jobid, hostid=None):
        """Finish normal work and every delegated priority batch before closing."""
        try:
            self._run_job(jobid, hostid)
            self._finish_priority()
        finally:
            self._cleanup_priority()

    @staticmethod
    def _terminate_worker(signum, _frame):
        # Let the loop's finally block reap its subprocess on stop/shutdown.
        raise SystemExit(128 + signum)

    def _start_priority(self, jobid, hostid):
        if self._priority_started:
            return
        self.database.ensure_no_priority_worker(hostid)
        context = multiprocessing.get_context('spawn')
        self._priority_jobid = jobid
        self._priority_hostid = hostid
        parent_ipc, child_ipc = context.Pipe(duplex=True)
        self._write_lock = context.Lock()
        worker = Binarycheck(self.database)
        process = context.Process(
            target=worker.highpriorityprovess,
            args=(jobid, hostid, child_ipc, self._write_lock),
            name=f'binarycheck-priority-{jobid}',
        )
        try:
            process.start()
        except BaseException:
            parent_ipc.close()
            child_ipc.close()
            self._log_priority_lifecycle('spawn failed')
            raise
        child_ipc.close()
        self._priority_started = True
        self._priority_process = process
        self._priority_ipc = parent_ipc
        self._previous_sigterm = signal.signal(signal.SIGTERM, self._terminate_worker)
        try:
            self._priority_runid = self.database.register_binarycheck_priority_run(
                jobid, hostid, process.pid,
            )
            self._priority_ipc.send(('runid', self._priority_runid))
        except BaseException:
            self._cleanup_priority()
            raise
        self._log_priority_lifecycle('spawned')

    def _log_priority_lifecycle(self, event, exitcode=None):
        """Always send worker lifecycle events to both stdout and syslog."""
        process = self._priority_process
        pid = process.pid if process is not None else None
        message = (
            f"Binarycheck priority subprocess {event}: "
            f"jobid={self._priority_jobid}, hostid={self._priority_hostid}, "
            f"runid={self._priority_runid}, pid={pid}"
        )
        if exitcode is not None:
            message += f", exitcode={exitcode}"
        print(message, flush=True)
        # The binarycheck worker does not import server/client modules. Use the
        # same destination and RFC5424 formatter as the server, at INFO level
        # even in localserver mode, where these lifecycle events are required.
        handler = SysLogHandler(address=('localhost', 5101),
                                facility=SysLogHandler.LOG_DAEMON)
        try:
            handler.setFormatter(IETF_syslog_formatter())
            logger = logging.Logger(f'filewalker.binarycheck.{os.getpid()}', logging.INFO)
            logger.addHandler(handler)
            logger.info(message)
        finally:
            handler.close()

    def _delegate_priority(self, pending, jobid, hostid):
        if not self._priority_ids or not self._priority_idle:
            return pending
        selected = [(row[0], row[1]) for row in pending
                    if row[0] in self._priority_ids or row[1] in self._priority_ids]
        if not selected:
            return pending
        self._start_priority(jobid, hostid)
        self._priority_ipc.send(('work', selected))
        self._priority_idle = False
        selected = set(selected)
        return deque(row for row in pending if (row[0], row[1]) not in selected)

    def _service_priority_ipc(self):
        ipc = self._priority_ipc
        if ipc is None:
            return
        while ipc.poll():
            try:
                message = ipc.recv()
            except EOFError:
                break
            kind = message[0]
            if kind == 'write_request':
                token = message[1]
                ipc.send(('waiting', token, time.monotonic() + self.WRITE_WINDOW))
                time.sleep(self.WRITE_WINDOW)
                ipc.send(('reset', token))
            elif kind == 'idle':
                self._priority_idle = True
            elif kind == 'error':
                raise RuntimeError('Priority binarycheck failed:\n' + message[1])
        if (self._priority_process.exitcode is not None
                and self._priority_process.exitcode != 0):
            raise RuntimeError(f'Priority binarycheck exited with {self._priority_process.exitcode}')

    def _finish_priority(self):
        process = self._priority_process
        if process is None:
            return
        finishing = False
        while process.is_alive():
            self._service_priority_ipc()
            if self._priority_idle and not finishing:
                self._priority_ipc.send(('finish',))
                finishing = True
            process.join(timeout=0.05)
        self._service_priority_ipc()
        if not finishing:
            raise RuntimeError('Priority binarycheck exited before finishing its work')

    def _cleanup_priority(self):
        process = self._priority_process
        if process is not None:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=5)
            else:
                process.join()
            try:
                self._log_priority_lifecycle('closed', exitcode=process.exitcode)
            finally:
                process.close()
                self._priority_process = None
        if self._priority_ipc is not None:
            self._priority_ipc.close()
            self._priority_ipc = None
        if self._previous_sigterm is not None:
            signal.signal(signal.SIGTERM, self._previous_sigterm)
            self._previous_sigterm = None

    def _enable_priority_verbose(self, _signum, _frame):
        """SIGUSR1 enables per-directory output for this subprocess's lifetime."""
        self._priority_verbose = True

    def _report_priority_finished(self, jobid, hostid, runid, left_id, right_id, status):
        if self._priority_verbose:
            for dirid in (left_id, right_id):
                print(f"Binarycheck priority finished dirid={dirid}, status={status}, "
                      f"jobid={jobid}, hostid={hostid}, runid={runid}, pid={os.getpid()}",
                      flush=True)

    def highpriorityprovess(self, jobid, hostid, ipc, write_lock):
        """Spawn entry: accept delegated pairs and batch their comparison results."""
        previous_usr1 = signal.signal(signal.SIGUSR1, self._enable_priority_verbose)
        try:
            while True:
                message = ipc.recv()
                if message[0] == 'finish':
                    return
                if message[0] == 'runid':
                    self._priority_runid = message[1]
                    continue
                if message[0] == 'work':
                    self._run_job(jobid, hostid, selected=message[1],
                                  ipc=ipc, write_lock=write_lock)
                    ipc.send(('idle',))
                # Late reset messages belong to already completed write windows.
        except BaseException:
            try:
                ipc.send(('error', traceback.format_exc()))
            except (BrokenPipeError, EOFError, OSError):
                pass
            raise
        finally:
            signal.signal(signal.SIGUSR1, previous_usr1)
            ipc.close()

    def _batch_due(self, batch, started, exhausted=False):
        return bool(batch) and (len(batch) >= self.BATCH_SIZE or exhausted
                                or time.monotonic() - started >= self.BATCH_SECONDS)

    def _flush_priority_batch(self, batch, ipc, write_lock):
        # Sequence numbers keep a delayed acknowledgment/reset from granting
        # permission for a different batch or a retried write window.
        token = getattr(self, '_write_token', 0)
        while True:
            token += 1
            self._write_token = token
            ipc.send(('write_request', token))
            retry = False
            while not retry:
                time.sleep(self.ACK_POLL_INTERVAL)
                while ipc.poll():
                    message = ipc.recv()
                    if message[0] == 'waiting' and message[1] == token:
                        deadline = message[2]
                        with write_lock:
                            if time.monotonic() >= deadline:
                                retry = True
                                break
                            self.database._retry_on_lock_error(
                                self._save_priority_batch_once, tuple(batch),
                            )
                        ipc.send(('committed', token))
                        return
                    if message[0] == 'reset' and message[1] == token:
                        retry = True
                        break

    def _save_priority_batch_once(self, batch):
        """Commit all outcomes together; lock retries replay the entire batch."""
        with self.database.connect() as connection:
            try:
                for outcome in batch:
                    self.database._save_binarycheck_result_once(
                        *outcome, _connection=connection, _zero_content_ids=self._zero_content_ids,
                    )
                connection.commit()
            except BaseException:
                DataBaseConnectionCore._rollback_preserving_error(connection)
                raise

    def _refresh_priority(self, jobid, hostid):
        now = time.monotonic()
        if now < self._next_priority_check:
            return
        self._next_priority_check = now + self.PRIORITY_INTERVAL
        self._priority_ids = set()
        with self.database.connect() as connection:
            cursor = connection.cursor()
            try:
                # SHOW includes connection-local temporary tables as well as
                # ordinary tables, without assuming an optional table exists.
                try:
                    sqlstructure.SQLStructure.select_priority_columns(cursor)
                    columns = {row[0] for row in cursor.fetchall()}
                    if 'dirid' not in columns:
                        raise RuntimeError("tmp_highpriority requires a dirid column")
                    filters, params = [], []
                    for column, value in (('hostid', hostid), ('jobid', jobid)):
                        if column in columns:
                            filters.append(f"`{column}` = %s")
                            params.append(value)
                    where = ' WHERE ' + ' AND '.join(filters) if filters else ''
                    cursor.execute("SELECT dirid FROM `tmp_highpriority`" + where,
                                   tuple(params))
                    self._priority_ids = {row[0] for row in cursor.fetchall()}
                except Error as exc:
                    # A producer may drop the table between discovery and read.
                    code = getattr(exc, 'errno', None)
                    if code is None and exc.args:
                        code = exc.args[0]
                    if code != 1146:
                        raise
            finally:
                cursor.close()

    def _run_job(self, jobid, hostid=None, selected=None, ipc=None, write_lock=None):
        """Compare canonical pairs and reuse persisted positive subtree results.

        Cached equality belongs to this job/host and assumes unchanged files.
        A restarted job copies these records, including discovered subdirectories.
        """
        with self.database.connect() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT runid, hostid FROM {self.database.TABLE_JOBS} "
                    "WHERE jobid = %s AND status = 'active'", (jobid,),
                )
                job_row = cursor.fetchone()
                if job_row is None:
                    raise RuntimeError(f"Binarycheck-Job {jobid} ist nicht aktiv.")
                runid = job_row[0]
                if ipc is not None:
                    if self._priority_runid is None:
                        raise RuntimeError('Priority subprocess has no registered runid')
                    runid = self._priority_runid
                job_hostid = int(job_row[1]) if len(job_row) > 1 else 0
                if hostid is not None and int(hostid) != job_hostid:
                    raise RuntimeError("Host-ID des Binarycheck-Jobs stimmt nicht überein.")
                cursor.execute(f"""
                    SELECT dj.left_dirid, dj.right_dirid,
                           left_dir.path, right_dir.path,
                           left_dir.subtree_structure, right_dir.subtree_structure,
                           left_dir.localfilecount, left_dir.subdircount,
                           right_dir.localfilecount, right_dir.subdircount
                    FROM {self.database.TABLE_DOUBLE_JOBS} dj
                    LEFT JOIN {self.database.TABLE_DIRECTORIES} left_dir
                      ON left_dir.hostid = dj.hostid AND left_dir.dirid = dj.left_dirid
                    LEFT JOIN {self.database.TABLE_DIRECTORIES} right_dir
                      ON right_dir.hostid = dj.hostid AND right_dir.dirid = dj.right_dirid
                    WHERE dj.jobid = %s AND dj.hostid = %s AND dj.hostidB = dj.hostid AND dj.is_requested = TRUE
                      AND dj.status IN ('pending', 'unreadable')
                    ORDER BY dj.left_dirid, dj.right_dirid
                """, (jobid, job_hostid))
                pending = cursor.fetchall()
                cursor.execute(f"""
                    SELECT dj.left_dirid, dj.right_dirid, l.path, r.path
                    FROM {self.database.TABLE_DOUBLE_JOBS} dj
                    JOIN {self.database.TABLE_DIRECTORIES} l
                      ON l.hostid = dj.hostid AND l.dirid = dj.left_dirid
                    JOIN {self.database.TABLE_DIRECTORIES} r
                      ON r.hostid = dj.hostid AND r.dirid = dj.right_dirid
                    WHERE dj.jobid = %s AND dj.hostid = %s AND dj.hostidB = dj.hostid AND dj.status = 'equal'
                """, (jobid, job_hostid))
                verified_roots = list(cursor.fetchall())
            finally:
                cursor.close()

        is_excluded = self.database.binarycheck_exclusion_matcher(jobid, job_hostid)
        self._zero_content_ids = self.database.zero_content_directory_ids(hostid=job_hostid)
        if selected is not None:
            selected = set(selected)
            pending = [row for row in pending if (row[0], row[1]) in selected]
        batch = []
        batch_started = time.monotonic() if ipc is not None else None
        verified_pairs = {_directory_pair_key(left, right)
                          for _, _, left, right in verified_roots}
        pending.sort(key=lambda row: (
            min(len(Path(row[2]).parts) if row[2] else float("inf"),
                len(Path(row[3]).parts) if row[3] else float("inf")), row[0], row[1],
        ))
        pending = deque(pending)
        while pending:
            if ipc is None:
                self._service_priority_ipc()
                self._refresh_priority(jobid, job_hostid)
                pending = self._delegate_priority(pending, jobid, job_hostid)
                if not pending:
                    break
            (left_id, right_id, left_path, right_path, left_structure,
             right_structure, left_files, left_subdirs, right_files,
             right_subdirs) = pending.popleft()
            status, method, source = 'unreadable', None, None
            errors, newly_verified = [], []
            if ((job_hostid, left_id) in self._zero_content_ids
                    and (job_hostid, right_id) in self._zero_content_ids):
                status = 'skipped_empty'
            elif (left_path is None or right_path is None
                  or left_structure is None or right_structure is None):
                errors.append('Directory record or subtree_structure is missing.')
            elif left_structure != right_structure:
                status, method = 'different', 'direct'
            else:
                # A verified parent proves corresponding children equal, even if
                # those children did not have separate cache records previously.
                for source_left, source_right, root_left, root_right in verified_roots:
                    for a, b in ((root_left, root_right), (root_right, root_left)):
                        if (_path_is_within(a, left_path) and _path_is_within(b, right_path)
                                and os.path.relpath(left_path, a) == os.path.relpath(right_path, b)):
                            source = (source_left, source_right)
                            break
                    if source is not None:
                        break
                if source is not None:
                    status, method = 'equal', 'inherited'
                else:
                    same = _directory_contents_equal(
                        left_path, right_path, verified_pairs=verified_pairs,
                        newly_verified=newly_verified, error_details=errors, is_excluded=is_excluded,
                    )
                    status = 'equal' if same is True else 'different' if same is False else 'unreadable'
                    method = 'direct'
                    if same is None and not errors:
                        errors.append('File access prevented a complete comparison.')
            outcome = (jobid, job_hostid, left_id, right_id, runid, status,
                       method, source, '; '.join(errors) or None,
                       tuple(newly_verified))
            if ipc is not None:
                self._report_priority_finished(jobid, job_hostid, runid,
                                               left_id, right_id, status)
                batch.append(outcome)
                if self._batch_due(batch, batch_started, not pending):
                    self._flush_priority_batch(batch, ipc, write_lock)
                    batch = []
                    batch_started = time.monotonic()
            else:
                # A request received during comparison is acknowledged before
                # opening the ordinary worker's next write transaction.
                self._service_priority_ipc()
                with self._write_lock if self._write_lock is not None else nullcontext():
                    self.database._retry_on_lock_error(
                        self.database._save_binarycheck_result_once, *outcome,
                        _zero_content_ids=self._zero_content_ids,
                    )
            if status == 'equal':
                verified_pairs.add(_directory_pair_key(left_path, right_path))
                verified_roots.append((left_id, right_id, left_path, right_path))



# Compatibility spelling for existing imports.
DatabaseConnection = DataBaseConnection


database_connection = DataBaseConnection()


def claim_job(status, include_binarycheck=False, hostid=0, userhost=None):
    """Atomically claim a job; the database object assigns its new runid."""
    return database_connection.claim_job(status, include_binarycheck, hostid, userhost)


def set_job_status(jobid, status, error=None, stage=None, usererror=None, hostid=None):
    return database_connection.set_job_status(
        jobid, status, error, stage, usererror, hostid
    )


def _path_hash(path):
    """Return a fast 64-bit BLAKE2b fingerprint as 16 hexadecimal characters."""
    normalized = os.path.normpath(path)
    return hashlib.blake2b(
        normalized.encode("utf-8", "surrogateescape"), digest_size=8
    ).hexdigest()


def _files_have_same_bytes(left_path, right_path, *, error_details=None):
    """Compare files; return None when access errors prevent a full check."""
    try:
        with open(left_path, "rb") as left_file, open(right_path, "rb") as right_file:
            while True:
                left_block = left_file.read(1024 * 1024)
                right_block = right_file.read(1024 * 1024)
                if left_block != right_block:
                    return False
                if not left_block:
                    return True
    except OSError as exc:
        if error_details is not None:
            error_details.append(str(exc))
        return None


def _directory_pair_key(left, right):
    """Identify an unordered path pair without following symbolic links."""
    return tuple(sorted((os.path.abspath(left), os.path.abspath(right))))


def _directory_contents_equal(
    left_root, right_root, *, verified_pairs=None, newly_verified=None, error_details=None, is_excluded=None
):
    """Compare trees; reuse successful pairs while files remain unchanged.

    A pair is cached only after all its entries and descendants have matched.
    Matching child pairs remain useful even if a later sibling differs or is
    unreadable. Neither differences nor incomplete comparisons are cached.
    Return None when access errors prevent a full check.
    """
    is_excluded = is_excluded or (lambda path: False)
    if verified_pairs is None:
        verified_pairs = set()

    try:
        # Completion frames verify children before marking their parent equal.
        pending = [(left_root, right_root, False)]
        while pending:
            left_directory, right_directory, complete = pending.pop()
            key = _directory_pair_key(left_directory, right_directory)
            if key in verified_pairs:
                continue
            if complete:
                verified_pairs.add(key)
                if newly_verified is not None:
                    newly_verified.append(key)
                continue
            with os.scandir(left_directory) as entries:
                left_entries = {entry.name: entry for entry in entries if not is_excluded(entry.path)}
            with os.scandir(right_directory) as entries:
                right_entries = {entry.name: entry for entry in entries if not is_excluded(entry.path)}
            if left_entries.keys() != right_entries.keys():
                return False

            children = []
            for name in sorted(left_entries):
                left_entry = left_entries[name]
                right_entry = right_entries[name]
                left_mode = left_entry.stat(follow_symlinks=False).st_mode
                right_mode = right_entry.stat(follow_symlinks=False).st_mode
                if stat.S_IFMT(left_mode) != stat.S_IFMT(right_mode):
                    return False
                if stat.S_ISDIR(left_mode):
                    children.append((left_entry.path, right_entry.path, False))
                elif stat.S_ISREG(left_mode):
                    file_equal = _files_have_same_bytes(
                        left_entry.path, right_entry.path, error_details=error_details
                    )
                    if file_equal is None:
                        return None
                    if not file_equal:
                        return False
                elif stat.S_ISLNK(left_mode):
                    if os.readlink(left_entry.path) != os.readlink(right_entry.path):
                        return False
                elif left_mode != right_mode:
                    return False
            pending.append((left_directory, right_directory, True))
            pending.extend(children)
    except OSError as exc:
        if error_details is not None:
            error_details.append(str(exc))
        return None
    return True


def _path_is_within(parent, candidate):
    """Return whether candidate equals or is a descendant of parent."""
    try:
        return os.path.commonpath((os.path.normpath(parent), os.path.normpath(candidate))) == os.path.normpath(parent)
    except ValueError:
        return False


_live_excluded_absdirs = {}


def _directory_exclusion_matcher(skipdirs):
    return exclusion_matcher(skipdirs)


def _path_depth(root_path, path):
    """Count directory edges from root_path; the root itself has depth zero."""
    relative = os.path.relpath(os.path.normpath(path), os.path.normpath(root_path))
    if relative == os.curdir:
        return 0
    if relative == os.pardir or relative.startswith(os.pardir + os.sep):
        return -1
    return len(relative.split(os.sep))


def _glob_subdirectories(path, is_excluded):
    """Glob one level, including hidden directories, without following symlinks."""
    # glob suppresses directory-open errors; probe first to retain permission reporting.
    with os.scandir(path):
        pass
    children = []
    for pattern in ("*", ".*"):
        for candidate in glob.iglob(os.path.join(glob.escape(path), pattern)):
            if os.path.basename(candidate) in (".", "..") or is_excluded(candidate):
                continue
            try:
                if stat.S_ISDIR(os.lstat(candidate).st_mode):
                    children.append(candidate)
            except PermissionError:
                children.append(candidate)
            except FileNotFoundError:
                continue
    return children


def coarse_scan(
    root_path, jobid, skipdirs="", scanall=1, recent_runids=(), hostid=0,
    debug_event=None, coarse_glob=False,
):
    """Scan coarse directory batches and persist a compact subtree frontier."""
    root_path = os.path.realpath(os.path.abspath(root_path))
    if not os.path.isdir(root_path):
        raise NotADirectoryError(f"Kein Verzeichnis: {root_path}")

    started = time.monotonic()
    inserted_count = 0
    database_connection.prepare_stage_work(jobid, "coarse", root_path, hostid=hostid)
    while True:
        works = database_connection.claim_work_batch(
            jobid, "coarse", WORK_BATCH_SIZE, hostid=hostid
        )
        if not works:
            break
        is_excluded = _directory_exclusion_matcher(
            ",".join([skipdirs or "", *_live_excluded_absdirs.get(hostid, ())])
        )
        results = []
        for work in works:
            path = os.path.realpath(work["path"])
            parent_path = work["parent_path"]
            if is_excluded(path):
                results.append({
                    "work": work, "store": False, "children": [],
                    "permission_parent": None, "remove_subtree": True,
                })
                continue
            if debug_event is not None and debug_event.is_set():
                message = f"Coarse-Debug: prüfe hostid={hostid} {path}"
                print(message, flush=True)
                _append_localserver_log(message)
            depth = _path_depth(root_path, path)
            if (
                not scanall and depth >= 2 and depth % 2 == 0
                and (bool(work["directory"]
                          and work["directory"]["path"] == path
                          and work["directory"]["finished"] in recent_runids
                          and work["directory"].get("combined_sha256") is not None
                          and work["directory"].get("subdircount") == len(work.get("subdirectories", ()))
                          and all(child.get("combined_sha256") is not None
                                  for child in work.get("subdirectories", ())))
                     if "directory" in work else database_connection.is_recently_finished(
                         work["path_hash"], path, recent_runids, hostid=hostid))
            ):
                print(
                    f"skipped subdirectory {work['path']} "
                    f"(path_hash: {work['path_hash']})",
                    flush=True,
                )
                results.append({
                    "work": work, "store": False, "children": [],
                    "permission_parent": None, "skip_subtree": True,
                })
                continue
            try:
                children = []
                if coarse_glob and hostid > 0:
                    # glob silently returns no entries for vanished paths. Check
                    # first so they are not stored as valid empty directories.
                    if not stat.S_ISDIR(os.stat(path).st_mode):
                        raise NotADirectoryError(path)
                    children = _glob_subdirectories(path, is_excluded)
                else:
                    with os.scandir(path) as entries:
                        for entry in entries:
                            try:
                                if (not is_excluded(entry.path)
                                        and entry.is_dir(follow_symlinks=False)):
                                    children.append(entry.path)
                            except PermissionError:
                                if not is_excluded(entry.path):
                                    children.append(entry.path)
                            except FileNotFoundError:
                                continue
            except PermissionError:
                if parent_path is None:
                    store = True
                    permission_parent = path
                else:
                    store = False
                    permission_parent = parent_path
                print(
                    f"Verzeichnis übersprungen (keine Berechtigung): {path}",
                    flush=True,
                )
                results.append({
                    "work": work,
                    "store": store,
                    "children": [],
                    "permission_parent": permission_parent,
                    "remove_subtree": parent_path is not None,
                })
            except (FileNotFoundError, NotADirectoryError):
                if parent_path is None:
                    # Losing the requested root is a job error, unlike churn
                    # in a descendant cache directory between queue and scan.
                    raise
                print(f"Verzeichnis verschwunden oder kein Verzeichnis mehr; "
                      f"übersprungen: {path}", flush=True)
                results.append({
                    "work": work, "store": False, "children": [],
                    "permission_parent": None, "remove_subtree": True,
                })
            else:
                if debug_event is not None and debug_event.is_set():
                    message = (
                        f"Coarse-Debug: {path}: {len(children)} Unterverzeichnisse"
                    )
                    print(message, flush=True)
                    _append_localserver_log(message)
                results.append({
                    "work": work, "store": True, "children": children,
                    "permission_parent": None,
                })
        inserted = database_connection.complete_coarse_batch(
            jobid, root_path, results, hostid=hostid
        )

        if isinstance(inserted, int):
            inserted_count += inserted
    database_connection.ensure_combined_hash_readiness(hostid=hostid)
    elapsed = max(0.0, time.monotonic() - started)
    speed = (f"{elapsed * 1000 / inserted_count:.2f} s pro 1000 Verzeichnisse"
             if inserted_count else "keine neuen Verzeichnisse; Rate nicht verfügbar")
    print(f"Job {jobid}: coarse abgeschlossen; {inserted_count} Verzeichnisse "
          f"eingefügt in {elapsed:.2f} s; {speed}", flush=True)


def _zero_file_is_readable(path):
    """Check read access without following links or blocking on a replaced FIFO."""
    descriptor = None
    try:
        flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
        descriptor = os.open(path, flags)
        info = os.fstat(descriptor)
        return stat.S_ISREG(info.st_mode) and info.st_size == 0
    except OSError:
        return False
    finally:
        if descriptor is not None:
            os.close(descriptor)


def detailed_scan(root_path, jobid, scanall=1, recent_runids=(), task="scan", hostid=0, skipdirs=""):
    """Scan local statistics and latest eligible regular-file mtime (naive UTC)."""
    root_path = os.path.realpath(os.path.abspath(root_path))
    has_root = database_connection.prepare_stage_work(
        jobid, "detailed", root_path, scanall=scanall,
        recent_runids=set(recent_runids), hostid=hostid,
    )
    if not has_root:
        return

    vanished_count = 0
    vanished_reset_at = time.monotonic() + 7200
    while True:
        works = database_connection.claim_work_batch(
            jobid, "detailed", WORK_BATCH_SIZE, hostid=hostid
        )
        if not works:
            break
        is_excluded = _directory_exclusion_matcher(
            ",".join([skipdirs or "", *_live_excluded_absdirs.get(hostid, ())])
        )
        results = []
        for work in works:
            if is_excluded(work["path"]):
                results.append({"work": work, "excluded": True})
                continue
            localfilesize = localfilecount = emptyfilecount = 0
            subdircount = otherentrycount = 0
            files_lastwrite = None
            time_inode_changed = None
            scan_complete = True
            digest = hashlib.sha256()
            permission_parent = None
            try:
                with os.scandir(work["path"]) as entries:
                    time_inode_changed = datetime.fromtimestamp(
                        os.stat(work["path"], follow_symlinks=False).st_mtime, timezone.utc
                    ).replace(tzinfo=None)
                    for item in sorted(entries, key=lambda value: value.name):
                        if is_excluded(item.path):
                            continue
                        name = item.name.encode("utf-8", "surrogateescape")
                        try:
                            if item.is_dir(follow_symlinks=False):
                                subdircount += 1
                            elif item.is_file(follow_symlinks=False):
                                localfilecount += 1
                                try:
                                    info = item.stat(follow_symlinks=False)
                                    modified = datetime.fromtimestamp(
                                        info.st_mtime, timezone.utc
                                    ).replace(tzinfo=None)
                                    if files_lastwrite is None or modified > files_lastwrite:
                                        files_lastwrite = modified
                                    localfilesize += info.st_size
                                    if info.st_size == 0:
                                        if _zero_file_is_readable(item.path):
                                            emptyfilecount += 1
                                        else:
                                            scan_complete = False
                                    # Hash metadata, not file contents: same name,
                                    # size produce the same token; modification times are ignored.
                                    digest.update(
                                        b"F\0" + name + b"\0"
                                        + str(info.st_size).encode() + b"\n"
                                    )
                                except FileNotFoundError:
                                    scan_complete = False
                                    digest.update(b"MISSING\0" + name + b"\n")
                            else:
                                otherentrycount += 1
                        except PermissionError:
                            scan_complete = False
                            print(
                                "Verzeichniseintrag übersprungen "
                                f"(keine Berechtigung): {item.path}",
                                flush=True,
                            )
            except (FileNotFoundError, NotADirectoryError):
                if work["parent_path"] is None:
                    raise
                # Queued cache directories can disappear or become files
                # between coarse and detailed scans. Keep the checkpoint, but
                # never record this incomplete scan as proof of empty content.
                now = time.monotonic()
                if now >= vanished_reset_at:
                    vanished_count = 0
                    vanished_reset_at += (int((now - vanished_reset_at) // 7200) + 1) * 7200
                vanished_count += 1
                print(f"Job {jobid}: verschwundene Verzeichnisse {vanished_count}/24", flush=True)
                if vanished_count >= 24:
                    return vanished_count
                scan_complete = False
                print(f"Verzeichnis verschwunden oder kein Verzeichnis mehr; "
                      f"übersprungen: {work['path']}", flush=True)
            except PermissionError:
                scan_complete = False
                permission_parent = work["parent_path"]
                print(
                    f"Verzeichnis übersprungen (keine Berechtigung): {work['path']}",
                    flush=True,
                )
            results.append({
                "work": work,
                "stats": {
                    "files_lastwrite": files_lastwrite if scan_complete else None,
                    "time_inode_changed": time_inode_changed if scan_complete else None,
                    "localfilesize": localfilesize,
                    "localfilecount": localfilecount,
                    "emptyfilecount": emptyfilecount if scan_complete else None,
                    "subdircount": subdircount,
                    "otherentrycount": otherentrycount,
                    "filehash": digest.hexdigest() if scan_complete else None,
                },
                "permission_parent": permission_parent,
            })
        database_connection.complete_detailed_batch(
            jobid, root_path, results, hostid=hostid
        )

    database_connection.rebuild_subtree_statistics(root_path, hostid=hostid)

def process_job(job, stage, server):
    jobid = job["jobid"]
    hostid = int(job.get("hostid", 0))
    task = job.get("task", "scan")
    if task in ("binarycheck", "rescan", "check-modifications", "rescan-after-days"):
        stage = (job.get("stage") or "firstscan") if task == "rescan-after-days" else task
        root_path = ("<duplicate-subtrees>" if task == "binarycheck"
                     else "<check-modifications>" if task == "check-modifications"
                     else "<age-rescan>" if task == "rescan-after-days"
                     else "<marked-directories>")
        print(f"Job {jobid}: {task} für Host {hostid}", flush=True)
        server.log_job_started(jobid, stage, root_path)
        try:
            result = server.run_spawn(stage, root_path, jobid, task=task, hostid=hostid)
            if result == "stopped":
                print(f"Job {jobid}: gestoppt", flush=True)
            elif result == "paused":
                set_job_status(jobid, "paused", hostid=hostid)
            elif result == "network_retry":
                server.recover_network_job(jobid, hostid)
            elif result == "error":
                message = getattr(server, "worker_error", None)
                if not isinstance(message, str) or not message:
                    message = f"{task}-Prozess wurde unerwartet beendet."
                set_job_status(jobid, "error", message, hostid=hostid)
                server.log_job_error(jobid, message)
            elif result == 'age_retry_fix' and task == 'rescan-after-days':
                set_job_status(jobid, 'pending', stage='fixunreadable', hostid=hostid)
            elif task == 'rescan-after-days' and stage in ('firstscan','fixunreadable'):
                next_stage = 'fixunreadable' if stage == 'firstscan' else 'workmodified'
                set_job_status(jobid, 'pending', stage=next_stage, hostid=hostid)
            else:
                set_job_status(jobid, "closed", hostid=hostid)
            print(f"Job {jobid}: {result}", flush=True)
        except BaseException as exc:
            if is_network_error(exc) and not _shutdown:
                server.recover_network_job(jobid, hostid)
                return
            try:
                error_status = "paused" if _shutdown else "error"
                set_job_status(jobid, error_status, str(exc), hostid=hostid)
                if error_status == "error":
                    server.log_job_error(jobid, str(exc))
            except Exception:
                traceback.print_exc()
            if not _shutdown:
                traceback.print_exc()
        return

    root_path = os.path.realpath(os.path.abspath(job["startdirname"]))
    print(f"Job {jobid}: {stage} für {root_path}", flush=True)
    server.log_job_started(jobid, stage, root_path)
    try:
        try:
            if not _directory_exclusion_matcher(",".join([job.get("skipdirs") or "",
                    *_live_excluded_absdirs.get(hostid, ())]))(root_path):
                with os.scandir(root_path) as entries:
                    next(entries, None)
        except OSError as exc:
            usererror = "Starting directory doesn't exist or isn't readable."
            detail = f"{type(exc).__name__}: {exc}"
            set_job_status(
                jobid, "error", error=detail, usererror=usererror, hostid=hostid
            )
            server.log_job_error(
                jobid,
                f"{usererror} Path: {root_path}; cause: {detail}",
            )
            print(f"Job {jobid}: {usererror} {root_path}", flush=True)
            return

        recent_runids = getattr(server, "recent_runids", set())
        if not isinstance(recent_runids, set):
            recent_runids = set()
            server.recent_runids = recent_runids
        # claim_job() allocates this job's active runid immediately before the
        # scan. Include it so finished markers written during this same run are
        # eligible for skip checks later in the run or after a stage resumes.
        current_runid = job.get("runid")
        if current_runid is not None:
            recent_runids.add(current_runid)
        result = server.run_spawn(
            stage, root_path, jobid, job.get("skipdirs") or "",
            job.get("scanall", 0), tuple(recent_runids), job.get("task", "scan"),
            hostid,
        )
        if result == "stopped":
            print(f"Job {jobid}: gestoppt", flush=True)
        elif result == "paused":
            runid = set_job_status(jobid, "paused", hostid=hostid)
        elif result == "network_retry":
            server.recover_network_job(jobid, hostid)
        elif result == "error":
            message = getattr(server, "worker_error", None)
            if not isinstance(message, str) or not message:
                message = "Scan-Prozess wurde unerwartet beendet."
            runid = set_job_status(jobid, "error", message, hostid=hostid)
            server.log_job_error(jobid, message)
        elif result == "rescan_coarse":
            if database_connection.reschedule_full_coarse(jobid, hostid):
                server.log_coarse_rescheduled(jobid, hostid, 24)
        elif stage == "coarse":
            runid = set_job_status(jobid, "pending", stage="detailed", hostid=hostid)
        else:
            runid = set_job_status(jobid, "closed", hostid=hostid)
        if (
            result == "ok" and runid and isinstance(recent_runids, set)
            and stage in ("coarse", "detailed")
        ):
            recent_runids.add(runid)
        print(f"Job {jobid}: {result}", flush=True)
    except BaseException as exc:
        if is_network_error(exc) and not _shutdown:
            server.recover_network_job(jobid, hostid)
            return
        try:
            error_status = "paused" if _shutdown else "error"
            runid = set_job_status(jobid, error_status, str(exc), hostid=hostid)
            if error_status == "error":
                server.log_job_error(jobid, str(exc))
        except Exception:
            traceback.print_exc()
        if not _shutdown:
            traceback.print_exc()


class IETF_syslog_formatter(logging.Formatter):
    """Formatiert Syslog-Nachrichten als RFC 5424 (IETF)."""

    def format(self, record):
        timestamp = datetime.fromtimestamp(record.created, timezone.utc)
        timestamp_text = timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        hostname = socket.gethostname().replace(" ", "-") or "-"
        app_name = "flwlk"
        process_id = str(record.process) if record.process is not None else "-"
        message_id = "FILEWALKER"
        message = record.getMessage()
        return (
            f"1 {timestamp_text} {hostname} {app_name} {process_id} "
            f"{message_id} - {message}"
        )


def __getattr__(name):
    """Import application classes only when requested."""
    if name == "DataBaseCLI":
        from DataBaseCLI import DataBaseCLI
        return DataBaseCLI
    if name == "client_operations":
        from client_operations import client_operations
        return client_operations
    if name == "server_operations":
        from server_operations import server_operations
        return server_operations
    if name == "doubles_output":
        from doubles_output import doubles_output
        return doubles_output
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def main():
    argv = sys.argv[1:]
    mode = None
    if not argv:
        script_name = os.path.basename(sys.argv[0]).lower()
        if script_name == "dirserver.py":
            mode = "serverplus"
        elif script_name == "fileserver.py":
            mode = "server"
    elif len(argv) == 1 and argv[0].lower() in ("server", "serverplus"):
        mode = argv[0].lower()

    if mode is not None:
        # The simple server modes do not need the argument parser or CLI code.
        from types import SimpleNamespace
        args = SimpleNamespace(mode=mode)
        try:
            result = getattr(sys.modules[__name__], "server_operations")(args).run()
        except (Error, OSError, RuntimeError, ValueError) as exc:
            print(f"FEHLER: {exc}", file=sys.stderr)
            return 1
        return result if isinstance(result, int) else 0

    from client_operations import ClientArgumentParser
    args = ClientArgumentParser().parse(argv)
    try:
        if args.mode in ("server", "serverplus", "localserver"):
            result = getattr(sys.modules[__name__], "server_operations")(args).run()
        else:
            result = getattr(sys.modules[__name__], "client_operations")(args).run()
    except (Error, OSError, RuntimeError, ValueError) as exc:
        print(f"FEHLER: {exc}", file=sys.stderr)
        return 1
    return result if isinstance(result, int) else 0


if __name__ == "__main__":
    raise SystemExit(main())
