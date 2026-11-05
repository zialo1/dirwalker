# version 0.25.2

"""Client-Befehle und interaktive Verwaltung für version22."""

__version__ = "0.25.2"

import argparse
import csv
import html
import logging
from logging.handlers import SysLogHandler
import multiprocessing
import os
import re
import signal
import socket
import sys
import textwrap
import time
import traceback
from pathlib import Path
from datetime import datetime, timedelta, timezone


def _runtime():
    from version22 import ApplicationContext
    return ApplicationContext.runtime()

class DoublesHelpAction(argparse.Action):
    """Show the duplicate-report workflow without dispatching database commands."""

    def __call__(self, parser, namespace, values, option_string=None):
        program = parser.prog
        text = f"""Hilfe zum Vorgehen: Von --doubles-letcheck zum Bericht --doubles-tree-html

1. Verzeichnisse vollständig scannen lassen.
   Für alle gewünschten Pfade müssen Grob- und Detailscan
   abgeschlossen sein, damit directories aktuelle Hashes und
   Teilbaum-Strukturen enthält.

2. Bytevergleiche einreihen:
   python3 {program} --doubles-letcheck
   Dies plant pro Host Vergleichsjobs in jobs und tmp_doubles_letchecked;
   es führt selbst noch keinen Bytevergleich aus.
   Die ausgegebenen JOBIDs gehören zum neu angelegten Batch: eine davon merken.

3. Einen passenden Server die Vergleichsjobs abarbeiten lassen.

4. In einem zweiten Terminal den Fortschritt beobachten:
   python3 {program} --doubles-progress JOBID
   Für einen möglichst vollständigen Bericht warten, bis
   alle Vergleichsjobs dieses Batches closed sind.
   Nicht lesbare oder nicht bestätigte Bereiche fehlen im Ergebnis.

5. Den gewünschten Baum-Bericht erzeugen:
   python3 {program} --doubles-tree-html JOBID
   Erstellt doubles-tree-JOBID.html im aktuellen Verzeichnis und zeigt den Pfad an.
   Der Bericht wertet den zu JOBID gehörenden Batch über dessen Hosts aus.
   Ohne JOBID wird der neueste Binarycheck-Job verwendet; eine explizite JOBID
   verhindert, dass später eingereihte Jobs versehentlich ausgewählt werden.

Optional: --doubles-listlevel zeigt die Ebenen auch im Terminal an.
--doubles-tree-html berechnet diese Analyse selbst;
--doubles-listlevel ist KEIN Pflichtschritt vor dem HTML-Bericht.
Die Analyse ersetzt die Zeilen dieses Batches in tmp_deepdoubles_result;
sie ist keine zusätzliche byteweise Prüfung und kann auch Teilresultate anzeigen.

Diese Hilfe ändert keine Tabellen und startet keinen Server.
"""
        print("\n".join(
            textwrap.fill(line, width=72, break_on_hyphens=False,
                          subsequent_indent=line[:len(line) - len(line.lstrip())])
            for line in text.splitlines()
        ))
        parser.exit()


class _AlphabeticalArgumentParser(argparse.ArgumentParser):
    """Display optional arguments alphabetically without changing parsing."""

    def format_help(self):
        # Sort only the displayed option section, retaining parser/usage order.
        actions = self._optionals._group_actions
        def key(action):
            name = next((option for option in action.option_strings
                         if option.startswith("--")), action.option_strings[0])
            return name.lstrip("-").casefold()
        self._optionals._group_actions = sorted(actions, key=key)
        try:
            return super().format_help()
        finally:
            self._optionals._group_actions = actions



class ClientArgumentParser:
    """Define, parse and validate every CLI option and subcommand."""

    def parse(self, argv=None):
        argv = list(sys.argv[1:] if argv is None else argv)

        parser = _AlphabeticalArgumentParser(allow_abbrev=False, description="Jobverwaltung und Scan-Server für filestatus")
        # Select the host for supported client commands.
        # Scopes access to jobs, hosts and live_excluded_absdirs; server mode still uses host 0 and
        # localserver has its own HOSTID argument.
        parser.add_argument("--hostid", type=int, default=None,
                            help="Host-ID für lokale Server, Ausschlüsse, --jobs-insert und optional --doubles-progress (sonst Standard: 0)")
        # Enumerate coarse-scan directories using glob.
        # Only localserver accepts this setting, with normal scan writes to directories and
        # tmp<HOSTID>_job_work.
        parser.add_argument("--coarse-glob", action="store_true",
                            help="Im localserver-Grobscan Verzeichnisse per glob ermitteln")
        # Skip the deletion confirmation.
        # The selected command still deletes its documented jobs or directories rows and any dependent
        # work/results.
        parser.add_argument(
            "-Y", dest="yes", action="store_true",
            help="Löschbestätigung bei --jobs-deleteid, --dropdir oder --dirs-droprunid überspringen",
        )
        excluded_group = parser.add_mutually_exclusive_group()
        excluded_group.add_argument("--jobs-remove-unused", action="store_true",
                                    help="Unbenutzte abgeschlossene Jobs auflisten und nach [Yn]-Bestätigung löschen")
        excluded_group.add_argument("--resetlocks", action="store_true",
                                    help="Lokale Lockdateien löschen; setzt gestoppte Server voraus; optional --hostid")
        excluded_group.add_argument('--dropdir', action='store_true',
                                    help='Einen ausgewählten root_path aus directories löschen')
        excluded_group.add_argument('--dropall', action='store_true',
                                    help='Anwendungstabellen zurücksetzen; reguläre Tabellen neu anlegen')
        excluded_group.add_argument('--droptmp', action='store_true',
                                    help='Temporäre Arbeits- und Ergebnistabellen löschen')
        # Select an error job for retry.
        # Updates jobs and resets active rows in tmp<HOSTID>_job_work so the server can resume processing
        # without dropping tables.
        excluded_group.add_argument(
            "--jobs-reset-on-error", action="store_true",
            help="Fehlerjobs anzeigen und einen Job zurücksetzen",
        )
        # Delete one selected job after confirmation.
        # Stop its worker first: DELETE removes its jobs and tmp<HOSTID>_job_work rows and cascades to
        # tmp_doubles_letchecked, without dropping tables.
        excluded_group.add_argument(
            "--jobs-deleteid", type=self.positive_jobid, metavar="JOBID",
            help="Einen Job nach Rückfrage löschen",
        )
        # Resume a paused job or copy a stopped job for restart.
        # Updates or inserts jobs and may copy tmp<HOSTID>_job_work and tmp_doubles_letchecked rows for the
        # server to process.
        excluded_group.add_argument(
            "--jobs-start", type=self.positive_jobid, metavar="JOBID",
            help="Pausierten Job fortsetzen oder gestoppten Job neu einreihen",
        )
        # Mark a queued or running job as stopped.
        # Updates jobs; a running server notices the status at its next check and terminates that worker
        # without deleting tables.
        excluded_group.add_argument(
            "--jobs-stop", type=self.positive_jobid, metavar="JOBID", help="Job stoppen",
        )
        # Display jobs whose stored status is active.
        # Reads jobs without modifying data or verifying whether the recorded workers are still alive.
        excluded_group.add_argument(
            "--jobs-list-active", action="store_true", help="Aktive Jobs anzeigen",
        )
        # Remove every job and its queued work.
        # Stop all workers first: DELETE empties jobs and all tmp<HOSTID>_job_work tables and cascades to
        # tmp_doubles_letchecked, without TRUNCATE or DROP.
        excluded_group.add_argument(
            "--jobs-deleteall", action="store_true",
            help="Alle Einträge aus der Tabelle jobs löschen",
        )
        # Create or reset the directory inventory.
        # Stop scanning first: confirmation allows DROP and recreation of directories, permanently deleting
        # its existing rows.
        excluded_group.add_argument(
            "--init-alldirs", action="store_true",
            help="directories initialisieren (wie --drop-directories)",
        )
        excluded_group.add_argument(
            "--dirs-summary-data", action="store_true",
            help="Directory-Anteile je Host mit Runid, Anzahl und Zeitstempel anzeigen",
        )
        excluded_group.add_argument(
            "--dirs-purge-from-exclusion-lists", action="store_true",
            help="Ausgeschlossene Directory-Datensätze samt Unterverzeichnissen per SQL löschen; optional --hostid",
        )
        excluded_group.add_argument(
            "--dirs-delete-live-excluded", action="store_true",
            help="Directory-Einträge unter live_excluded_absdirs löschen; optional --hostid",
        )
        # Delete directory records matching a run ID.
        # Stop affected workers first: DELETE removes directories rows whose runid or finished matches, while
        # preserving the table.
        excluded_group.add_argument(
            "--dirs-droprunid", type=self.nonnegative_runid, metavar="RUNID",
            help="Einträge zu einer Runid aus directories löschen",
        )
        # Create the database and missing job-related tables.
        # Creates jobs, jobsrunid, excluded_names, live_excluded_absdirs and tmp_doubles_letchecked without
        # truncating or dropping existing tables.
        excluded_group.add_argument(
            "--init-jobs", action="store_true", help="Datenbank und Jobtabelle initialisieren",
        )
        # Select a positive local host and offer query cancellation and error-job reset with yN prompts.
        # KILL CONNECTION rolls back its transaction; reset updates jobs and work states, without dropping tables.
        excluded_group.add_argument(
            "--smartlocalserver", action="store_true",
            help="mit --hostid positive Host-ID wählen oder automatisch ermitteln, DB-Abfragen und Fehlerjobs mit yN prüfen, localserver starten",
        )
        # Register this user and computer with a positive host ID.
        # Writes hosts and creates tmp<HOSTID>_job_work, moving and deleting that host's legacy job_work rows
        # before localserver uses the registration.
        excluded_group.add_argument(
            "--setmyhostid", action="store_true",
            help="Host registrieren; bei gleichem System/Rechner/Home weitere ID nach Rückfrage [yN]",
        )
        # Save global exclusion names for future jobs.
        # Adds excluded_names rows that jobs-insert can copy into jobs.skipdirs for later server scans.
        excluded_group.add_argument(
            "--excluded-dirnames", metavar="CSV",
            help="Kommagetrennte globale Ausschlussnamen speichern",
        )
        # Interactively remove absolute exclusions for a host.
        # DELETE removes selected live_excluded_absdirs rows; restart the server to reload its cached
        # exclusions.
        excluded_group.add_argument(
            "--excluded-absolute-delete", type=int, metavar="HOSTID",
            help="Absolute Ausschlüsse eines Hosts anzeigen und interaktiv löschen",
        )
        # Save an absolute exclusion path for a host.
        # Adds a live_excluded_absdirs row; restart the server to load it into the scan exclusion snapshot.
        excluded_group.add_argument(
            "--excluded-absolute", nargs=2, metavar=("HOSTID", "ABSOLUTER-PFAD"),
            help="Absoluten Live-Ausschlusspfad für die angegebene Host-ID speichern",
        )
        # Remove global names and this host's absolute exclusions.
        # DROP deletes excluded_names itself and DELETE clears this host's live_excluded_absdirs rows; restart
        # the server to refresh cached exclusions.
        excluded_group.add_argument(
            "--erase-excluded", action="store_true",
            help="Globale Namen und Live-Pfade des gewählten Hosts entfernen",
        )
        # Validate the managed database schema.
        # Reads schema metadata and hosts without modifying or deleting tables or starting server work.
        excluded_group.add_argument(
            "--directories-rescan-marked", action="store_true",
            help="Job zum groben Prüfen markierter Teilbäume für Server/localserver einreihen",
        )
        excluded_group.add_argument("--rescan-after-days", type=int, metavar="TAGE",
                                    help="Schnellprüfjobs für Inventar-Jobs älter als TAGE einreihen; optional --hostid")
        excluded_group.add_argument(
            "--check-modifications", action="store_true",
            help="Prüfjobs für --hostid oder ohne --hostid für alle registrierten Hosts einreihen",
        )
        # Explain the steps from scheduling comparisons to the final HTML tree report.
        # Prints instructions only, without changing tables or starting server work.
        excluded_group.add_argument(
            "--doubles-help", action=DoublesHelpAction, nargs=0,
            help="Ablauf von --doubles-letcheck bis --doubles-tree-html erklären und beenden",
        )
        # Display candidate duplicate directory trees.
        # Reads directories hashes and structure without changing tables or asking the server to compare
        # bytes.
        excluded_group.add_argument(
            "--doubles-list", action="store_true",
            help="Vollständig doppelte Unterbäume anzeigen",
        )
        # Resolve pasted directory IDs to paths.
        # Reads directories without modifying tables or starting server work.
        excluded_group.add_argument(
            "--doubles-dirids-to-dirnames", action="store_true",
            help="Eingefügte dirid-Liste in Verzeichnisnamen umwandeln",
        )
        # Queue bytewise directory comparisons.
        # Reads directories and inserts jobs and tmp_doubles_letchecked rows for serverplus or the matching
        # localserver to process, without deleting prior results.
        excluded_group.add_argument(
            "--doubles-letcheck", dest="doubles_check", action="store_true",
            help="Byteweise Vergleichsjobs für doppelte Unterbäume einreihen",
        )
        # Calculate nesting levels from available verified comparisons.
        # Reads jobs, directories and tmp_doubles_letchecked and replaces this batch's tmp_deepdoubles_result
        # rows using DELETE/INSERT, while existing server comparisons may continue.
        excluded_group.add_argument(
            "--doubles-listlevel", "--doubles-listlevels", dest="doubles_listlevel", action="store_true",
            help="Duplikate als Verzeichnisbäume mit Tiefe anzeigen",
        )
        # Display stored binarycheck results.
        # Reads tmp_doubles_letchecked without modifying data or starting server work.
        excluded_group.add_argument(
            "--doubles-list-binary", action="store_true",
            help="Status der Tabelle tmp_doubles_letchecked anzeigen",
        )
        # Monitor binarycheck progress in the console.
        # Reads jobs, directories and tmp_doubles_letchecked while a binarycheck-enabled server produces
        # further results.
        excluded_group.add_argument(
            "--doubles-progress", dest="doubles_progress_jobid",
            nargs="?", type=int, const=0, default=None, metavar="JOBID",
            help="Binarycheck-Fortschritt alle 20 Sekunden anzeigen; optional mit --hostid filtern, ohne JOBID den neuesten passenden Job nutzen",
        )
        # Write a live binarycheck HTML report.
        # Reads jobs, directories and tmp_doubles_letchecked and writes an HTML file, while a binarycheck-
        # enabled server supplies new results.
        excluded_group.add_argument(
            "--doubles-progress-html", dest="doubles_progress_html_jobid",
            nargs="?", type=int, const=0, default=None, metavar="JOBID",
            help="Fortschritt als HTML-Datei schreiben; ohne JOBID den neuesten Job nutzen",
        )
        # Write an HTML report of saved nested duplicate groups.
        # Reads jobs, directories, tmp_doubles_letchecked and tmp_deepdoubles_result without changing database
        # tables or scheduling server work.
        excluded_group.add_argument(
            "--doubles-tree-html", dest="doubles_tree_html_jobid",
            nargs="?", type=int, const=0, default=None, metavar="JOBID",
            help="Alle dirids je verschachtelter Hash-Gruppe (level > 0) als HTML anzeigen",
        )
        excluded_group.add_argument(
            "--doubles-tree", dest="doubles_tree_jobid",
            nargs="?", type=int, const=0, default=None, metavar="JOBID",
            help="HTML-Snapshot vorhandener Ergebnisse; ohne JOBID alle Jobs und Hosts, mit JOBID dessen Batch",
        )
        # Create or reset the directory inventory.
        # Stop scanning first: confirming the reset DROPs and recreates directories, deleting its old rows.
        excluded_group.add_argument(
            "--drop-directories", action="store_true",
            help="directories nach Rückfrage löschen und neu anlegen",
        )
        # Queue directory IDs for priority comparison.
        # Reads directories and inserts tmp_highpriority rows for the given host, without deleting data.
        excluded_group.add_argument(
            "--insert-priority", nargs="+", type=int, metavar="ID",
            help="HOSTID DIRID [DIRID ...]: Verzeichnisse für Binarycheck priorisieren",
        )
        # Queue a directory scan for the selected host.
        # Inserts jobs; the matching server later writes directories, jobsrunid and its host work queue.
        excluded_group.add_argument(
            "--jobs-insert", nargs="+", metavar="PFAD",
            help="PFAD [AUSSCHLUSS]: Scan einreihen; optional kommaseparierte Ausschlussnamen",
        )
        # Queue a directory scan for the explicitly supplied host ID.
        # Inserts jobs for that host's server to process into directories, jobsrunid and tmp<HOSTID>_job_work.
        excluded_group.add_argument(
            "--jobs-insert-hostid", nargs="+", metavar="PFAD",
            help="HOSTID PFAD [AUSSCHLUSS]: Scan für die angegebene Host-ID einreihen",
        )
        # Queue a scan without skipping previously finished subtrees.
        # Inserts a host-0 jobs row with scanall=1 for server/serverplus to process without deleting tables.
        excluded_group.add_argument(
            "--jobs-insert-scanall", nargs="+", metavar="PFAD",
            help="[HOSTID] PFAD [AUSSCHLUSS]: Scan ohne Überspringen fertiger Teilbäume einreihen",
        )
        subparsers = parser.add_subparsers(dest="mode")

        def add_command(name, **kwargs):
            kwargs.setdefault("allow_abbrev", False)
            return subparsers.add_parser(name, **kwargs)

        add_command("check", help="Schema, Host-Arbeitstabellen und Root-Job-Konflikte prüfen")
        # Process ordinary scan jobs for host 0.
        # Writes jobs, jobsrunid, directories and tmp0_job_work; existing active rows are not automatically
        # recovered and binarycheck jobs are skipped.
        add_command(
            "server", help="Scan-Server starten; Binarycheck-Jobs bleiben liegen"
        )
        # Process scan and binarycheck jobs for host 0.
        # Writes jobs, jobsrunid, directories, tmp0_job_work and tmp_doubles_letchecked, reading
        # tmp_highpriority when delegating priority comparisons.
        add_command(
            "serverplus", help="Alle Jobs einschließlich Binarycheck abarbeiten"
        )
        # Process jobs owned by this registered local host.
        # Writes jobs, jobsrunid, directories, tmp<HOSTID>_job_work and tmp_doubles_letchecked, reading hosts
        # and the startup live_excluded_absdirs snapshot.
        localserver_parser = add_command(
            "localserver", help="Jobs einer registrierten lokalen Host-ID abarbeiten"
        )
        # Select the registered local host to serve.
        # The server validates hosts and uses this ID to scope jobs, directories and tmp<HOSTID>_job_work.
        localserver_parser.add_argument("--hostid", type=int, default=argparse.SUPPRESS,
                                        help="Registrierte positive Host-ID dieses Rechners")
        # Enumerate coarse-scan directories using glob.
        # Only localserver accepts this setting, with normal scan writes to directories and
        # tmp<HOSTID>_job_work.
        localserver_parser.add_argument(
            "--coarse-glob", action="store_true", default=argparse.SUPPRESS,
            help="Im Grobscan Verzeichnisse per glob ermitteln",
        )
        # Initialize job tables, check MariaDB timeout and set up the directory inventory.
        # Stop workers first: recreating directories also DELETEs jobs, jobsrunid, queues and cached results;
        # declining preserves the existing inventory and its job history.
        add_command(
            "init", help="MariaDB-Timeout prüfen; directories einrichten und bei Neuaufbau Jobs/Runidhistorie leeren; Schema prüfen"
        )
        # Remove scan queues and saved duplicate-analysis results.
        # Stop workers first: DROP deletes all tmp<HOSTID>_job_work, tmp_doubles_letchecked and
        # tmp_deepdoubles_result tables, but keeps tmp_highpriority.
        # Ensure host registration exists and register this machine as server host zero.
        # Creates missing hosts/tmp0_job_work and upserts host zero; no tables or job data are deleted.
        add_command("serverinit", help="hosts und Server-Arbeitsliste bei Bedarf anlegen; aktuellen Rechner als Host 0 registrieren")
        # Display a table or a formatted inventory view.
        # Reads the selected table or jobs, directories, hosts and exclusion data without modifying tables or
        # starting server work.
        show_parser = add_command("show", help="Tabellen oder eine gespeicherte Anzeige auflisten")
        # Select the host for the formatted exclusion view.
        # Reads this host's live_excluded_absdirs entries together with global excluded_names without changing
        # data.
        show_parser.add_argument("--hostid", type=int, default=argparse.SUPPRESS,
                                 help="Host für show excluded")
        # Select the table or formatted view to display.
        # Only reads the requested database data; it does not delete tables or trigger server processing.
        show_parser.add_argument(
            "show_name", nargs="?", metavar="NAME",
            help="hosts, jobs, excluded, listrunid, dirs-listrunid oder Tabellenname; ohne NAME Tabellen auflisten",
        )

        # Delete the inventory for one selected root path.
        # Stop affected scans first: DELETE removes matching directories rows after confirmation while
        # preserving the table and filesystem contents.
        # Skip the deletion confirmation.
        # The selected command still deletes its documented jobs or directories rows and any dependent
        # work/results.
        # Reset application tables with optional core-table deletion.
        # Stop all workers first: DROP removes numeric host queues, tmp_doubles_letchecked, excluded_names and
        # live_excluded_absdirs, plus directories/jobs/hosts/jobsrunid if confirmed, then recreates regular
        # tables while retaining job_work, tmp_highpriority and tmp_deepdoubles_result.

        args = parser.parse_args(argv)
        args.hostid_explicit = args.hostid is not None
        if args.hostid is None:
            args.hostid = 0
        if not 0 <= args.hostid <= 32767:
            parser.error("--hostid muss zwischen 0 und 32767 liegen.")
        if args.mode == "localserver":
            if not args.hostid_explicit or not 1 <= args.hostid <= 32767:
                parser.error("localserver benötigt --hostid HOSTID (1 bis 32767).")
            args.local_hostid = args.hostid
        if args.smartlocalserver:
            if args.mode is not None:
                parser.error("--smartlocalserver kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "localserver"
            if args.hostid_explicit and args.hostid == 0:
                parser.error("--smartlocalserver benötigt eine positive --hostid (1 bis 32767).")
            args.local_hostid = args.hostid if args.hostid_explicit else None
            args.smart_start = True
        # All operational options share one exclusion group and dispatch through
        # the same internal modes; no --command is treated as a positional name.
        selected = [
            ("--drop-directories", "drop-directories", args.drop_directories),
            ("--insert-priority", "insert-priority", args.insert_priority),
            ("--jobs-insert", "jobs-insert", args.jobs_insert),
            ("--jobs-insert-hostid", "jobs-insert-hostid", args.jobs_insert_hostid),
            ("--jobs-insert-scanall", "jobs-insert-scanall", args.jobs_insert_scanall),
        ]
        for option, mode, values in selected:
            if values is None or values is False:
                continue
            if args.mode is not None:
                parser.error(f"{option} kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = mode
            if mode == "insert-priority":
                if len(values) < 2:
                    parser.error("--insert-priority benötigt HOSTID und mindestens eine DIRID.")
                args.priority_hostid, args.priority_dirids = values[0], values[1:]
                if any(value <= 0 for value in args.priority_dirids):
                    parser.error("DIRIDs müssen positive ganze Zahlen sein.")
            elif (mode == "jobs-insert-hostid" or (
                    mode == "jobs-insert-scanall" and len(values) >= 2
                    and values[0].lstrip('+-').isdigit())):
                if len(values) not in (2, 3):
                    parser.error("--jobs-insert-hostid benötigt HOSTID PFAD und optional AUSSCHLUSS.")
                try:
                    explicit_hostid = int(values[0])
                except ValueError:
                    parser.error("HOSTID muss eine ganze Zahl sein.")
                if not 0 <= explicit_hostid <= 32767:
                    parser.error("HOSTID muss zwischen 0 und 32767 liegen.")
                if args.hostid not in (0, explicit_hostid):
                    parser.error("--hostid und --jobs-insert-hostid nennen unterschiedliche Hosts.")
                args.hostid = explicit_hostid
                args.hostid_explicit = True
                args.path = values[1]
                args.skipdirs = values[2] if len(values) == 3 else ""
            elif mode.startswith("jobs-insert"):
                if len(values) > 2:
                    parser.error(f"{option} benötigt PFAD und optional eine kommaseparierte Ausschlussliste.")
                args.path = values[0]
                args.skipdirs = values[1] if len(values) == 2 else ""
        selected_drop = next((mode for mode in ('dropdir', 'dropall', 'droptmp')
                              if getattr(args, mode)), None)
        if args.jobs_remove_unused:
            if args.mode is not None:
                parser.error('--jobs-remove-unused benötigt einen eigenen Modus.')
            args.mode = 'jobs-remove-unused'
        elif args.resetlocks:
            if args.mode is not None:
                parser.error('--resetlocks benötigt einen eigenen Modus.')
            args.mode = 'resetlocks'
        elif selected_drop is not None:
            if args.mode is not None:
                parser.error(f"--{selected_drop} kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = selected_drop
        elif args.init_alldirs:
            if args.mode is not None:
                parser.error("--init-alldirs kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "drop-directories"
        elif args.dirs_summary_data:
            if args.mode is not None:
                parser.error("--dirs-summary-data kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "dirs-summary-data"
        elif args.dirs_purge_from_exclusion_lists:
            if args.mode is not None:
                parser.error("--dirs-purge-from-exclusion-lists kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "dirs-purge-from-exclusion-lists"
        elif args.dirs_delete_live_excluded:
            if args.mode is not None:
                parser.error("--dirs-delete-live-excluded kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "dirs-delete-live-excluded"
        elif args.dirs_droprunid is not None:
            if args.mode is not None:
                parser.error("--dirs-droprunid kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "dirs-droprunid"
            args.runid = args.dirs_droprunid
        elif args.jobs_reset_on_error:
            if args.mode is not None:
                parser.error("--jobs-reset-on-error kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "jobs-reset-on-error"
        elif args.jobs_deleteid is not None:
            if args.mode is not None:
                parser.error("--jobs-deleteid kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "jobs-deleteid"
            args.jobid = args.jobs_deleteid
        elif args.jobs_start is not None:
            if args.mode is not None:
                parser.error("--jobs-start kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "jobs-start"
            args.jobid = args.jobs_start
        elif args.jobs_stop is not None:
            if args.mode is not None:
                parser.error("--jobs-stop kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "jobs-stop"
            args.jobid = args.jobs_stop
        elif args.jobs_list_active:
            if args.mode is not None:
                parser.error("--jobs-list-active kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "jobs-list-active"
        elif args.jobs_deleteall:
            if args.mode is not None:
                parser.error("--jobs-deleteall kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "jobs-deleteall"
        elif args.init_jobs:
            if args.mode is not None:
                parser.error("--init-jobs kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "init-jobs"
        elif args.setmyhostid:
            if args.mode is not None:
                parser.error("--setmyhostid kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "setmyhostid"
        elif args.excluded_dirnames is not None:
            if args.mode is not None:
                parser.error("--excluded-dirnames kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "excluded-dirnames"
        elif args.excluded_absolute_delete is not None:
            if args.mode is not None:
                parser.error("--excluded-absolute-delete kann nicht mit einem anderen Modus kombiniert werden.")
            if not 0 <= args.excluded_absolute_delete <= 32767:
                parser.error("HOSTID muss zwischen 0 und 32767 liegen.")
            args.mode = "excluded-absolute-delete"
        elif args.excluded_absolute is not None:
            if args.mode is not None:
                parser.error("--excluded-absolute kann nicht mit einem anderen Modus kombiniert werden.")
            hostid, path = args.excluded_absolute
            try:
                hostid = int(hostid)
            except ValueError:
                parser.error("HOSTID muss eine ganze Zahl sein.")
            if not 0 <= hostid <= 32767:
                parser.error("HOSTID muss zwischen 0 und 32767 liegen.")
            if not os.path.isabs(path):
                parser.error("--excluded-absolute benötigt einen absoluten Pfad.")
            if "," in path:
                parser.error("Absolute Ausschlusspfade dürfen kein Komma enthalten.")
            args.excluded_absolute = (hostid, path)
            args.mode = "excluded-absolute"
        elif args.erase_excluded:
            if args.mode is not None:
                parser.error("--erase-excluded kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "erase-excluded"
        elif args.directories_rescan_marked:
            if args.mode is not None:
                parser.error("--directories-rescan-marked kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "directories-rescan-marked"
        elif args.rescan_after_days is not None:
            if args.mode is not None or args.rescan_after_days < 0:
                parser.error('--rescan-after-days benötigt nichtnegative Tage und einen eigenen Modus.')
            args.mode = 'rescan-after-days'
        elif args.check_modifications:
            if args.mode is not None:
                parser.error("--check-modifications kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "check-modifications"
        elif args.doubles_list:
            if args.mode is not None:
                parser.error("--doubles-list kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "doubles-list"
        elif args.doubles_dirids_to_dirnames:
            if args.mode is not None:
                parser.error("--doubles-dirids-to-dirnames kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "doubles-dirids-to-dirnames"
        elif args.doubles_listlevel:
            if args.mode is not None:
                parser.error("--doubles-listlevel kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "doubles-listlevel"
        elif args.doubles_check:
            if args.mode is not None:
                parser.error("--doubles-letcheck kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "doubles-check"
        elif args.doubles_list_binary:
            if args.mode is not None:
                parser.error("--doubles-list-binary kann nicht mit einem anderen Modus kombiniert werden.")
            args.mode = "doubles-list-binary"
        elif args.doubles_tree_jobid is not None:
            if args.mode is not None:
                parser.error("--doubles-tree kann nicht mit einem anderen Modus kombiniert werden.")
            if args.doubles_tree_jobid < 0:
                parser.error("JOBID muss positiv sein.")
            args.mode = "doubles-tree"
            args.jobid = args.doubles_tree_jobid or None
        elif args.doubles_tree_html_jobid is not None:
            if args.mode is not None:
                parser.error("--doubles-tree-html kann nicht mit einem anderen Modus kombiniert werden.")
            if args.doubles_tree_html_jobid < 0:
                parser.error("JOBID muss positiv sein.")
            args.mode = "doubles-tree-html"
            args.jobid = args.doubles_tree_html_jobid or None
        elif args.doubles_progress_html_jobid is not None:
            if args.mode is not None:
                parser.error("--doubles-progress-html kann nicht mit einem anderen Modus kombiniert werden.")
            if args.doubles_progress_html_jobid < 0:
                parser.error("JOBID muss positiv sein.")
            args.mode = "doubles-progress-html"
            args.jobid = args.doubles_progress_html_jobid or None
        elif args.doubles_progress_jobid is not None:
            if args.mode is not None:
                parser.error("--doubles-progress kann nicht mit einem anderen Modus kombiniert werden.")
            if args.doubles_progress_jobid < 0:
                parser.error("JOBID muss positiv sein.")
            args.mode = "doubles-progress"
            args.jobid = args.doubles_progress_jobid or None
        elif args.mode is None:
            parser.error("Bitte einen Modus oder eine excluded_names-Option angeben.")
        if args.mode == "insert-priority":
            if not 0 <= args.priority_hostid <= 32767:
                parser.error("HOSTID muss zwischen 0 und 32767 liegen.")
            if any(value > 18446744073709551615 for value in args.priority_dirids):
                parser.error("DIRID überschreitet BIGINT UNSIGNED.")
        if args.yes and args.mode not in ("jobs-deleteid", "dropdir", "dirs-droprunid", "dirs-delete-live-excluded"):
            parser.error("-Y kann nur mit --jobs-deleteid, --dropdir oder --dirs-droprunid verwendet werden.")
        if args.coarse_glob and args.mode != "localserver":
            parser.error("--coarse-glob kann nur mit localserver verwendet werden.")
        return args

    @staticmethod
    def positive_jobid(value):
        """Parse a client job ID and reject zero or negative values."""
        try:
            jobid = int(value)
        except (TypeError, ValueError):
            raise argparse.ArgumentTypeError("JOBID muss eine positive ganze Zahl sein.")
        if jobid < 1:
            raise argparse.ArgumentTypeError("JOBID muss mindestens 1 sein.")
        return jobid


    @staticmethod
    def nonnegative_runid(value):
        """Parse a runid, including zero, which is the schema default."""
        try:
            runid = int(value)
        except (TypeError, ValueError):
            raise argparse.ArgumentTypeError("RUNID muss eine nichtnegative ganze Zahl sein.")
        if runid < 0:
            raise argparse.ArgumentTypeError("RUNID darf nicht negativ sein.")
        return runid



    @staticmethod
    def parse_excluded_dirnames(value):
        """Parse a comma-separated list of directory entry names."""
        try:
            row = next(csv.reader([value or ""], skipinitialspace=True))
        except csv.Error as exc:
            raise ValueError(f"Ungültige CSV-Liste: {exc}")
        names = list(dict.fromkeys(part.strip() for part in row if part.strip()))
        if not names:
            raise ValueError("Bitte mindestens einen Ausschlussnamen angeben.")
        if any("," in name for name in names):
            raise ValueError("Ausschlussnamen dürfen kein Komma enthalten.")
        return names



class client_operations:
    """Dispatch administrative commands; load duplicate rendering on demand."""

    def __init__(self, args):
        self.args = args
        self.database = _runtime().ApplicationContext.cli_database()


    def run(self):
        args = self.args
        if args.mode == 'jobs-remove-unused':
            return self.remove_unused_jobs()
        elif args.mode == 'resetlocks':
            hostid = args.hostid if args.hostid_explicit else None
            cleared = self.database.reset_server_locks(hostid=hostid)
            print(f'{len(cleared)} lokale Lockdateien gelöscht (Server als gestoppt vorausgesetzt).'
                  + (' Host-IDs: ' + ', '.join(map(str,cleared)) if cleared else ''))
            return 0
        elif args.mode == "show":
            return self.show(getattr(args, "show_name", None))
        elif args.mode == "setmyhostid":
            hostid = self.database.set_my_hostid()
            if hostid is None:
                print("Host-Registrierung abgebrochen; bestehende Einträge bleiben unverändert.")
                return
            print(f"Eigener Host registriert: hostid={hostid}")
        elif args.mode == "insert-priority":
            count = self.database.insert_priority(args.priority_hostid, args.priority_dirids)
            print(f"Priorisierte Verzeichnisse: {count}")
        elif args.mode == "serverinit":
            self.database.serverinit()
            print("hosts ist eingerichtet; aktueller Rechner als Host 0 registriert. Bestehende Jobs, Arbeitslisten und Ergebnisse bleiben erhalten.")
        elif args.mode == "init":
            return self.database.initialize()
        elif args.mode == "init-jobs":
            self.database.init_database()
        elif args.mode == "drop-directories":
            self.database.init_alldirs()
        elif args.mode == "jobs-deleteall":
            self.database.delete_all_jobs()
        elif args.mode == "droptmp":
            tables = self.database.droptmp()
            print("Gelöschte tmp-Tabellen: " + (", ".join(tables) or "keine"))
        elif args.mode == "dropall":
            self.database.drop_all_tables()
        elif args.mode == "directories-rescan-marked":
            jobid = self.database.enqueue_directories_rescan_marked(hostid=args.hostid)
            print(f"Rescan markierter Verzeichnisse als Job {jobid} eingereiht.")
            return 0
        elif args.mode == 'rescan-after-days':
            selected = args.hostid if args.hostid_explicit else None
            created = self.database.enqueue_rescan_after_days(args.rescan_after_days, selected)
            for hostid, source, jobid in created:
                print(f'Host {hostid}, Quelljob {source}: Schnellprüfung als Job {jobid} eingereiht.')
            print(f'{len(created)} Schnellprüfjobs eingereiht.')
            return 0
        elif args.mode == "check-modifications":
            selected = args.hostid if getattr(args, "hostid_explicit", False) else None
            created = self.database.enqueue_check_modifications(hostid=selected)
            for hostid, jobid in created:
                print(f"Änderungsprüfung für Host {hostid} als Job {jobid} eingereiht.")
            if not created:
                print("Keine registrierten Hosts; keine Prüfjobs eingereiht.")
            return 0
        elif args.mode == "check":
            return 0 if self.database.check_schema() else 1
        elif args.mode == "jobs-deleteid":
            self.delete_job(args.jobid, getattr(args, "yes", False))
        elif args.mode == "jobs-reset-on-error":
            self.reset_after_error()
        elif args.mode == "dropdir":
            self.drop_directory_root(getattr(args, "yes", False))
        elif args.mode == "dirs-summary-data":
            self.show_directory_summary_data()
        elif args.mode == "dirs-purge-from-exclusion-lists":
            hostid = args.hostid if args.hostid_explicit else None
            deleted = self.database.purge_directories_from_exclusion_lists(hostid=hostid)
            print(f"{deleted} ausgeschlossene Directory-Datensätze gelöscht.")
            return 0
        elif args.mode == "dirs-delete-live-excluded":
            self.delete_live_excluded_directories()
        elif args.mode == "dirs-droprunid":
            self.drop_directories_for_runid(args.runid, getattr(args, "yes", False))
        elif args.mode == "doubles-list":
            self.list_doubles()
        elif args.mode == "doubles-dirids-to-dirnames":
            self.transform_double_dirnames()
        elif args.mode == "doubles-listlevel":
            try:
                rows = self.database.list_deep_double_levels()
            except RuntimeError as exc:
                print(f"--doubles-listlevel abgebrochen: {exc}")
                return
            print(self.database.deep_double_analysis_summary, file=sys.stderr, flush=True)
            print("LEVEL | Hostid:DIRID von A <|>Hostid:DIRID von B| Pfad A | Pfad B")
            for left_host, left, right_host, right, depth, left_path, right_path in sorted(
                rows, key=lambda row: row[4], reverse=True
            ):
                print(f"{depth} | {left_host}:{left} <->{right_host}:{right}| "
                      f"{left_path} | {right_path}")
            if not rows:
                print("Keine weiteren Deep-Double-Kandidaten gefunden.")
        elif args.mode == "doubles-check":
            print(
                "Suche auch zwischen Hosts nach doppelten Unterbäumen mit gleichem Hash und "
                "gleicher Struktur und trage Vergleichsaufträge in "
                "tmp_doubles_letchecked ein. "
                "Bei verschiedenen Hosts führt localserver den Vergleich; "
                "der andere Host liefert im Listening-Modus Inhaltsprüfsummen.",
                flush=True,
            )
            created = self.database.create_binarycheck_job(confirm_repeat=self._confirm_binarycheck_repeat)
            if not created or created == (None, 0):
                print("Keine neuen Binarycheck-Vergleiche eingereiht.")
            else:
                jobs = [created] if isinstance(created, tuple) else created
                for jobid, count in jobs:
                    print(
                        f"Binarycheck-Job {jobid} mit {count} Verzeichnisvergleichen "
                        "wurde eingereiht."
                    )
        elif args.mode == "doubles-list-binary":
            self.list_double_jobs()
        elif args.mode == "doubles-progress":
            if getattr(args, 'hostid_explicit', False):
                self.show_doubles_progress(getattr(args, "jobid", None), hostid=args.hostid)
            else:
                self.show_doubles_progress(getattr(args, "jobid", None))
        elif args.mode == "doubles-tree":
            self._duplicate_output().show_doubles_tree(getattr(args, 'jobid', None))
        elif args.mode == "doubles-tree-html":
            self.show_doubles_tree_html(getattr(args, "jobid", None))
        elif args.mode == "doubles-progress-html":
            self.show_doubles_progress_html(getattr(args, "jobid", None))
        elif args.mode == "excluded-dirnames":
            names = ClientArgumentParser.parse_excluded_dirnames(args.excluded_dirnames)
            self.database.add_excluded_names(names)
            print(f"Ausschlussnamen gespeichert: {len(names)}")
        elif args.mode == "excluded-absolute-delete":
            self.delete_absolute_exclusions(args.excluded_absolute_delete)
        elif args.mode == "excluded-absolute":
            hostid, path = args.excluded_absolute
            self.database.add_live_excluded_absdirs([path], int(hostid))
            print(f"Live-Ausschlusspfad gespeichert (hostid={hostid}): {path}")
        elif args.mode == "erase-excluded":
            self.database.erase_excluded_names()
            self.database.erase_live_excluded_absdirs(args.hostid)
            print(f"excluded_names gelöscht; Live-Pfade für hostid={args.hostid} entfernt.")
        elif args.mode == "jobs-insert":
            skipdirs = self.include_excluded_names(args.skipdirs)
            if getattr(args, "hostid", 0):
                self.database.insert_job(args.path, skipdirs, hostid=args.hostid)
            else:
                self.database.insert_job(args.path, skipdirs)
        elif args.mode == "jobs-insert-hostid":
            skipdirs = self.include_excluded_names(args.skipdirs)
            self.database.insert_job(args.path, skipdirs, hostid=args.hostid)
        elif args.mode == "jobs-insert-scanall":
            skipdirs = self.include_excluded_names(args.skipdirs)
            if getattr(args, 'hostid', 0):
                self.database.insert_job(args.path, skipdirs, scanall=1, hostid=args.hostid)
            else:
                self.database.insert_job(args.path, skipdirs, scanall=1)
        elif args.mode == "jobs-list-active":
            self.database.list_jobs(active_only=True)
        elif args.mode == "jobs-stop":
            self.database.change_job_status(args.jobid, "stop")
        elif args.mode == "jobs-start":
            self.database.change_job_status(args.jobid, "start")


    def show(self, name=None):
        """List available tables, use a formatted display, or print a raw preview."""
        if name is None:
            for table in self.database.list_tables():
                print(table)
            return
        if name == "hosts":
            return self._show_hosts()
        if name in ("jobs", "jobs-list"):
            return self.database.list_jobs(active_only=False)
        if name == "excluded":
            return self.show_excluded_names(getattr(self.args, "hostid", 0))
        if name == "listrunid":
            return self._show_recent_runids()
        if name == "dirs-listrunid":
            return self.list_directory_runids()
        rows, total = self.database.table_preview(name)
        for row in rows:
            # Keep each record on one line, including TEXT fields with newlines.
            print(" | ".join(
                "NULL" if value is None else str(value).replace("\\", "\\\\")
                .replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t")
                for value in row
            ))
        print(f"Datensätze insgesamt: {total}")


    def _show_hosts(self):
        rows = self.database.list_hosts()
        print("hostid | user | homepath | homedirid | system | machine | timecreated | tabellenname")
        for row in rows:
            print(" | ".join("NULL" if value is None else str(value) for value in row))
        if not rows:
            print("Keine Hosts registriert.")


    def _show_recent_runids(self):
        jobs = self.database.list_recent_jobs(days=15)
        if not jobs:
            print("Keine Runids von Jobs aus den letzten 15 Tagen vorhanden.")
        else:
            columns = (
                "jobid", "userhost", "startdirname", "startdirid", "task",
                "status", "stage", "skipdirs", "scanall", "runid", "errmsg",
                "usererror",
            )
            widths = {
                "jobid": 6, "userhost": 24, "startdirname": 32,
                "startdirid": 10, "task": 8, "status": 10, "stage": 10,
                "skipdirs": 24, "scanall": 7, "runid": 9,
                "errmsg": 36, "usererror": 48,
            }

            def format_value(value, width):
                value = "-" if value is None else str(value)
                if len(value) > width:
                    value = value[:width - 3] + "..."
                return value.ljust(width)

            header = " | ".join(
                format_value(column, widths[column]) for column in columns
            )
            print("Jobs der letzten 15 Tage:")
            print(header)
            print("-" * len(header))
            for job in jobs:
                print(" | ".join(
                    format_value(job.get(column), widths[column])
                    for column in columns
                ))


    def delete_job(self, jobid, skip_confirmation=False):
        """Job anzeigen, Rückfrage stellen und den Datensatz gezielt löschen."""
        job = self.database.get_job(jobid)
        if job is None:
            print(f"Job {jobid} wurde nicht gefunden.")
            return

        print(
            f"Job {job['jobid']}: {job['task']} | {job['status']} | "
            f"{job['startdirname']} | {job['userhost']} | "
            f"erstellt {job['timecreated']}"
        )
        if not skip_confirmation:
            while True:
                answer = input(f"Job {jobid} wirklich löschen? [y/n] ").strip().lower()
                if answer == "y":
                    break
                if answer == "n":
                    print("Löschen verworfen.")
                    return
                print("Bitte y oder n eingeben.")

        if self.database.delete_job(jobid):
            print(f"Job {jobid} gelöscht.")
        else:
            print(f"Job {jobid} wurde zwischenzeitlich entfernt.")


    def reset_after_error(self):
        """Fehlerjobs anzeigen, eine ID abfragen und den Job auf insert setzen."""
        jobs = self.database.list_jobs(status="error", reset_error_display=True)
        if not jobs:
            return

        try:
            jobid = int(input("Job-ID zum Zurücksetzen auf insert: ").strip())
        except ValueError:
            print("Ungültige Job-ID; es wurde nichts geändert.")
            return
        if jobid < 1:
            print("JOBID muss mindestens 1 sein; es wurde nichts geändert.")
            return

        if not any(job["jobid"] == jobid for job in jobs):
            print(f"Job {jobid} ist nicht in der Liste der Fehlerjobs.")
            return

        if self.database.reset_error_job(jobid):
            print(f"Job {jobid} wurde auf insert zurückgesetzt.")
        else:
            print(f"Job {jobid} ist nicht mehr im Status error; nichts geändert.")


    def drop_directory_root(self, skip_confirmation=False):
        """Root-Pfad nummeriert auswählen und dessen Directory-Zeilen löschen."""
        roots = self.database.list_directory_roots()
        if not roots:
            print("Keine root_path-Werte in directories vorhanden.")
            return

        print("Root-Pfade in directories:")
        for number, (root_path, count) in enumerate(roots, start=1):
            print(f"{number}) {root_path} ({count} Einträge)")

        try:
            selection = int(input("Nummer des zu löschenden root_path: ").strip())
        except ValueError:
            print("Ungültige Auswahl; es wurde nichts geändert.")
            return
        if selection < 1 or selection > len(roots):
            print("Auswahl außerhalb der Liste; es wurde nichts geändert.")
            return

        root_path, count = roots[selection - 1]
        print(f"Für {root_path} werden {count} Einträge in directories gelöscht.")
        if not skip_confirmation:
            while True:
                answer = input(
                    f"Diese {count} Einträge wirklich löschen? [y/n] "
                ).strip().lower()
                if answer == "y":
                    break
                if answer == "n":
                    print("Löschen verworfen.")
                    return
                print("Bitte y oder n eingeben.")

        remaining = self.database.delete_directory_root(root_path)
        print(f"Noch vorhandene Einträge in directories: {remaining}")


    @staticmethod
    def _confirm_binarycheck_repeat(hostid,count):
        scope='Insgesamt' if hostid is None else f'Host {hostid}'
        try:
            return input(f'{scope}: {count} unveränderte, bereits geprüfte Vergleiche erneut einreihen? [yN] ').strip().lower()=='y'
        except (EOFError,KeyboardInterrupt):
            return False

    def remove_unused_jobs(self):
        """Show unused jobs and require an explicit [Yn] prompt before deleting."""
        hostid=self.args.hostid if self.args.hostid_explicit else None
        rows=self.database.list_unused_jobs(hostid=hostid)
        if not rows:
            print('Keine unbenutzten abgeschlossenen Jobs gefunden.')
            return 0
        print('Job-ID | Host | Task | Status | Root-Pfad')
        for row in rows:
            print(f"{row['jobid']:>6} | {row['hostid']} | {row['task']} | {row['status']} | {row['startdirname']}")
        try:
            answer=input(f'{len(rows)} unbenutzte Jobs löschen? [Yn] ').strip().lower()
        except (EOFError,KeyboardInterrupt):
            print('Keine Jobs gelöscht.')
            return 0
        if answer not in ('','y'):
            print('Keine Jobs gelöscht.')
            return 0
        deleted=self.database.remove_unused_jobs([row['jobid'] for row in rows])
        print(f'{deleted} unbenutzte Jobs gelöscht.')
        return 0

    def delete_live_excluded_directories(self):
        hostid = self.args.hostid if self.args.hostid_explicit else None
        count = self.database.live_excluded_directories(hostid=hostid)
        print(f"{count} Directory-Einträge unter live_excluded_absdirs gefunden.")
        if not count:
            return
        if not getattr(self.args, 'yes', False):
            try:
                answer = input(f"Diese {count} Directory-Einträge löschen? [yN] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = ''
            if answer != 'y':
                print("Keine Einträge gelöscht.")
                return
        deleted = self.database.live_excluded_directories(hostid=hostid, delete=True)
        print(f"{deleted} Directory-Einträge gelöscht.")


    def show_directory_summary_data(self):
        rows = self.database.directory_summary_data()
        if not rows:
            print("Keine Einträge in directories vorhanden.")
            return
        hosts = {}
        incomplete = {}
        for row in rows:
            counts = hosts.setdefault(row['hostid'], {})
            counts[row['runid']] = counts.get(row['runid'], 0) + row['directory_count']
            key = (row['hostid'], row['runid'])
            incomplete[key] = incomplete.get(key, 0) + int(row.get('incomplete_count') or 0)
        symbols = "ABCDEFGHJKLMNOPQRSTUVWXYZabcdefghiklmnopqrstuvwxyz0123456789"
        max_total = max(sum(counts.values()) for counts in hosts.values())
        next_mark = 0
        legend = []
        timestamps = {(row["hostid"], row["runid"]): row.get("run_timestamp") for row in rows}
        job_ids = {(row["hostid"], row["runid"]): row.get("jobid") for row in rows}
        active_jobs = {(row["hostid"], row["runid"]): bool(row.get("active_job")) for row in rows}
        missing_hashes = {}
        for row in rows:
            key = (row['hostid'], row['runid'])
            missing_hashes[key] = missing_hashes.get(key, 0) + int(row.get('missing_hash_count') or 0)
        for hostid, counts in sorted(hosts.items()):
            runs = sorted(counts, key=lambda runid: (-counts[runid], -runid))
            total = sum(counts.values())
            # Shared scale: 64 characters represent the largest host total.
            bar_width = (total * 64 + max_total // 2) // max_total
            widths = [counts[runid] * 64 // max_total for runid in runs]
            remainders = sorted(range(len(runs)),
                                key=lambda i: counts[runs[i]] * 64 % max_total, reverse=True)
            for i in remainders[:bar_width - sum(widths)]:
                widths[i] += 1
            marks = [symbols[i] if i < len(symbols) else chr(0x100 + i - len(symbols))
                     for i in range(next_mark, next_mark + len(runs))]
            next_mark += len(runs)
            segments = []
            colored_segments = []
            omitted_total = omitted_completed = 0
            for mark, runid, width in zip(marks, runs, widths):
                if width < 3:
                    omitted_total += counts[runid]
                    key = (hostid, runid)
                    omitted_completed += max(0, counts[runid] - incomplete[key])
                    continue
                jobid = job_ids[(hostid, runid)]
                label = f" {mark}: job {jobid if jobid is not None else '?'} "
                if len(label) > width - 2:
                    label = f" {mark} " if width >= 5 else mark
                segment = '<' + label.center(width - 2, '-') + '>'
                segments.append(segment)
                pending_width = min(width, (incomplete[(hostid, runid)] * width
                                           + counts[runid] // 2) // counts[runid])
                done_width = width - pending_width
                colored_segments.append(
                    (f"\033[7m{segment[:done_width]}\033[0m" if done_width else '')
                    + segment[done_width:]
                )
            padding = '.' * (bar_width - sum(len(segment) for segment in segments))
            bar = ''.join(segments) + padding
            inverse_dots = ((len(padding) * omitted_completed + omitted_total // 2) // omitted_total
                            if omitted_total else 0)
            colored_padding = ((f"\033[7m{padding[:inverse_dots]}\033[0m" if inverse_dots else '')
                               + padding[inverse_dots:])
            display_bar = ''.join(colored_segments) + colored_padding if sys.stdout.isatty() else bar
            print(f"{hostid} |{display_bar}| Gesamt: {total}")
            cells = []
            for mark, runid in zip(marks, runs):
                timestamp = timestamps[(hostid, runid)]
                if timestamp is None:
                    stamp = '-'
                else:
                    # Database timestamps are UTC; display age in local calendar days.
                    local_time = (timestamp.replace(tzinfo=timezone.utc)
                                  if timestamp.tzinfo is None else timestamp).astimezone()
                    days = (datetime.now().astimezone().date() - local_time.date()).days
                    stamp = local_time.strftime('%Hh%M') if days == 0 else f"{-days} days"
                run_label = f"{mark}->{runid}"
                key = (hostid, runid)
                mode = 1 if incomplete[key] else 2 if missing_hashes[key] else 3
                active_mark = '*' if active_jobs[key] else ' '
                cells.append(f"{run_label:>6}  {counts[runid]:06d}  {mode}{active_mark}  {stamp}")
            for offset in range(0, len(cells), 2):
                left = cells[offset]
                right = cells[offset + 1] if offset + 1 < len(cells) else ''
                legend.append(f"| {hostid:>4} | {left} |  | {right}")
        print("| Host | Run id  Count   M   Time    |  | Run id  Count   M   Time  |")
        for line in legend:
            print(line)
        jobs = self.database.list_directory_hostids()
        if jobs:
            print("\nHost | Job-ID | Directories | Runids | Ende (UTC) | Root paths")
            for job in jobs:
                jobid = job['jobid'] if job['jobid'] is not None else '-'
                runids = ', '.join(str(runid) for runid in job['runids']) or '-'
                ended = job['timeend']
                if ended is None:
                    end_label = '-'
                else:
                    if ended.tzinfo is not None:
                        ended = ended.astimezone(timezone.utc)
                    end_label = ended.strftime('%Y-%m-%d %H:%M:%S')
                roots = '; '.join(job['root_paths']) or '-'
                print(f"{job['hostid']} | {str(jobid):>3} | {int(job['directory_count']):05d} | "
                      f"{runids} | {end_label} | {roots}")


    def list_directory_runids(self):
        """Runid-Werte samt Anzahl je Feld in directories anzeigen."""
        rows = self.database.list_directory_runids()
        if not rows:
            print("Keine Runids in directories vorhanden.")
            return
        print("Runids in directories (Anzahl je Feld):")
        print("runid | directories.runid | directories.finished")
        for runid, runid_count, finished_count in rows:
            label = "NULL" if runid is None else str(runid)
            print(f"{label} | {runid_count} | {finished_count}")


    def delete_absolute_exclusions(self, hostid):
        """Offer deletion of all, one numbered entry, or none for one host."""
        paths = self.database.list_live_excluded_absdirs(hostid)
        if not paths:
            print(f"Keine absoluten Ausschlusspfade für hostid={hostid} gespeichert.")
            return
        print(f"Absolute Ausschlusspfade für hostid={hostid}:")
        for number, path in enumerate(paths, start=1):
            print(f"{number}: {path}")
        while True:
            try:
                answer = input(
                    "Löschen: * = alle, Nummer = ein Eintrag, n oder Enter = keine: "
                ).strip().lower()
            except (EOFError, KeyboardInterrupt):
                print("\nKeine Einträge gelöscht.")
                return
            if answer in ("", "n", "nein"):
                print("Keine Einträge gelöscht.")
                return
            if answer == "*":
                self.database.erase_live_excluded_absdirs(hostid)
                print(f"Alle absoluten Ausschlusspfade für hostid={hostid} gelöscht.")
                print("Laufende Scan-Prozesse übernehmen die Änderung nach einem Server-Neustart.")
                return
            try:
                number = int(answer)
            except ValueError:
                number = 0
            if not 1 <= number <= len(paths):
                print(f"Bitte *, eine Nummer von 1 bis {len(paths)} oder n eingeben.")
                continue
            path = paths[number - 1]
            deleted = self.database.delete_live_excluded_absdir(hostid, path)
            if deleted:
                print(f"Ausschlusspfad für hostid={hostid} gelöscht: {path}")
                print("Laufende Scan-Prozesse übernehmen die Änderung nach einem Server-Neustart.")
            else:
                print(f"Eintrag nicht mehr vorhanden: {path}")
            return


    def show_excluded_names(self, hostid=0):
        """Show names stored globally for client-side job insertion."""
        names = self.database.list_excluded_names()
        if names is None:
            print("Tabelle excluded_names ist nicht eingerichtet.")
        elif not names:
            print("Keine Ausschlussnamen gespeichert.")
        else:
            print("Gespeicherte Ausschlussnamen:")
            for name in names:
                print(name)

        print(f"Live-Ausschlusspfade (hostid={hostid}):")
        for path in self.database.list_live_excluded_absdirs(hostid):
            print(path)


    def include_excluded_names(self, skipdirs):
        """Ask whether shared excluded names should be copied into this job."""
        if skipdirs and skipdirs.strip():
            return skipdirs
        names = [name for name in (self.database.list_excluded_names() or [])
                 if not os.path.isabs(name)]
        if not names:
            return skipdirs
        while True:
            answer = input(
                f"{len(names)} Ausschlussnamen (Tbl excluded_names) übernehmen? [Y/n] "
            ).strip().lower()
            if answer in ("", "y"):
                combined = [
                    value.strip()
                    for value in (skipdirs or "").split(",")
                    if value.strip()
                ]
                combined.extend(names)
                return ",".join(dict.fromkeys(combined))
            if answer == "n":
                return skipdirs
            print("Bitte y oder n eingeben.")


    def drop_directories_for_runid(self, runid, skip_confirmation=False):
        """Runid-markierte Zeilen anzeigen, bestätigen und gezielt löschen."""
        count = self.database.count_directories_for_runid(runid)
        if count == 0:
            print(f"Keine Einträge mit Runid {runid} in directories vorhanden.")
            return
        print(
            f"Runid {runid} trifft {count} Einträge in directories "
            "(Spalte runid oder finished)."
        )
        if not skip_confirmation:
            while True:
                answer = input(
                    f"Diese {count} Einträge wirklich löschen? [y/n] "
                ).strip().lower()
                if answer == "y":
                    break
                if answer == "n":
                    print("Löschen verworfen.")
                    return
                print("Bitte y oder n eingeben.")
        remaining = self.database.delete_directories_for_runid(runid)
        print(f"Noch vorhandene Einträge in directories: {remaining}")


    def _duplicate_output(self):
        from doubles_output import doubles_output
        output = getattr(self, "_duplicate_output_instance", None)
        if output is None:
            output = doubles_output(self.database)
            self._duplicate_output_instance = output
        output.database = self.database
        return output

    def list_doubles(self, *args, **kwargs):
        return self._duplicate_output().list_doubles(*args, **kwargs)

    def transform_double_dirnames(self, *args, **kwargs):
        return self._duplicate_output().transform_double_dirnames(*args, **kwargs)

    def list_double_jobs(self, *args, **kwargs):
        return self._duplicate_output().list_double_jobs(*args, **kwargs)

    def show_doubles_progress(self, *args, **kwargs):
        return self._duplicate_output().show_doubles_progress(*args, **kwargs)

    def _list_verified_duplicate_files(self, *args, **kwargs):
        return self._duplicate_output()._list_verified_duplicate_files(*args, **kwargs)

    @staticmethod
    def _verified_pairs_html_data(*args, **kwargs):
        from doubles_output import doubles_output
        return doubles_output._verified_pairs_html_data(*args, **kwargs)

    @staticmethod
    def _largest_duplicate_directories(*args, **kwargs):
        from doubles_output import doubles_output
        return doubles_output._largest_duplicate_directories(*args, **kwargs)

    @staticmethod
    def _render_3d_tree(*args, **kwargs):
        from doubles_output import doubles_output
        return doubles_output._render_3d_tree(*args, **kwargs)

    def _render_doubles_progress_html(self, *args, **kwargs):
        return self._duplicate_output()._render_doubles_progress_html(*args, **kwargs)

    def show_doubles_progress_html(self, *args, **kwargs):
        return self._duplicate_output().show_doubles_progress_html(*args, **kwargs)


    def show_doubles_tree_html(self, *args, **kwargs):
        return self._duplicate_output().show_doubles_tree_html(*args, **kwargs)
