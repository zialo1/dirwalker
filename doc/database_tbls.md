# Datenbanktabellen in Version 0.25.0

Die Datenbank wird über `DB_NAME` gewählt (Programmstandard: `filestatus`).
Tabellennamen, DDL, Constraints, erwartete Spalten und die Strukturprüfung liegen
in `sqlstructure.py`. `SQLStructure` enthält serverfähige Hilfen,
`SQLStructureCLI` die über `DataBaseCLI` aufgerufenen Verwaltungsfunktionen.
Verbindungen und Transport-Timeouts gehören weiterhin zu `DataBaseCore.py`.
Die exakte Definition im Code ist maßgeblich; diese Übersicht erklärt ihre Nutzung.

## Tabellenübersicht

| Tabelle | Aufgabe |
| --- | --- |
| `hosts` | Rechnerregistrierung und Name der hosteigenen Arbeitstabelle. |
| `jobs` | Aufgaben, Status, aktuelle Stufe und Ausführungszeiten. |
| `jobsrunid` | Runid-Historie mit Job- und Prozesszuordnung. |
| `directories` | Inventar, Elternbeziehungen, Zeitstempel, Hashes und Rescan-Zustand. |
| `tmp<HOSTID>_job_work` | Persistente Scan-Arbeitsliste mit Checkpoints je Host. |
| `excluded_names` | Globale Ausschlussnamen. |
| `live_excluded_absdirs` | Absolute Ausschlusspfade je Host. |
| `tmp_modified` | Host- und batchgetrennte Berichte über Änderungen und Lesefehler. |
| `tmp_doubles_letchecked` | Angeforderte oder abgeleitete Vergleichspaare und Ergebnisse. |
| `tmp_remotedoubles` | Prüfanfragen und Antworten für hostübergreifende Binarychecks. |
| `tmp_highpriority` | Prioritätshinweise als `(hostid, dirid)`. |
| `tmp_deepdoubles_result` | Gespeicherte rekursive Duplikatanalyse je Batch. |

Die `tmp`-Tabellen sind persistente, zwischen Verbindungen gemeinsam nutzbare
Tabellen. Der Namensbestandteil bedeutet nicht SQL-`TEMPORARY TABLE`.
Einige Arbeitstabellen entstehen erst bei Verwendung der jeweiligen Funktion.

## `hosts` und Host-Arbeitstabellen

`hostid=0` bezeichnet das Serverdateisystem; positive IDs registrieren weitere
Rechner. `user`, `homepath`, `system` und `machine` identifizieren den Rechner,
`homedirid` kann sein inventarisiertes Home-Verzeichnis referenzieren.
`tabellenname` nennt die Arbeitstabelle, gewöhnlich `tmp0_job_work`,
`tmp1_job_work` usw. Ein leerer Name ist ein Fehler.

`serverinit` registriert den aktuellen Rechner als Host 0 und richtet dessen
Arbeitstabelle bei Bedarf ein. `--setmyhostid` registriert einen lokalen Host;
bei bereits bekannten Rechner-/Home-Daten erfolgt eine Rückfrage vor einer
zusätzlichen Registrierung. `check` prüft für jede registrierte Host-ID den
hinterlegten Tabellennamen und die Existenz der Tabelle.

Die Queue besitzt die Schlüsselspalten `jobid`, `stage`, `hostid`, `path_hash`
sowie `path`, `parent_path`, `child_count` und `state`. Zustände sind `pending`,
`expanded`, `done`. Fertige Teilbäume können zu einem einzigen Marker
kompaktiert werden. Siehe [modus.md](modus.md).

## `jobs`

| Spalten | Bedeutung |
| --- | --- |
| `jobid` | Globale Auto-Increment-Job-ID. |
| `hostid`, `userhost` | Zuständiges Dateisystem und Rechnerzuordnung des Jobs. |
| `startdirname`, `startdirid` | Einstiegspfad oder internes Label und optionale Directory-ID. |
| `task` | `scan`, `rescan`, `check-modifications`, `rescan-after-days`, `binarycheck`, `check`, `verify`. |
| `stage` | Gespeicherte Stufe; unter anderem `coarse`, `detailed`, `firstscan`, `fixunreadable`, `workmodified`. |
| `source_jobid` | Quelljob einer altersabhängigen Schnellprüfung. |
| `status` | `insert`, `pending`, `active`, `paused`, `stopped`, `closed`, `error`, `stale`. |
| `skipdirs`, `scanall` | Gespeicherte Ausschlussnamen und Schalter gegen das Überspringen fertiger Teilbäume. |
| `runid` | Aktuelle beziehungsweise letzte zugewiesene Runid. |
| `timecreated`, `timestart`, `timeend`, `timeupdated`, `last_activity` | UTC-Zeitpunkte für Anlage, Start, Ende, Änderung und Aktivität. |
| `errmsg`, `usererror` | Technischer Fehler und Benutzerhinweis. |

`chk_job_task` und `chk_job_status` sind benannte SQL-`CHECK`-Constraints.
Sie beschränken erlaubte Zeichenketten; sie sind keine Jobs oder Tasks.
`init` und `serverinit` richten diese Prüfregeln ebenfalls ein.

`--jobs-remove-unused` berücksichtigt die indirekte Verknüpfung über
`jobsrunid` und schützt Jobs, die noch vom Inventar oder laufbereiten Folgejobs
benötigt werden. Vor dem Löschen nach `[Yn]` wird die Eignung erneut geprüft.
Die Runid-Historie bleibt bei gewöhnlichen Joblöschungen erhalten.

## `jobsrunid`

`runid` ist der eindeutige Schlüssel, `jobid` der zugehörige Job.
`task`, `status`, `stage` und `timecreated` beschreiben das protokollierte
Laufereignis. `pid`, `process_host`, `process_start` und `remotehost` unterstützen
die Prozess- und Listenerzuordnung. Die globale Vergabe wird mit einer
DB-Sperre serialisiert. Ein Job kann mehrere Runids besitzen.

## `directories`

| Spaltengruppe | Zweck |
| --- | --- |
| Identität | `dirid` ist global eindeutig; `hostid`, `path`, `path_hash`, `root_path`, `name` ordnen den Pfad zu. `path_hash` ist ein 16-stelliger BLAKE2b-Fingerabdruck. |
| Baum und Lauf | `parentdir` referenziert den Eltern-Datensatz, Wurzeln verwenden 0. `runid` ordnet den Bestand über `jobsrunid` einem Job zu; `finished` kennzeichnet abgeschlossene Teilbäume. |
| Lokale Werte | `localfilesize`, `localfilecount`, `emptyfilecount`, `subdircount`, `otherentrycount`, `nopermdirs` und die älteren Zählfelder beschreiben lokale Einträge. |
| Hashes und Aggregate | `file_list_sha256`, `files_sha256`, `subdirs_sha256`, `combined_sha256`, `subdirectory_hash`, `subtree_size_bytes`, `subtree_structure`. |
| Ausschlussflag | `zero_content_only` kennzeichnet vollständig bekannte, lesbare Teilbäume ausschließlich aus Nullbyte-Dateien beziehungsweise leere Bäume ohne sonstige Einträge. |
| Rescan | `rescan_from_here` markiert einen Einstieg; `read_failures` zählt aufeinanderfolgende Lesefehler. |
| `scanned_at` | Zeitpunkt der Inventarisierung beziehungsweise Metadatenaktualisierung. |
| `hashed_at` | Zeitpunkt einer abgeschlossenen binären Prüfung; gültig gegenüber dem Scan bei `hashed_at >= scanned_at`. Zusätzlich wird die Frische des konkreten Vergleichspaars geprüft. |
| `files_lastwrite` | Neueste mtime einer direkt enthaltenen, eingeschlossenen regulären Datei; NULL ohne solche Dateien. |
| `time_inode_changed` | Gespeicherte Directory-`st_mtime`, trotz des Namens nicht `st_ctime`. |

Ein kombinierter Hash entsteht erst bei vollständigen lokalen Daten und fertig
bearbeiteten Kindern. Fehlende Hashes werden nicht als leere Teilbäume behandelt.
Die altersabhängige Nacharbeit kann die Runid des Root-Bestands übernehmen;
die Spalte ist kein unveränderlicher Erstellungszeitpunkt.

`--dirs-summary-data` zeigt je Host und Eigentümerjob Directory-Anzahl, Runids,
Endzeit und Root-Pfade. Die Kernabfrage sortiert absteigend nach Anzahl; Job-ID
wird mit `%3s`, die Anzahl in der Spalte `Directories` mit `%05d` ausgegeben.
Konfliktprüfungen suchen verschiedene Job-IDs desselben Root-Pfads.

## Ausschlusslisten

`excluded_names.name` speichert globale Namen. Beim Einreihen gewöhnlicher Scans
können sie in `jobs.skipdirs` übernommen werden; eine explizite Liste ersetzt sie.
`live_excluded_absdirs` speichert `(hostid, path_hash, path)` für absolute Pfade.
Die Listen und gespeicherten Job-Ausschlüsse werden je nach Operation kombiniert;
der markierte Rescan verwendet auch die aktuellen globalen Namen.

`--dirs-purge-from-exclusion-lists` entfernt passende Inventarzweige hostweise
in begrenzten SQL-Batches und meldet pro Host sowie insgesamt die Löschzahl.
Es löscht keine Dateien auf dem Datenträger.

## `tmp_modified`

Der Primärschlüssel ist `(batch_jobid, hostid, dirid)`.
`runid`, `newtimestamp`, `subfolders_excluded`, `unreadable` und `path`
beschreiben die Änderung beziehungsweise den Lesezustand.

Batch 0 gehört zur klassischen Änderungskontrolle. Ein Alters-Rescan verwendet
seine eigene Job-ID als Batch. Dadurch löscht ein Host oder Job nicht die
Berichte eines anderen. `subfolders_excluded=TRUE` bezeichnet eine lokale
Änderungsmarkierung und bedeutet nicht, dass der gesamte Workflow keine
Unterverzeichnisse mehr prüft. Erfolgreich verarbeitete Berichtseinträge werden
am Ende bereinigt; benötigte Wiederholungsinformationen bleiben erhalten.

## Vergleichstabellen

`tmp_doubles_letchecked` identifiziert ein Paar durch
`(jobid, hostid, hostidB, left_dirid, right_dirid)`. Bei gleichem Host gilt
`left_dirid < right_dirid`; unterschiedliche Hosts sind erlaubt.
`status` ist `pending`, `equal`, `different`, `unreadable` oder `skipped_empty`.
`verification_method` unterscheidet direkte und abgeleitete Ergebnisse;
`source_left_dirid` und `source_right_dirid` verweisen gegebenenfalls auf das
bestätigte übergeordnete Paar. `finished_at`, `error_message`, `is_requested`
und `runid` beschreiben Abschluss, Fehler, angeforderte Arbeit und Lauf.

`tmp_remotedoubles` transportiert Prüfanfragen pro `(runid, hostid, dirid)`.
Die Zeilen enthalten unter anderem Job, Listener-Runid, Algorithmus, Status,
Prüfsumme, Claim-Token und Aktualisierungszeit. Der Listener kann nach Abschluss
eines Jobs weiterhin aktiv sein. Details: [duplikate-pruefen.md](duplikate-pruefen.md).

## Einrichtung, Migration und Rücksetzen

| Befehl | Wirkung |
| --- | --- |
| `init` | Kerntabellen und Constraints einrichten; Directory-Neuaufbau anbieten, gegebenenfalls alte Jobverweise leeren; Host 0 registrieren und Schema prüfen. |
| `serverinit` | Timeout und Job-Constraints prüfen/einrichten; Host 0 samt Arbeitstabelle registrieren. |
| `--init-jobs` | Datenbank und Jobtabellen bei Bedarf erstellen. |
| `--init-alldirs`, `--drop-directories` | Directory-Tabelle nach Rückfrage neu anlegen. |
| `check` | Struktur, registrierte Host-Arbeitstabellen und Root-Job-Konflikte prüfen. |
| `--droptmp` | Numerische Host-Arbeitslisten sowie ausgewählte Vergleichs-, Remote- und Deep-Doubles-Tabellen löschen. Nicht alle Tabellen mit `tmp`-Präfix werden gelöscht. |
| `--dropall` | Host-Arbeitslisten sowie Ausschluss- und Vergleichstabellen zurücksetzen; zusätzlich nach `[yN]` auch `directories`, `jobs`, `hosts`, `jobsrunid`. Reguläre Tabellen werden neu angelegt; legacy `job_work` bleibt erhalten. |

Vor Schemaänderungen und Rücksetzen alle Worker stoppen und eine Sicherung
anlegen. DDL wird von MariaDB eigenständig bestätigt; mehrere DDL- und
Bereinigungsschritte sind kein atomarer Gesamtvorgang. `CREATE TABLE IF NOT EXISTS`
ergänzt keine fehlenden Spalten einer bestehenden Tabelle.

Die Directory-Migrationen liegen unter [../sql](../sql), darunter
[migrate_directories_hashed_at.sql](../sql/migrate_directories_hashed_at.sql).
Die Migration für `source_jobid`, Task-Constraints und die Batch-Struktur von
`tmp_modified` erfolgt manuell über [migrate_age_rescan.sql](../sql/migrate_age_rescan.sql); der Ablauf ist in [age-rescan.md](age-rescan.md#bestehende-datenbank-aktualisieren)
beschrieben. Es gibt keine Python-Migrationsmethode dafür. Nach jeder Aktualisierung `check` ausführen.
