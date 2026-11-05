# Altersabhängige Schnellprüfung

```sh
python3 dirwalker.py --rescan-after-days 10
```

Optional begrenzt `--hostid N` die Auswahl auf einen Host. Ohne Host-ID werden alle
in `directories` verwendeten Hosts berücksichtigt. Für jedes betroffene Paar aus
Host und Quelljob wird ein neuer Job mit Task `rescan-after-days` eingetragen.
Mehrere Runids oder Root-Pfade desselben Quelljobs erzeugen keine zusätzlichen
Jobs. Bereits wartende, aktive oder pausierte Schnellprüfungen desselben Paars
werden nicht doppelt eingereiht.

Die Altersgrenze verwendet `jobs.timeend`, bei fehlender Stoppzeit
`jobs.timecreated`. Jobs müssen strikt älter als die angegebene Anzahl Tage sein.
Alle Zeiten werden in UTC verglichen.

`server`, `serverplus` und `localserver` führen denselben neuen Job in drei Stufen
mit jeweils einem gestarteten Worker-Prozess aus:

1. `firstscan`: Ermittelt die Root-Pfade über die Runid/Job-Verknüpfung und läuft
   durch den Dateisystembaum. Directory-mtime und die mtimes eingeschlossener
   regulärer Dateien werden mit `time_inode_changed` verglichen. Änderungen kommen
   in `tmp_modified`. Nicht lesbare Directory-Einträge erhöhen `read_failures` und
   werden zur Nachprüfung vermerkt. Hashes und gespeicherte Scan-Zeitstempel werden
   in dieser Stufe nicht geändert. Fehlt zu einem gespeicherten Root-Pfad der
   eigene Directory-Datensatz dieses Hosts, werden die zugehörigen Inventarzeilen
   gelöscht und ein gewöhnlicher Scan-Job (`scan`, `insert`, `coarse`) eingereiht.
   Erst nach dem Commit wird dies auf Konsole und Syslog gemeldet.
2. `fixunreadable`: Führt höchstens die noch fehlenden Leseversuche bis insgesamt
   drei Fehlern durch. Bei Erfolg wird der Zähler zurückgesetzt. Nach drei Fehlern
   werden der Directory-Datensatz und seine inventarisierten Nachkommen gelöscht.
   Dateien und Verzeichnisse auf dem Datenträger werden nicht gelöscht.
3. `workmodified`: Verarbeitet die vermerkten Änderungen grob, entdeckt neue
   Unterverzeichnisse und entfernt verschwundene Inventory-Zweige. Aktualisiert
   `files_lastwrite`, `time_inode_changed`, die Kinderzahl und die Runid. Betroffene
   lokale Hashes sowie unvollständige Aggregate werden geleert. Die Datensätze
   bleiben damit für eine spätere Detailberechnung vorbereitet; diese wird nicht
   automatisch gestartet. Unveränderte Daten behalten ihre gültigen Hashes.

`workmodified` übernimmt die aktualisierten Root-Bestände mit seiner aktiven
Runid, damit ein Root nicht künstlich auf alte und neue Eigentümerjobs verteilt
bleibt. Erneut auftretende Zugriffsfehler führen zurück zu `fixunreadable`.
`firstscan`, `fixunreadable` und `workmodified` sind **Stufen desselben Jobs**;
sie erzeugen keine separaten Jobzeilen.

Die Arbeitstabelle ist nach `(batch_jobid, hostid, dirid)` getrennt. Die Batch-ID
ist die neue Wartungs-Job-ID. Frühere Änderungskontrollen verwenden Batch 0 und
entfernen ausschließlich ihre eigenen Host-Einträge. Dadurch bleiben Ergebnisse
anderer Hosts und pausierter Jobs erhalten. Bei Wiederholung von `firstscan`
wird nur dessen eigener Bericht neu aufgebaut.

## Bestehende Datenbank aktualisieren

Alle Worker anhalten und eine Datenbanksicherung anlegen. Die aktuellen
Python-Dateien einschließlich `sqlstructure.py` bereitstellen. Anschließend das
manuelle Skript [migrate_age_rescan.sql](../sql/migrate_age_rescan.sql) in der
gewählten Datenbank ausführen und prüfen:

```sh
python3 dirwalker.py check
```

Das Skript setzt vorhandene Tabellen `jobs`, `directories` und `tmp_modified`
voraus. Es ergänzt `source_jobid`, erweitert die Task-/Status-Constraints und
stellt den Änderungsbericht unter Erhalt bestehender Zeilen auf die
Batch-Struktur um. Eine Python-Methode zur Migration gibt es nicht mehr.
Neue Datenbanken erhalten die Definition durch `init`/`--init-jobs` aus
`sqlstructure.py`. Bei bestehenden Tabellen ergänzt `CREATE TABLE IF NOT EXISTS`
keine fehlenden Spalten. Vorherige Directory-Migrationen für `files_lastwrite`,
`rescan_from_here`, `read_failures`, `time_inode_changed` und inzwischen auch
`hashed_at` müssen ebenfalls vorhanden sein. Erfolgreich abgearbeitete
Batch-Berichte werden am Ende bereinigt; pausierte oder erneut zu bearbeitende
Batches bleiben getrennt erhalten.
