# Einrichtung und typische Arbeitsabläufe

Für Version 0.25.0. Befehle im Projektverzeichnis mit aktivierter Python-Umgebung
und gesetzten DB-Umgebungsvariablen ausführen; siehe [README.md](README.md).

## Datenbank einrichten oder prüfen

```sh
python3 dirwalker.py init
python3 dirwalker.py check
```

`init` legt fehlende Kerntabellen an, prüft das MariaDB-Zeitlimit und richtet die
Job-Constraints ein. Beim vorhandenen `directories` fragt es nach einem
Neuaufbau. Wird die Tabelle neu angelegt, werden auch Jobs, Runid-Historie,
Arbeitslisten und veraltete Vergleichsdaten geleert; Host-Registrierungen bleiben
erhalten. Bei Ablehnung bleiben das vorhandene Inventar und die Jobs bestehen.
Am Ende wird Host 0 registriert und das Schema geprüft.

`serverinit` registriert den aktuellen Server als Host 0, legt dessen Arbeitsliste
bei Bedarf an und richtet die Job-Constraints ein. Es setzt vorhandene
Vergleichsergebnisse oder andere Host-Registrierungen nicht zurück.
`--init-jobs` richtet die Jobtabellen ein; `--init-alldirs` beziehungsweise
`--drop-directories` bietet den Neuaufbau von `directories` an.

Bei einer vorhandenen Installation zuerst die benötigten Migrationen anwenden,
statt die Datenbank nur zur Ergänzung einer Spalte neu aufzubauen.
`check` prüft Schema und Root-Job-Konflikte. Zu jedem Eintrag in `hosts` muss ein
gültiger Tabellenname mit vorhandener Arbeitstabelle hinterlegt sein.

## Scan einreihen und ausführen

```sh
python3 dirwalker.py --jobs-insert /volume1/homes/alex
python3 dirwalker.py --jobs-insert /volume1/homes/alex '@eaDir,.git,cache'
python3 dirwalker.py --jobs-insert-scanall /volume1/homes/alex
python3 dirwalker.py server
```

Ohne explizite Namensliste fragt der Client nach der Übernahme globaler
Ausschlussnamen. Eine explizite Liste ersetzt diese für den Job. Absolute
Live-Ausschlüsse gelten zusätzlich für den jeweiligen Host. `scanall` verhindert
das Überspringen bereits abgeschlossener Teilbäume aus früheren Scans.

Auf einem lokalen Rechner eine positive Host-ID registrieren und verwenden:

```sh
python3 dirwalker.py --setmyhostid
python3 dirwalker.py show hosts
python3 dirwalker.py --hostid 1 --jobs-insert /Users/alex
python3 dirwalker.py localserver --hostid 1
```

Die tatsächliche registrierte Host-ID verwenden. `server` bearbeitet Host 0 ohne
Binarychecks, `serverplus` zusätzlich Binarychecks. `localserver` bearbeitet die
passenden Jobs seines registrierten Hosts einschließlich Binarychecks.

## Jobs verwalten

```sh
python3 dirwalker.py show jobs
python3 dirwalker.py --jobs-list-active
python3 dirwalker.py --jobs-stop 12
python3 dirwalker.py --jobs-start 12
python3 dirwalker.py --jobs-reset-on-error
```

Ein pausierter Job setzt seine Stufe fort. Beim Start eines gestoppten Jobs wird
ein neuer Job angelegt. `--jobs-reset-on-error` bietet Fehlerjobs zum erneuten
Einreihen an. `--jobs-deleteid JOBID` fragt vor dem Löschen; `-Y` überspringt
hier die Rückfrage. `--jobs-deleteall` löscht alle Jobs und zugehörige Arbeitsdaten.
`--jobs-remove-unused` listet abgeschlossene, nicht mehr vom Inventar oder von
laufbereiten Folgejobs benötigte Jobs und fragt mit `[Yn]` nach dem Löschen.

## Änderungen erkennen und nacharbeiten

```sh
python3 dirwalker.py --check-modifications
python3 dirwalker.py --hostid 1 --check-modifications
python3 dirwalker.py --hostid 1 --directories-rescan-marked
python3 dirwalker.py --rescan-after-days 6
```

Diese Befehle reihen Jobs ein; erst der zuständige Server greift auf die Dateien
zu. `--check-modifications` markiert Änderungen, `--directories-rescan-marked`
verarbeitet die obersten markierten Teilbäume und reiht nötige Detailjobs ein.
Die altersabhängige Schnellprüfung arbeitet dagegen als eigener Job mit den
Stufen `firstscan`, `fixunreadable`, `workmodified`; sie startet anschließend
keinen automatischen Detailscan. Siehe [rescan.md](rescan.md) und
[age-rescan.md](age-rescan.md).

## Inventar und Ausschlüsse verwalten

```sh
python3 dirwalker.py --dirs-summary-data
python3 dirwalker.py --dirs-droprunid 42
python3 dirwalker.py --dropdir
python3 dirwalker.py --excluded-dirnames '@eaDir,.git,.venv'
python3 dirwalker.py --excluded-absolute 1 /Users/alex/Library
python3 dirwalker.py --hostid 1 --dirs-purge-from-exclusion-lists
```

Die Zusammenfassung zeigt Host, Job-ID, Directory-Anzahl, Runids, Job-Endzeit und
Root-Pfade; die Sortierung erfolgt absteigend nach Anzahl der Directory-Einträge.
`--dirs-droprunid` und `--dropdir` löschen nach Rückfrage Inventarzeilen.
Die Ausschlussbereinigung löscht passende Directory-Einträge samt Nachkommen
hostweise in begrenzten SQL-Batches und meldet die Löschzahlen. Dateien auf dem
Datenträger bleiben erhalten. Ohne `--hostid` verarbeitet sie nacheinander die im
Inventar verwendeten Hosts.

## Duplikate prüfen und anzeigen

```sh
python3 dirwalker.py --doubles-list
python3 dirwalker.py --doubles-letcheck
python3 dirwalker.py serverplus
python3 dirwalker.py --doubles-progress
python3 dirwalker.py --doubles-progress-html
python3 dirwalker.py --doubles-tree
```

Bereits eingereihte Paare werden übersprungen. Für unveränderte, bereits geprüfte
Paare fragt der Client mit `[yN]`, ob sie erneut geprüft werden sollen.
Neue Vergleichspartner bleiben prüfbar. Siehe
[duplikate-pruefen.md](duplikate-pruefen.md) und
[binarycheck-freshness.md](binarycheck-freshness.md).

## Stoppen und Lockdateien bereinigen

Ctrl-C beendet den Server geordnet und wartet begrenzt auf DB-Aufräumen.
Eine verbleibende lokale Lockdatei bedeutet allein keinen gehaltenen Lock.
Sind alle Server und Localserver gestoppt, entfernt `--resetlocks` die lokalen
Dateien und zeigt je Datei den gesperrten Gültigkeitsbereich sowie die gespeicherte
historische PID. Optional begrenzt `--hostid` die Auswahl.

`--smartlocalserver --hostid 1` prüft zuordenbare DB-Verbindungen und bietet
Fehler-Resets mit `[yN]` an, bevor der lokale Server startet.
