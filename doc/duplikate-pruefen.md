# Verzeichnisse auf Duplikate prüfen

Alle Befehle im Projektverzeichnis ausführen:

```bash
cd /Users/alex/prog/ongh/dirwalker
```

## Voraussetzungen

- Die Datenbankverbindung ist über die Umgebungsvariablen eingerichtet.
- Die Verzeichnisse wurden vollständig gescannt; ihre Hashes und Baumstatistiken sind berechnet.
- Jeder beteiligte lokale Rechner ist mit seiner Host-ID registriert und hat Zugriff auf seine Dateien.

Die aktuelle Host-Registrierung anzeigen:

```bash
python3 dirwalker.py show hosts
```

### Schema prüfen

```bash
python3 dirwalker.py check
```

Bei älteren Installationen die passenden Migrationen aus `sql/` anwenden;
`serverinit` ist kein Neuaufbau der Vergleichstabellen. Es richtet die
Job-Constraints ein und registriert den aktuellen Server als Host 0, ohne andere
Host-Registrierungen oder bestehende Vergleichsergebnisse zurückzusetzen.

Für das neue Feld `directories.hashed_at` ist bei bestehenden Tabellen
[die Migration](../sql/migrate_directories_hashed_at.sql) erforderlich.
Vor Schemaänderungen die Worker stoppen und anschließend neu starten.

## 1. Duplikatkandidaten anzeigen

```bash
python3 dirwalker.py --doubles-list
```

Der Befehl liest `directories`. Er sucht nach Verzeichnissen mit gleichem `combined_sha256` und gleicher `subtree_structure`. Vollständig bekannte, lesbare reine Nullbyte-Teilbäume (`zero_content_only`) und untergeordnete Duplikatkandidaten werden ausgefiltert. Unbekannte oder unlesbare Bäume gelten nicht allein wegen einer Größe von null als leer.

Diese Liste beruht auf gespeicherten Metadaten. Sie bestätigt noch keine byteweise Gleichheit und kann bei großen Datenbeständen einige Zeit benötigen.

## 2. Byteweise Vergleiche einreihen

```bash
python3 dirwalker.py --doubles-letcheck
```

Der Befehl sucht zunächst Kandidaten je Host, anschließend je Hostpaar. Er legt
nur für Hosts mit verbleibender Vergleichsarbeit Binarycheck-Jobs an und speichert
die Paare in `tmp_doubles_letchecked`. Die ausgegebenen Job-IDs notieren.
Die eigentliche Prüfung übernimmt anschließend ein Worker.

Kandidaten benötigen vollständige Datei-, Unterverzeichnis-, Struktur- und
kombinierte Hashdaten. Bereits eingereihte Paare werden übersprungen. Bereits
geprüfte, seitdem unveränderte Paare werden gezählt und nur nach Zustimmung
insgesamt und je Host erneut eingereiht (`[yN]`, Standard Nein). Maßgeblich sind
`hashed_at >= scanned_at` und ein frisches Ergebnis für genau dieses Paar.
Neue Vergleichspartner bleiben prüfbar. Details:
[binarycheck-freshness.md](binarycheck-freshness.md).

Pro Paar werden beide Verzeichnis-IDs gespeichert. Die Ergebnisse unterscheiden zwischen `pending`, `equal`, `different`, `unreadable` und `skipped_empty`.

## 3. Worker starten

Für den Server mit `hostid=0`:

```bash
python3 dirwalker.py serverplus
```

Für einen registrierten lokalen Rechner, beispielsweise mit `hostid=1`:

```bash
python3 dirwalker.py localserver --hostid 1
```

Jeder Worker verarbeitet die Jobs seiner Host-ID. Der normale Modus `server` verarbeitet keine Binarycheck-Jobs.

Bei lokalen Jobs muss `jobs.userhost` zum registrierten Rechner passen. Beim Einreihen verwendet der Client für positive Host-IDs den in `hosts.machine` gespeicherten Rechnernamen.

Beim selben Host vergleicht Binarycheck reguläre Dateien rekursiv byteweise.
Für hostübergreifende Paare werden Inhaltsprüfsummen über `tmp_remotedoubles`
angefordert. Der Zielhost benötigt dafür einen aktiven Listener. Eine Meldung
`Listening Host 0: Runid …, PID …` bezeichnet diesen weiterlaufenden Dienst;
`Job …: ok` beendet den Listener nicht automatisch. Auf Host 0 dafür `serverplus`,
auf einem positiven Host den passenden `localserver` betreiben.

Abgeschlossene Ergebnisse (`equal` oder `different`) setzen `hashed_at`, sofern
kein neuerer Metadatenscan während der Prüfung stattgefunden hat. Unlesbare oder
fehlgeschlagene Vergleiche setzen diesen Zeitstempel nicht.

## 4. Fortschritt verfolgen

In einem weiteren Terminal:

```bash
python3 dirwalker.py --doubles-progress
```

Ohne Job-ID wird der neueste Binarycheck-Job gewählt. Einen bestimmten Job gezielt anzeigen, beispielsweise Job 34:

```bash
python3 dirwalker.py --doubles-progress 34
```

Die Anzeige liest den Jobstatus aus `jobs` und die Vergleichsergebnisse aus `tmp_doubles_letchecked`. Beim Start werden Job-ID und `jobs.last_activity` in UTC ausgegeben.

### HTML-Bericht

```bash
python3 dirwalker.py --doubles-progress-html
# Oder für einen bestimmten Job:
python3 dirwalker.py --doubles-progress-html 34
```

Der Befehl nennt den Pfad zur erzeugten Datei `doubles-progress-<JOBID>.html`. Diese Datei im Browser öffnen. Der Bericht berücksichtigt auch die anderen Host-Jobs desselben Batches.

### Vergleichstabelle direkt ansehen

```bash
python3 dirwalker.py show tmp_doubles_letchecked
```

Diese Rohansicht zeigt maximal 40 Datensätze ohne Header und anschließend die gesamte Datensatzanzahl. `left_dirid` und `right_dirid` identifizieren das Paar; `status` enthält das Ergebnis. `verification_method` unterscheidet direkte Prüfungen von Ergebnissen, die aus einem bestätigten übergeordneten Paar abgeleitet wurden.

## 5. Verschachtelte Duplikatkandidaten untersuchen

Nach vollständigem Abschluss der Vergleichsjobs:

```bash
python3 dirwalker.py --doubles-listlevel
```

Der Befehl gruppiert weitere Kandidaten nach Hash und verfolgt passende Elternpaare über mehrere Ebenen. Er berücksichtigt auch unterschiedliche Host-IDs; jede Elternkette bleibt innerhalb ihres jeweiligen Hosts.

Die Analyse startet nur, wenn alle Jobs des neuesten Batches geschlossen und alle angeforderten Vergleiche abgeschlossen sind. Nicht vollständig lesbare Vergleiche verhindern diesen Abschluss. Die Ergebnisse werden pro Batch in `tmp_deepdoubles_result` gespeichert.

### Gefilterter HTML-Baumbericht

```bash
python3 dirwalker.py --doubles-tree-html
# Oder für einen bestimmten Job:
python3 dirwalker.py --doubles-tree-html 34
```

Der Bericht wartet auf den vollständigen Abschluss des gewählten Batches und erstellt die rekursiven Ergebnisse. In `doubles-tree-<JOBID>.html` erscheint pro Hash-Gruppe nur die kleinste numerische `dirid` unter den Einträgen mit `level > 0`.

`level` zählt passende Elternebenen: Bei `level = 0` stimmen nur die Ausgangskandidaten überein; bei `level > 0` passen auch Elternpaare.

Diese rekursiven Ergebnisse sind **Hash-Kandidaten**, keine zusätzliche Inhaltsprüfung. Die Vergleichsjobs unterstützen sowohl lokale als auch hostübergreifende Paare.

## Temporäre Daten löschen

```bash
python3 dirwalker.py --droptmp
```

Der Befehl löscht die numerischen Host-Arbeitstabellen sowie Vergleichs-, Remote- und Deep-Doubles-Tabellen. `tmp_modified` und `tmp_highpriority` gehören nicht zu dieser Löschliste. Damit gehen Vergleichsergebnisse und Arbeitswarteschlangen verloren. Nur ausführen, wenn diese Daten nicht mehr benötigt werden und die betroffenen Worker beendet sind.

## HTML-Snapshot und Auswahl

```bash
python3 dirwalker.py --doubles-tree
python3 dirwalker.py --doubles-tree 34
```

`--doubles-tree` stellt bereits vorhandene Ergebnisse als Snapshot dar. Ohne
Job-ID umfasst die Auswahl alle Jobs und Hosts; mit Job-ID den betreffenden
Batch. Das ist von `--doubles-tree-html` mit rekursiver Ebenenanalyse zu
unterscheiden.

Der interaktive Baum unterstützt Drehen, Zoom, Seitenauswahl, Sortierung nach
Verzeichnisgröße oder Duplikatanzahl sowie Pfad- und Größeninformationen beim
Überfahren. Im finalen Fortschrittsbericht können Verzeichnisse für Copypaste
ausgewählt werden. Das kopierte Format beginnt mit `listof_files_job<JOBID>`
und enthält danach Gruppen-IDs und Directory-IDs.

```bash
python3 dirwalker.py --doubles-dirids-to-dirnames
```

Den kopierten Text einfügen und mit `.` auf eigener Zeile oder EOF abschließen.
Der Befehl schreibt die vollständigen Pfade in `listof_files_job<JOBID>.txt`
beziehungsweise ohne Jobkopf in `listof_files.txt`.
