# Jobs, Stufen und Serverbetrieb

Stand: Version 0.25.0. Die Bedienungsbeispiele stehen in
[workflow.md](workflow.md), die Tabellen in [database_tbls.md](database_tbls.md).

## Task, Stufe und Status

`jobs.task` bezeichnet die Aufgabe, `jobs.stage` den aktuellen Verarbeitungsschritt
und `jobs.status` den Zustand des Jobs.

| Task | Ablauf |
| --- | --- |
| `scan` | Gewöhnlich `coarse`, anschließend `detailed`; ein Nacharbeitsjob kann direkt bei `detailed` beginnen. |
| `check-modifications` | Prüft gespeicherte Verzeichnisse auf Änderungen und setzt Rescan-Markierungen. |
| `rescan` | Prüft die obersten markierten Teilbäume grob und reiht erforderliche Detailjobs ein. |
| `rescan-after-days` | `firstscan` → `fixunreadable` → `workmodified`, innerhalb desselben neuen Jobs. |
| `binarycheck` | Vergleicht Duplikatkandidaten lokal oder hostübergreifend mit dem eigenen Handler. |
| `check`, `verify` | Im Schema zugelassene Tasks; vom CLI-Unterbefehl `check` zu unterscheiden. |

`firstscan`, `fixunreadable` und `workmodified` sind Stufen, keine eigenen Tasks
oder Jobs. Interne Startnamen wie `<age-rescan>` sind Platzhalter; der
Spezialhandler ermittelt die echten Root-Pfade aus dem Inventar.

| Status | Bedeutung |
| --- | --- |
| `insert` | Neu eingereiht. |
| `pending` | Wartet auf die nächste gespeicherte Stufe. |
| `active` | Von einem Server übernommen; ein Kindprozess arbeitet. |
| `paused` | Unterbrochen und zur Fortsetzung vorgesehen. |
| `stopped` | Explizit angehalten; keine automatische Fortsetzung. |
| `closed` | Erfolgreich abgeschlossen. |
| `error` | Fehlgeschlagen; technische Details stehen in `errmsg`, Benutzerhinweise in `usererror`. |
| `stale` | Als veraltet markierter Zustand; kein regulär wartender Job. |

Ein normaler Scan läuft so:

```text
insert/coarse → active/coarse → pending/detailed → active/detailed → closed/detailed
```

Die Konsolenmeldung `Job …: ok` nach `coarse` bedeutet, dass diese Stufe
abgeschlossen wurde. Danach kann derselbe Job noch `detailed` bearbeiten.

## Host-Zuordnung und Polling

`server` und `serverplus` bearbeiten Host 0. Nur `serverplus` übernimmt auch
Binarychecks. `localserver --hostid N` verwendet eine registrierte positive ID
und prüft Rechneridentität beziehungsweise `jobs.userhost`.

Server starten Worker mit der Python-Prozessstartmethode `spawn`; jeder Worker
öffnet seine DB-Verbindungen selbst. Das normale Polling-Intervall beträgt
120 Sekunden. Nach einer bearbeiteten Aufgabe erfolgt genau die nächste Abfrage
nach 10 Sekunden. Gibt es dann keine Arbeit, gilt wieder das normale Intervall.
Bei erkannten Netzwerkfehlern im Polling wartet der Server 30 Sekunden.

## Runids und Eigentümerjobs

Runids werden unter einer DB-Sperre global vergeben und in `jobsrunid` einem
Job zugeordnet. Ein Job kann mehrere Runids besitzen. `directories.runid`
verknüpft Inventarzeilen mit einem Lauf und darüber mit dem Eigentümerjob;
`finished` kennzeichnet abgeschlossene Teilbäume. Die altersabhängige
Nacharbeit übernimmt den betroffenen Root-Bestand unter ihrer aktuellen Runid.
Die Runid ist deshalb kein unveränderliches Erstellungsmerkmal.

Problematisch sind mehrere **Job-IDs** für denselben gespeicherten Root-Pfad,
nicht mehrere Runids desselben Jobs. `check` meldet solche Konflikte;
`server` und `serverplus` warnen. `localserver` bietet eine Bereinigung an und
startet bei verbleibenden Konflikten nicht. Die Prüfung umfasst den gespeicherten
Bestand und kann auch Konflikte eines anderen Hosts anzeigen.

Die Bereinigung behält den neuesten abgeschlossenen Job pro Root und bietet
ältere sowie geeignete Jobs ohne Endzeit als Kandidaten an. Jobs mit Status `active` werden dabei geschützt. Eine fehlende Endzeit allein ist kein
Beleg für einen verwaisten Job. Gelöscht werden nur die bestätigten Inventarzeilen
für Host, Root und Kandidatenjob; Zeilen anderer Jobs bleiben erhalten.

## Checkpoints und gruppierte Datenbankzugriffe

Jeder registrierte Host besitzt seine persistente Tabelle `tmp<HOSTID>_job_work`.
Der Name wird in `hosts.tabellenname` hinterlegt. Ein fehlender oder leerer Name
ist ein Fehler; `check` benennt die Tabelle nicht automatisch um.

Coarse- und Detailed-Stufen bearbeiten bis zu 1000 Arbeitseinträge je Batch.
Zum aktuellen Verzeichnis werden dessen Datensatz und direkte Kinder gruppiert
aus `directories` gelesen. Ergebnisse, neue Arbeit und Checkpoint-Änderungen
werden batchweise gespeichert. Ein unbestätigter Batch kann nach einem Abbruch
wiederholt werden.

| Queue-Zustand | Bedeutung |
| --- | --- |
| `pending` | Noch zu bearbeiten. |
| `expanded` | Selbst bearbeitet, aber Kinder noch nicht vollständig fertig. |
| `done` | Verzeichnis und Teilbaum abgeschlossen. |

Abgeschlossene Kinder können zu einem `done`-Marker des Elternteilbaums
zusammengefasst werden. `child_count` verhindert einen verfrühten Abschluss.
Ein kombinierter Hash darf nur entstehen, wenn lokale Daten und alle benötigten
Kinder vollständig sind; Coarse und Rescan erhalten keine scheinbar gültigen
Elternhashes über unvollständigen Teilbäumen.

## Unterbrechen und Fortsetzen

`--jobs-stop JOBID` setzt den Job auf `stopped`; ein aktiver Worker wird beendet.
`--jobs-start JOBID` setzt einen pausierten Job fort oder legt für einen gestoppten
Job einen neuen Datensatz mit übernommener Arbeit an.
`--jobs-reset-on-error` reiht einen ausgewählten Fehlerjob auf seiner gespeicherten
Stufe erneut ein. Bestätigte Checkpoints bleiben dabei nutzbar.

Ctrl-C oder SIGTERM unterbricht die Arbeit geordnet. Die lokale Wartefrist für
Aufräumen und Rollback beträgt höchstens 20 Sekunden; ein DB-seitiger Rollback
kann länger dauern. Details: [smartstart-stop-aenderungen.md](smartstart-stop-aenderungen.md).

Checkpoints sind kein Dateisystem-Snapshot. Änderungen an bereits erledigten
Teilbäumen werden durch eine Fortsetzung allein nicht automatisch erkannt;
dafür die Änderungs- oder Schnellprüfung verwenden.

## Berichte

Die HTML-Berichte und der interaktive Baum sind in
[duplikate-pruefen.md](duplikate-pruefen.md) beschrieben. Ein laufender
Fortschrittsbericht lädt regelmäßig neu; der abschließende Bericht erlaubt
Baumauswahl und Copypaste. Die reine Snapshot-Option `--doubles-tree` kann
vorhandene Ergebnisse ohne erneute Dateiprüfung darstellen.
