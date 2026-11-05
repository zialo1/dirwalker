# Smartstart, Server-Stopp und lokale Sperrdateien

Stand: Version 0.25.0.

## Lokalen Server vorbereitet starten

```sh
python3 dirwalker.py --smartlocalserver --hostid 1
```

Ohne `--hostid` versucht der Client, die passende positive Host-ID zu ermitteln.
Bei mehreren passenden Registrierungen wird nicht geraten. Host 0 wird dafür
nicht verwendet.

Smartstart prüft `SHOW FULL PROCESSLIST`. Eindeutig zugeordnete Verbindungen
können nach `[yN]` mit `KILL CONNECTION` beendet werden; danach wartet der Client
bis zu 20 Sekunden auf ihr Verschwinden. Bei unklarer Zuordnung zeigt er nur
manuelle Hinweise und startet nicht. Enter oder `n` lehnt ab.

Danach bietet er Fehlerjobs der gewählten Host-ID mit `[yN]` zum Zurücksetzen an.
Dafür verwendet er dieselbe Verwaltungsfunktion wie `--jobs-reset-on-error`,
mit zusätzlichem Host-Schutz. Abgelehnte oder fehlgeschlagene nötige Resets
verhindern den Start. Tabellen werden dabei nicht gelöscht oder geleert.

## Geordnetes Beenden

`server`, `serverplus` und `localserver` geben bei Ctrl-C eine Stop-Meldung aus,
unterbrechen den Scan-Prozess und versuchen parallel, zugeordnete DB-Verbindungen
zu beenden. Ein Watchdog begrenzt das lokale Warten auf 20 Sekunden. Bei
Fristablauf wird die Scan-Prozessgruppe einschließlich Priority-Kindern beendet.
Ein MariaDB-Rollback kann auf dem Datenbankserver länger dauern.

SQL-Abfragen der Serverprozesse tragen zur Zuordnung einen Kommentar mit
Rechner, Host-ID, Prozesskennung und Servergruppe. Nicht sicher zuordenbare
Abfragen werden nicht automatisch beendet.

## Lokale Lockdateien

Der Server hält einen Betriebssystem-Lock für die Kombination aus DB-Host,
Port, Datenbankname und Host-ID. Unterschiedliche Datenbanken oder Host-IDs
haben unabhängige Sperren. Das gilt lokal auf einem Rechner.

Nach dem Prozessende ist der Betriebssystem-Lock freigegeben. Die Datei mit
der historischen PID kann trotzdem bestehen bleiben; sie beweist keinen
hängenden oder noch aktiven Server.

Wenn alle Server und Localserver gestoppt sind:

```sh
python3 dirwalker.py --resetlocks
python3 dirwalker.py --hostid 1 --resetlocks
```

Der Befehl vertraut dieser Voraussetzung und löscht die ausgewählten lokalen
Lockdateien auch dann, wenn sie noch gehalten wären. Er meldet je Datei Pfad,
Gültigkeitsbereich, gespeicherte historische PID und Ergebnis. Er prüft nicht,
ob ein Server noch läuft, und beendet keine DB-Verbindungen. Beim normalen
Serverstart wird zusätzlich geprüft, ob die gesperrte Datei noch dieselbe
Datei ist; dadurch werden veraltete Datei-Inodes nach einem Reset erkannt.
