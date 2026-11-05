# Änderungskontrolle und markierter Rescan

Diese beiden Tasks sind vom altersabhängigen Ablauf in
[age-rescan.md](age-rescan.md) zu unterscheiden. Alle Optionen reihen Jobs ein;
`server`, `serverplus` oder der zuständige `localserver` führt sie aus.

## Änderungen markieren

```sh
python3 dirwalker.py --check-modifications
python3 dirwalker.py --hostid 1 --check-modifications
```

Ohne Host-ID entsteht ein Prüfjob für jeden registrierten Host.
Die Prüfung berücksichtigt gespeicherte Job-Ausschlüsse und absolute
Live-Ausschlüsse des Hosts.

Zuerst wird `os.stat(...).st_mtime` des Verzeichnisses mit
`directories.time_inode_changed` verglichen. Trotz des Feldnamens ist das die
**Directory-mtime**, nicht `st_ctime`. Bei Abweichung wird nur dieses Verzeichnis
mit `subfolders_excluded=TRUE` in `tmp_modified` vermerkt; dessen Inhalt wird in
diesem Prüfschritt nicht aufgelistet. Andere inventarisierte Verzeichnisse werden
weiterhin als eigene Datensätze geprüft.

Bei unveränderter Directory-mtime bestimmt der Worker die neueste mtime direkt
enthaltener, eingeschlossener regulärer Dateien, ohne Symlinks zu verfolgen.
Dieser Wert wird mit `files_lastwrite` verglichen. Ohne passende Dateien ist er
NULL. Ein abweichender Wert oder ein während der Auflistung verschwundener
Eintrag löst ebenfalls eine Markierung aus.

Fehlt ein Verzeichnis oder ist es kein Verzeichnis mehr, sucht die Prüfung nach
einem tatsächlich lesbaren, inventarisierten Elternverzeichnis. Ein ebenfalls
verschwundener Elternpfad wird übersprungen. Ohne geeigneten Vorfahren schlägt
der Job fehl. Berechtigungsfehler in dieser Änderungskontrolle werden weiterhin
als Fehler gemeldet; die unten beschriebene Drei-Versuche-Regel gehört zum Rescan.

Am Ende markiert ein SQL-Join die passenden Inventarzeilen mit
`rescan_from_here=TRUE`. Die Prüfung verwendet Batch 0 in `tmp_modified`.
Zu Beginn entfernt sie ausschließlich alte Batch-0-Zeilen desselben Hosts;
sie löscht die gemeinsame Tabelle nicht.

## Markierte Teilbäume nacharbeiten

```sh
python3 dirwalker.py --hostid 1 --directories-rescan-marked
```

Der Task `rescan` wählt die obersten markierten Pfade: Ein markierter Nachkomme
eines bereits markierten Vorfahren wird nicht als zusätzlicher Einstieg verwendet.
Von diesen Einstiegen aus werden die eingeschlossenen Teilbäume grob gelesen.
Aktuelle globale Ausschlussnamen gelten zusätzlich zu den Job- und Live-Ausschlüssen.

Der Worker aktualisiert `files_lastwrite` und `time_inode_changed`, ergänzt neue
Verzeichnisse und entfernt verschwundene Inventarzweige. Unveränderte Zweige
behalten ihre Hashes. Ändern sich lokale Dateizeitstempel oder die Kinderliste,
werden betroffene lokale Hashes und die abhängigen Aggregate der Vorfahren
ungültig. Für die betroffenen Inventarwurzeln werden Detailjobs eingereiht.

Bei nicht lesbaren Verzeichnissen steigt `read_failures`. Die ersten beiden
Fehler erhalten den inventarisierten Teilbaum und seine Rescan-Markierung.
Beim dritten aufeinanderfolgenden Fehler werden die inventarisierten Nachkommen
abgeschnitten; der betroffene Grenz-Datensatz bleibt erhalten. Ein erfolgreiches
Lesen setzt den Zähler zurück. Andere Zweige können weiterbearbeitet werden.
Diese Regel unterscheidet sich von `fixunreadable` beim Alters-Rescan, das nach
drei Fehlversuchen auch den betroffenen Datensatz löscht.

Verarbeitete Markierungen und die zugehörigen Batch-0-Zeilen in `tmp_modified`
werden beim erfolgreichen Abschluss entfernt. Für erneut zu prüfende, blockierte
Zweige bleiben die Informationen bestehen. Andere Hosts und Wartungsbatches
werden nicht gelöscht. Alle Löschungen betreffen nur die Datenbank.
