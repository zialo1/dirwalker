# Bereits geprüfte Vergleiche

`--doubles-letcheck` prüft vor dem Einreihen die vorhandenen Ergebnisse und
offenen Vergleiche. Es werden nur Kandidaten mit vollständigen Datei-,
Unterverzeichnis-, Struktur- und kombinierten Hashdaten berücksichtigt.

`directories.hashed_at` enthält den UTC-Zeitpunkt einer abgeschlossenen
binären Prüfung (`equal` oder `different`). Ein Verzeichnis ist seit seiner
letzten Inventarisierung geprüft, wenn `hashed_at >= scanned_at` gilt.
Unlesbare oder fehlgeschlagene Vergleiche setzen diesen Zeitstempel nicht.
Ein neuer Scan während eines Vergleichs verhindert das Setzen des Zeitstempels.

Zusätzlich muss für genau dieses Verzeichnispaar ein erfolgreich abgeschlossenes
Ergebnis vorliegen, dessen Prüfung nach beiden letzten Scans begonnen hat.
Ein neuer Vergleichspartner wird dadurch weiterhin geprüft. Noch eingereihte
Vergleiche werden nicht erneut angelegt. Hosts ohne verbleibende Vergleiche
erhalten keinen leeren Job.

Für unveränderte, bereits geprüfte Paare zeigt der Befehl die Anzahl je Host.
Er fragt zunächst insgesamt und anschließend je Host mit `[yN]`, ob diese
Vergleiche erneut eingereiht werden sollen. Nur `y` stimmt zu; die Eingabetaste
oder ein Abbruch überspringt die Wiederholung.

Bei einer bestehenden Datenbank ist vor Verwendung der neuen Version die
Migration [migrate_directories_hashed_at.sql](../sql/migrate_directories_hashed_at.sql) anzuwenden. Neue Datenbanken
erhalten die Spalte durch `init`. Frische historische Vergleichsergebnisse
füllen die neue Spalte beim Vorbereiten der nächsten Vergleiche nach.
