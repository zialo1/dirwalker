# Hashes und Teilbaum-Metadaten

Stand: Version 0.25.0. Der Detailscan berechnet Metadaten-Hashes aus Dateinamen,
Dateigrößen und Verzeichnisstruktur. Dateiinhalt wird dabei nicht vollständig
gelesen. Inhaltsvergleiche übernimmt der separate Binarycheck.

## Felder

| Feld | Bedeutung |
| --- | --- |
| `files_sha256` | SHA-256 über die eingeschlossenen regulären Dateien direkt im Verzeichnis. |
| `subdirs_sha256` | SHA-256 über Namen und kombinierte Hashes der direkten Kinder. |
| `combined_sha256` | SHA-256 aus Datei- und Unterverzeichnis-Hash. |
| `subtree_size_bytes` | Lokale Dateigröße plus Größen aller Nachkommen. |
| `subtree_structure` | Zahl der Nachkommen je relativer Tiefe mit abschließender Null. |

`file_list_sha256` spiegelt `files_sha256`; `subdirectory_hash` spiegelt
`combined_sha256`. Ein Feld `hash256_combined` gibt es im aktuellen Schema nicht;
der kombinierte Wert heißt `combined_sha256`.

## Direkte Dateien

Der Scan sortiert die eingeschlossenen Einträge nach Namen und berücksichtigt
reguläre Dateien ohne Folgen von Symlinks. Namen werden mit UTF-8 und
`surrogateescape` kodiert. Für jede erfolgreich erfasste Datei geht folgender
Token in den Digest ein:

```text
F NUL Dateiname NUL dezimale-Dateigröße LF
```

`NUL` bedeutet Byte `0x00`, `LF` Byte `0x0a`. **Die mtime ist kein Bestandteil
dieses Hashes.** Gleiche Namen und Größen ergeben denselben lokalen
Metadaten-Hash, auch bei unterschiedlichen Änderungszeiten oder Inhalten.
Eine verschwundene Datei erzeugt `MISSING NUL Dateiname LF` und macht den Scan
unvollständig. Berechtigungsfehler werden gemeldet und ebenfalls als
Unvollständigkeit behandelt. Leere reguläre Dateien tragen einen Token mit
Größe 0 bei; ihre Lesbarkeit wird für die Nullbyte-Klassifizierung geprüft.

`files_lastwrite` speichert unabhängig vom Digest die neueste mtime relevanter
direkter Dateien. `time_inode_changed` speichert die Directory-mtime. Diese Werte
dienen der Änderungserkennung, nicht der Berechnung des Metadaten-Hashes.

## Unterverzeichnisse und kombinierter Wert

Die Berechnung arbeitet von den Blättern zur Wurzel. Direkte Kinder werden nach
ihrem binären Namen sortiert; je Kind wird angehängt:

```text
DIR NUL Kindname NUL Kind-combined_sha256-als-ASCII NUL
```

Der daraus entstehende Digest ist `subdirs_sha256`. Ohne Kinder entspricht er
dem SHA-256-Digest der leeren Bytefolge. Der kombinierte Digest verarbeitet:

```text
FILES NUL files_sha256-als-ASCII NUL SUBDIRS NUL subdirs_sha256-als-ASCII NUL
```

Ein Elternhash ist erst gültig, wenn die benötigten lokalen Werte und alle
Kinder vollständig sind. Fehlende Datei-Hashes, fehlende Kinder oder unfertige
Kinder dürfen keinen scheinbar gültigen kombinierten Hash ergeben. Diese
Bereitschaft wird auch im Grobscan und beim Rescan berücksichtigt.

Ändert sich ein lokaler Bestand, werden dessen Hashes und die davon abhängigen
Aggregate der Vorfahren ungültig. Unveränderte Zweige können ihre Hashes behalten.
Siehe [rescan.md](rescan.md) und [age-rescan.md](age-rescan.md).

## Größe, Struktur und Nullbyte-Teilbäume

```text
subtree_size_bytes = localfilesize + Summe(child.subtree_size_bytes)
```

`subtree_structure` zählt Nachkommen je Entfernung vom aktuellen Verzeichnis.
Die Wurzel selbst wird nicht mitgezählt; die letzte Null beendet die Folge.
`2-4-0` bedeutet zwei direkte Kinder, vier Enkel und keine weiteren Ebenen.

`zero_content_only=TRUE` setzt einen vollständig bekannten, lesbaren Teilbaum
mit ausschließlich Nullbyte-Dateien oder leeren Verzeichnissen voraus.
Spezialeinträge, Symlinks, fehlende Werte oder Lesefehler verhindern das Flag.
Eine Gesamtgröße von null allein reicht nicht. Diese Bäume werden aus den
Duplikatkandidaten gefiltert.

## Pfadidentität und binäre Prüfung

`path_hash` ist davon unabhängig: ein 64-Bit-BLAKE2b-Fingerabdruck des
normalisierten Pfads, gespeichert als 16 kleine Hex-Zeichen. Pfadvergleiche
prüfen bei Bedarf auch den vollständigen Pfad; der Hash beschreibt keinen Inhalt.

`hashed_at` bezeichnet den Abschluss einer binären Prüfung, nicht die Erstellung
der oben beschriebenen Metadaten-Hashes. Ein frisches konkretes Vergleichspaar
kann beim erneuten Einreihen übersprungen werden; siehe
[binarycheck-freshness.md](binarycheck-freshness.md).
