# Dokumentation zu Dirwalker 0.25.0

Stand: 5. November 2026. Die Anleitungen beziehen sich auf `dirwalker.py`
beziehungsweise `version25.py`. Auf dem Server kann dieselbe Anwendung als
`serverfiles.py` installiert sein. Die Beispiele verwenden `python3 dirwalker.py`
aus dem Projektverzeichnis und eine aktivierte Python-Umgebung.

| Thema | Anleitung |
| --- | --- |
| Einrichtung, Scans und tägliche Bedienung | [workflow.md](workflow.md) |
| Jobs, Stufen, Fortsetzung und Serververhalten | [modus.md](modus.md) |
| Tabellen, Zeitstempel und Schemaänderungen | [database_tbls.md](database_tbls.md) |
| Änderungskontrolle und markierte Teilbäume | [rescan.md](rescan.md) |
| Altersabhängige Schnellprüfung | [age-rescan.md](age-rescan.md) |
| Duplikatkandidaten, lokale und entfernte Vergleiche | [duplikate-pruefen.md](duplikate-pruefen.md) |
| Bereits geprüfte Vergleiche und `hashed_at` | [binarycheck-freshness.md](binarycheck-freshness.md) |
| Aufbau der Metadaten-Hashes | [hashlogic.md](hashlogic.md) |
| Verbindungen, Sperren, IPC und Fehlerdiagnose | [database_solutions.md](database_solutions.md) |
| Smartstart und geordnetes Stoppen | [smartstart-stop-aenderungen.md](smartstart-stop-aenderungen.md) |
| Lesende SQL-Diagnosebeispiele | [sqlqueries.txt](sqlqueries.txt) |

Die vollständige Befehlsliste liefert `python3 dirwalker.py --help`, den
Duplikatablauf erklärt `--doubles-help`. `check` ist ein Unterbefehl ohne `--`;
Verwaltungsoptionen wie `--jobs-insert` oder `--dirs-summary-data` haben `--`.

## Datenbank und Aktualisierung

Die Verbindung verwendet `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER` und
`DB_PASSWORD`; eine Vorlage steht in [shellenv_example.sh](shellenv_example.sh).
Zeitstempel in der Datenbank sind UTC. Zugangsdaten nicht ins Repository übernehmen.

Tabellendefinitionen, Constraints, Strukturprüfung und SQL-Zeitlimit-Hilfen liegen
in `sqlstructure.py`. Bestehende Tabellen werden nicht durch ein allgemeines
`init` automatisch auf jeden neuen Spaltenstand migriert. Passende Migrationen
stehen unter [../sql](../sql); die manuelle altersabhängige Migration ist in
[age-rescan.md](age-rescan.md#bestehende-datenbank-aktualisieren) beschrieben.
Vor Schemaänderungen alle Worker stoppen, die Datenbank sichern und danach
`python3 dirwalker.py check` ausführen. `init`, `--dropall` und
`--drop-directories` können Daten zurücksetzen und sind keine Reparatur für
hängende Verbindungen.

## Entwicklung und Prüfungen

[skills.md](skills.md) beschreibt die Python-Konventionen,
[skills_3dplots.md](skills_3dplots.md) die Vorgaben für 3D-Berichte.
Offline-Prüfungen: `make test-dryrun`; Testsuite: `make test`.
Ein echter Datenbanktest ist separat über `make test-integration` aktivierbar.

## Zuordnung der Tests

Die aktuelle Testsuite besteht aus genau sieben Dateien, jeweils passend zu
einem Produktionsmodul:

| Produktionsmodul | Testdatei |
| --- | --- |
| `version25.py` (Laufzeit von `dirwalker.py`/`serverfiles.py`) | `test_version25.py` |
| `DataBaseCore.py` | `test_DataBaseCore.py` |
| `DataBaseCLI.py` | `test_DataBaseCLI.py` |
| `sqlstructure.py` | `test_sqlstructure.py` |
| `client_operations.py` | `test_client_operations.py` |
| `server_operations.py` | `test_server_operations.py` |
| `doubles_output.py` | `test_doubles_output.py` |

Die Struktur- und Releaseprüfungen liegen in `test_sqlstructure.py`.
Weiterhin gültige Regressionstests früherer Versionen prüfen die aktuelle
Version; archivierte Testkopien wurden entfernt.
