# Datenbankzugriff: Probleme, Lösungen, IPC und Sperren

Stand: 05.11.2026, Implementierung 0.25.0 in `version25.py` und den
zugehörigen Modulen. Beschrieben wird der vorhandene Code einschließlich
seiner Grenzen. Die Beispiele beziehen sich auf MariaDB 10.11/InnoDB.

## 1. Verbindungen und Verantwortlichkeiten

`DataBaseCore.py` bündelt Treiberwahl, Verbindungsaufbau, Prozesskennungen,
SQL-Kennzeichnung und das gezielte Bereinigen von Serververbindungen.
Jeder Aufruf von `connect()` öffnet eine eigene Verbindung mit
`autocommit=False`; es gibt keinen gemeinsamen Verbindungspool.
`connect_database()` ist ein Alias. Die separate Kontrollverbindung für
`SHOW FULL PROCESSLIST` und `KILL CONNECTION` verwendet Autocommit und
fünf Sekunden Verbindungsaufbau-Timeout.

`server_operations.py` steuert Serverstart, Job-Kindprozesse, Signale und
Stop-Fristen. Die Scan- und Binarycheck-Operationen liegen im Laufzeitmodul
`version25.py`. `sqlstructure.py` enthält Tabellendefinitionen, Constraints, Strukturprüfung
und SQL-Zeitlimit-Hilfen. `DataBaseCLI.py` enthält Verwaltungsoperationen;
`client_operations.py` enthält Argumentauswertung und CLI-Abläufe.

Prozesse werden mit `multiprocessing.get_context('spawn')` gestartet.
Sie öffnen ihre Datenbankverbindungen selbst. Eine bereits offene
Verbindung oder ein Cursor wird nicht an den Kindprozess weitergegeben.
Verbindungen gehören zu einer konkreten Operation; Commit und Rollback
liegen bei deren Aufrufer. Cursor und Verbindungen werden anschließend
geschlossen.

### Tabellen und Lebenszyklus

| Tabelle | Zweck |
| --- | --- |
| `hosts` | Registrierung von Benutzer, Rechner, Home und Host-ID |
| `jobs` | Aktueller Status, Stufe und Runid des Jobs |
| `jobsrunid` | Laufhistorie mit PID, Rechner und Prozessstartkennung |
| `directories` | Hostbezogene Verzeichnisdaten und Teilbaumstatistiken |
| `tmp<HOSTID>_job_work` | Persistente Arbeitsliste und Checkpoints pro Host |
| `tmp_doubles_letchecked` | Angeforderte Vergleichspaare und Binarycheck-Ergebnisse |
| `tmp_highpriority` | Prioritätshinweise als `(hostid, dirid)` |
| `tmp_modified` | Batch- und hostgetrennte Änderungsberichte |
| `tmp_remotedoubles` | Hostübergreifende Prüfanfragen und Antworten |

Die Tabellen mit `tmp` im Namen sind im regulären Schema gemeinsame,
persistente InnoDB-Tabellen; der Name macht sie nicht zu einer
verbindungslokalen SQL-`TEMPORARY TABLE`.

## 2. Beobachtete Störungen und ihre Ursachen

### 2.1 Lange Elternreparatur blockiert andere Schreiber

Die frühere Elternreparatur verwendete einen großen Self-Join:
`UPDATE directories child LEFT JOIN directories parent ...`, mit einem
binären Vergleich des vollständigen Kindpfades gegen einen zusammengesetzten
Elternpfad. Dieser Schreibzugriff lief in der beobachteten Installation sehr
lange; andere Coarse- und Detailed-Batches meldeten Lock-Wartefehler.
Ein laufendes `UPDATE` kann Sperren auch halten, wenn der Client bereits
beendet ist und MariaDB das Verbindungsende noch nicht verarbeitet hat.

Die Reparatur verwendet jetzt `_repair_directory_parents()`:

1. Nur die Verzeichnisse der betreffenden Host-ID und Scan-Wurzel lesen.
2. Eine exakte Pfad-zu-Dirid-Zuordnung im Python-Speicher aufbauen.
3. Den Soll-Elternwert mit `os.path.dirname()` bestimmen; die Wurzel erhält 0.
4. Nur tatsächlich abweichende Elternwerte aktualisieren.
5. Je höchstens 200 Änderungen per Dirid, Host-ID und Wurzel schreiben und
   committen. Die vorherige Lesetransaktion wird vorher beendet.

Der alte große Join entfällt. Bereits korrekte Daten benötigen keine
Eltern-Updates. Nach einer Unterbrechung bleiben die bereits bestätigten
Reparatur-Batches gültig und ein erneuter Lauf kann die Arbeit fortsetzen.
Die Pfadzuordnung benötigt weiterhin Speicher proportional zur gelesenen
Verzeichnisanzahl.

Bei der Reparatur alter `root_path`-Zuordnungen werden ebenfalls höchstens
200 Datensätze je Transaktion geändert. Der Wurzeleintrag wird zuletzt
umgestellt: Solange die Migration unvollständig ist, bleibt sie beim
Neustart als notwendig erkennbar.

### 2.2 Lock-Wartezeit und Deadlock sind verschiedene Fehler

| Fehler | Bedeutung | Behandlung im Code |
| --- | --- | --- |
| 1205 | Eine benötigte Sperre wurde nicht rechtzeitig frei | Ganze Operation nach Rollback erneut versuchen, sofern der Retry-Wrapper verwendet wird |
| 1213 | MariaDB hat eine Transaktion als Deadlock-Opfer abgebrochen | Ganze Operation mit neuer Transaktion wiederholen |
| 2013 | Verbindung während einer Abfrage verloren | Fehler melden; kein automatischer Lock-Retry |
| 1054 mit `pid` | Laufzeitcode und Schema passen nicht zusammen | Migration `migrate_jobsrunid_pid.sql` anwenden |

`_retry_on_lock_error()` behandelt ausschließlich 1205 und 1213. Es gibt
höchstens neun Versuche, also acht Wiederholungen. Die Wartezeiten sind:

```text
0,33 s → 1 s → 3 s → 9 s → 27 s → 81 s → 243 s → 729 s
```

Allein diese Pausen ergeben 1093,33 Sekunden, also 18 Minuten 13 Sekunden.
Dazu kommt die Dauer jedes Versuchs. Bei neun ausgeschöpften
Lock-Wartezeiten von jeweils 50 Sekunden ergeben sich ungefähr 25 Minuten
43 Sekunden zuzüglich Verarbeitung. Wiederholungen beseitigen die
ursprüngliche blockierende Abfrage nicht.

Der Wrapper wird unter anderem für Coarse-/Detailed-Abschlüsse,
Statusänderungen und Binarycheck-Schreiboperationen eingesetzt. Nicht jede
Datenbankoperation des Programms besitzt diesen Wrapper. Beim Fehler 1205
muss der Aufrufer die gesamte Transaktion zurückrollen; ein fehlgeschlagenes
Statement allein ist kein Beleg für eine vollständig beendete Transaktion.

### 2.3 Rollback verdeckt den ursprünglichen Fehler

Nach einem Verbindungsverlust kann auch `connection.rollback()` scheitern.
Dann soll der eigentliche Abfragefehler erhalten bleiben.
`_rollback_preserving_error()` protokolliert einen zusätzlichen
Rollback-Fehler. `_close_preserving_error()` schützt entsprechend vor einem
zweiten Fehler beim Schließen, wenn bereits ein ursprünglicher Fehler aktiv
ist. Diese Hilfen werden insbesondere in `prepare_stage_work()` verwendet;
sie sind keine pauschale Umstellung aller Datenbankoperationen.

## 3. Sperrstrategien und ihre Gültigkeitsbereiche

| Mechanismus | Geschützter Bereich | Grenze |
| --- | --- | --- |
| `fcntl.flock` auf Server-Sperrdatei | Eine Serverinstanz je Rechner, DB-Endpunkt, Datenbank und Host-ID | Kein verteilter Lock zwischen verschiedenen Rechnern |
| MariaDB `GET_LOCK` für Runids | Vergabe von globalem `MAX(runid)+1` und zugehörige Änderungen | Verbindungsbezogener Advisory-Lock, kein Ersatz für Zeilensperren |
| MariaDB `GET_LOCK` für Hosts | Gleichzeitige Host-Registrierung und Reset | Schützt diesen Verwaltungsablauf, nicht sämtliche Serverarbeit |
| `SELECT ... FOR UPDATE` | Jobauswahl und bestimmte Statusänderungen | Sperren bestehen bis zum Transaktionsende |
| InnoDB-Schreibsperren | Konsistenz bei `UPDATE`, `INSERT`, Upsert und Checkpoints | Andere Schreiber können trotzdem warten oder Deadlocks erzeugen |
| `multiprocessing.Lock` | Ergebnis-Schreiben eines normalen Binarycheck-Workers und seines Priority-Kindes | Gilt nur für dieses Prozesspaar |
| Normales `SELECT` mit MVCC | Lesen eines konsistenten Datenstands | Metadatensperren und Ressourcenverbrauch bleiben möglich |

### 3.1 Genau eine Serverinstanz pro lokalem Gültigkeitsbereich

`_server_instance_lock()` bildet einen Schlüssel aus Datenbankhost, Port,
Datenbankname und Host-ID und hält darauf `flock(LOCK_EX | LOCK_NB)`.
`server` und `serverplus` verwenden Host-ID 0; `localserver --hostid HOSTID` und
`--smartlocalserver [--hostid HOSTID]` eine positive Host-ID.

Damit können auf demselben Rechner nicht zwei Servermodi für dieselbe
Kombination laufen. Unterschiedliche Host-IDs sind erlaubt. Die Sperrdatei
bleibt nach dem Ende regulär erhalten. `--resetlocks` darf unter der Voraussetzung
gestoppter Server die lokalen Dateien löschen; es meldet pro Datei deren
Gültigkeitsbereich und historische PID. Beim Sperrerwerb prüft der Server
den Datei-Inode erneut, damit er nicht mit einer inzwischen gelöschten Datei startet. Entscheidend ist der Betriebssystem-Lock, nicht
der möglicherweise alte PID-Text. Ein Prozessende gibt den Lock frei.
Ein solcher lokaler Lock garantiert keine globale Einzelinstanz auf
mehreren Rechnern.

### 3.2 Runid- und Job-Synchronisierung

`_acquire_runid()` verwendet `GET_LOCK(name, 30)`. Unter diesem Lock wird die
nächste Runid aus `jobs` und `jobsrunid` bestimmt. `claim_job()` sperrt den
ältesten passenden Job zusätzlich mit `LIMIT 1 FOR UPDATE`, aktiviert ihn,
setzt seine Runid und schreibt den Historieneintrag in derselben
Transaktion. Anschließend wird committet und der Advisory-Lock explizit
freigegeben. Ein Commit allein gibt diesen Advisory-Lock nicht frei;
auch ein Verbindungsende gibt ihn frei.

Alle neuen `jobsrunid`-Einträge entstehen zentral durch
`DataBaseCore.write_run_record()`. PID, `process_host` und `process_start`
sind dabei verpflichtende Prozessinformationen. Alte NULL-Werte lassen sich
nicht nachträglich zuverlässig rekonstruieren. Eine PID allein genügt
nicht, weil das Betriebssystem sie wiederverwenden kann. Unter Linux wird
eine Startkennung aus `/proc` samt Bootkennung verwendet, unter macOS aus
`ps`. Zombieprozesse gelten nicht als lebend.

Vor dem Serverstart und vor der Priority-Registrierung wird anhand dieser
Kennungen geprüft, ob ein passender lokaler Priority-Worker lebt. Die
Registrierung wird unter dem Runid-Lock erneut geprüft. Der Priority-Lauf
erhält eine eigene Runid in `jobsrunid`; er überschreibt nicht die Runid des
übergeordneten Jobs in `jobs`.

### 3.3 Lesende und schreibende Scanphasen

Normale `SELECT`-Abfragen unter `REPEATABLE READ` oder `READ COMMITTED`
verwenden InnoDB-MVCC ohne normale Zeilensperren. Für die rekursiven
Leseabfragen und die anfänglichen Binarycheck-Leseabfragen wird kein
`FOR UPDATE` benötigt. `SERIALIZABLE` verhält sich anders; das Programm
erzwingt selbst keine andere Isolationsstufe. Metadatensperren können
weiterhin DDL wie `ALTER TABLE` oder `DROP TABLE` aufhalten.
[MariaDB: Transaktionsisolation](https://mariadb.com/docs/server/reference/sql-statements/administrative-sql-statements/set-commands/set-transaction)

`claim_work_batch()` liest höchstens 1000 ausstehende Arbeitslisteneinträge
und committet die Lesetransaktion. Es sperrt oder reserviert diese Einträge
nicht mit `FOR UPDATE`. Deshalb darf für denselben Job nicht beliebig ein
zweiter Scan-Worker gestartet werden. Die Serversteuerung und Jobübernahme
sind Teil dieser Annahme.

Coarse- und Detailed-Ergebnisse werden mit ihren Arbeitslisten-Checkpoints
in Batches abgeschlossen. Bei `rebuild_subtree_statistics()` liest ein
separater unbuffered Cursor den Baum, während eine andere Verbindung die
berechneten Werte schreibt; die Schreibseite committet nach jeweils 1000
gelesenen Knoten. Die neue Detailed-Arbeitsliste wird zwar in
`fetchmany(1000)`-Blöcken aufgebaut, aber erst am Ende ihrer Vorbereitung
committet. Ein Lese- oder Fetch-Batch bedeutet also nicht automatisch eine
kurze Schreibtransaktion.

## 4. Vollständiges Binarycheck-IPC-Design

### 4.1 Prozesshierarchie

```text
Server-Hauptprozess: Polling, Jobübernahme, Status und Stop-Steuerung
└── Scan-/Binarycheck-Worker für einen Job
    └── optionaler Priority-Binarycheck-Worker mit eigener Runid
```

Das im Folgenden als „normaler Worker“ bezeichnete Gegenüber des
Priority-Prozesses ist der Binarycheck-Job-Worker, nicht der äußere
Server-Hauptprozess. Der Server-Hauptprozess darf ohne Kinder laufen,
beispielsweise während des Polling-Wartens ohne bearbeitbaren Job.

`Binarycheck.loop()` führt den normalen Vergleichslauf aus, wartet vor dem
regulären Abschluss auf delegierte Arbeit und räumt das Kind in `finally`
auf. Pro `Binarycheck`-Instanz wird höchstens ein Priority-Kind gestartet;
es wird für weitere Zuweisungen wiederverwendet. Ein späterer Job kann eine
neue Binarycheck-Instanz und damit ein neues Kind erhalten.

### 4.2 Ermitteln und Zuweisen von Prioritätsarbeit

Der normale Worker prüft `tmp_highpriority` vor dem nächsten Vergleich,
frühestens nach erneut abgelaufenen 120 Sekunden. Das ist kein unabhängiger
Timer: Ein langer laufender Dateivergleich wird nicht unterbrochen und
kann die nächste Prüfung verzögern.

`SHOW COLUMNS` prüft die Existenz und vorhandenen Spalten. `dirid` ist
notwendig; nach `hostid` und `jobid` wird gefiltert, soweit die jeweilige
Spalte vorhanden ist. Die aktuelle reguläre Tabelle besitzt `hostid` und
`dirid`. Fehler 1146, auch bei gleichzeitigem Entfernen der Tabelle, gilt
als fehlende Prioritätstabelle. Andere Fehler werden nicht verschluckt.

Für die Zuweisung werden nur noch ausstehende Vergleichspaare des aktiven
Binarycheck-Jobs gewählt, deren linke oder rechte Dirid priorisiert ist.
Die IPC-Arbeitsliste enthält `(left_dirid, right_dirid)`-Paare, keine
beliebigen Inodes und keine vollständigen Ergebnisdatensätze. Diese Paare
werden aus der normalen Warteschlange entfernt. Das Kind lädt
Jobinformationen, Pfade und Vergleiche selbst aus der Datenbank und filtert
auf die zugewiesenen Paare.

Ohne Tabelle oder passende Paare bleibt das normale Verhalten bestehen.
Ein Eintrag allein startet weder einen Coarse-/Detailed-Priority-Scan noch
einen neuen Binarycheck-Job. Die Worker schreiben keine Ergebnisse nach
`tmp_highpriority` und löschen die Hinweise auch nicht als Quittierung.
`--insert-priority HOSTID DIRID ...` prüft die vorhandenen Verzeichnisse
dieses Hosts und trägt die Hinweise atomar und idempotent ein.

### 4.3 Transport und Nachrichten

Verwendet werden eine bidirektionale `multiprocessing.Pipe(duplex=True)`
und ein gemeinsames `multiprocessing.Lock` aus demselben Spawn-Kontext.
Es gibt hierfür keinen TCP-Port, keine Message-Queue und keine gemeinsam
genutzte Datenbankverbindung.

| Nachricht | Richtung | Bedeutung |
| --- | --- | --- |
| `('runid', runid)` | normal → Priority | Erst nach erfolgreicher Registrierung darf das Kind arbeiten |
| `('work', pairs)` | normal → Priority | Nächste zu bearbeitende Paarliste |
| `('write_request', token)` | Priority → normal | Fertiger Batch soll geschrieben werden |
| `('waiting', token, deadline)` | normal → Priority | Normaler Worker pausiert; monotone Frist für Beginn des Schreibens |
| `('reset', token)` | normal → Priority | Das angekündigte 10-Sekunden-Fenster ist beendet |
| `('committed', token)` | Priority → normal | Kind hat seinen Batch bestätigt; aktuell keine gesonderte Auswertung beim Empfänger |
| `('idle',)` | Priority → normal | Zugewiesene Liste vollständig bearbeitet und geschrieben |
| `('finish',)` | normal → Priority | Keine weitere Arbeit; geordnet beenden |
| `('error', traceback)` | Priority → normal | Fehler des Kindes; im normalen Worker als Fehler weitergeben |

Die Nachrichten werden als Python-Objekte über die Pipe übertragen.
Nur die zusammengehörigen lokalen Prozesse verwenden dieses Protokoll.

### 4.4 Batchbildung und Schreibfreigabe

Ein Ergebnisdatensatz beschreibt ein Vergleichspaar mit Job-ID, Host-ID,
beiden Dirids, Runid, Ergebnisstatus, Verifikationsmethode, gegebenenfalls
Quellpaar, Fehlertext und zusätzlich bestätigten Unterverzeichnispaaren.
Der Priority-Worker sammelt diese Ergebnisse zunächst im Speicher.

Ein Batch wird fällig, sobald nach einem fertigen Vergleich mindestens eine
Bedingung zutrifft:

- Mindestens 20 Vergleichsergebnisse sind vorhanden.
- Seit Beginn dieses Batches sind mindestens 600 Sekunden vergangen.
- Die aktuelle zugewiesene Liste ist vollständig abgearbeitet.

Somit wird auch ein kleiner Restbatch geschrieben. Die 600 Sekunden werden
zwischen fertigen Vergleichen geprüft; ein einzelner noch laufender
Vergleich wird dadurch nicht abgebrochen. Ein Vergleichsergebnis kann
zusätzliche bestätigte Unterverzeichnispaare erzeugen: 20 Ergebnisse sind
keine Obergrenze von 20 betroffenen SQL-Zeilen.

```mermaid
sequenceDiagram
    participant N as Normaler Binarycheck-Worker
    participant P as Priority-Worker
    participant D as MariaDB
    N->>P: runid, anschließend work(Paare)
    Note over P: Vergleichen; Ergebnisse sammeln
    P->>N: write_request(token)
    Note over N: IPC zwischen Vergleichen / vor nächstem Schreiben bedienen
    N->>P: waiting(token, jetzt + 10 s)
    Note over N: 10 Sekunden pausieren
    Note over P: Alle 5 Sekunden auf Antwort prüfen
    P->>P: gemeinsames Lock erwerben; Frist prüfen
    P->>D: Batch in eigener Transaktion schreiben und committen
    P->>N: committed(token)
    N->>P: reset(token)
    Note over N,P: Normales Schreiben benötigt dasselbe Lock
    P->>N: idle nach letztem bestätigten Restbatch
    N->>P: finish beim regulären Jobabschluss
```

Der Priority-Worker erhöht für jede Anforderung einen Token. Er prüft alle
fünf Sekunden die Pipe und akzeptiert nur die passende Freigabe. Ist das
Fenster beim Erlangen des Locks bereits abgelaufen oder kommt ein passendes
`reset`, fordert er mit neuem Token ein weiteres Fenster an. Dadurch kann
eine verspätete Nachricht keinen anderen Batch versehentlich freigeben.

Nach passendem `waiting` erwirbt das Kind das gemeinsame Lock, kontrolliert
die Frist und schreibt den gesamten Batch über eine eigene Verbindung.
`_save_priority_batch_once()` committet erst nach allen Ergebnissen. Bei
Fehlern wird zurückgerollt; Lock-Retries wiederholen den ganzen Batch.
Die reguläre Ergebnisroutine erhält hierfür die Batch-Verbindung und
committet nicht jedes einzelne Ergebnis separat. Ergebnisse und positive
Teilbaumbestätigungen landen in `tmp_doubles_letchecked`.

Der normale Worker bedient IPC auch unmittelbar vor seinem nächsten
Ergebnis-Write. Er schreibt ebenfalls nur unter dem gemeinsamen Lock.
Ohne Priority-Kind wird kein solches Lock benötigt und der normale Worker
speichert weiterhin ein Ergebnis je eigener Transaktion.

### 4.5 Warum das zusätzliche Python-Lock erforderlich ist

Die Nachricht `waiting` ist eine Absprache, keine Datenbanksperre. Der
normale Worker schläft zehn Sekunden und sendet anschließend `reset`.
Er wartet nicht gezielt auf `committed`. Ein bereits gestarteter Commit
oder dessen Lock-Retries können länger dauern als dieses Fenster.

Das gemeinsame Lock verhindert dann trotzdem überlappendes
Ergebnis-Schreiben der beiden Worker. Die Frist wird vor dem Schreiben
geprüft, nicht als harter Abbruch während des Commits. Insbesondere wird
das Lock während der gesamten Retry-Folge gehalten. Dadurch kann der
normale Worker deutlich länger als zehn Sekunden am Lock warten.

Andere Server, Hosts, Verwaltungsbefehle und unabhängige Binarycheck-Jobs
teilen dieses Python-Lock nicht. Gegen deren Konflikte wirken weiterhin
SQL-Transaktionen, InnoDB-Sperren und gegebenenfalls Lock-Retries. Die
getrennten Caches bereits verifizierter Paare werden nicht per IPC laufend
zusammengeführt; delegierte Paare haben aber jeweils nur einen zuständigen
Worker.

### 4.6 Registrierung, Ende, Debug und weitere IPC

Das Priority-Kind startet zunächst ohne Arbeitsfreigabe. Der normale
Worker registriert seine PID und eigene Runid unter dem Runid-Lock und
sendet erst danach `runid`. Bei Registrierungsfehlern wird das Kind beendet
und abgeholt, statt ohne Eintrag weiterzulaufen.

Vor dem regulären Jobende wartet der normale Worker auf `idle`, sendet
`finish` und wartet auf das Ende des Kindes. Fehler und unerwartete
Exitcodes werden weitergegeben. Das Aufräumen schließt Pipe-Enden und
beendet nötigenfalls das Kind mit Eskalation von `terminate()` zu `kill()`.
Spawn und Schließen werden mit PID, Job-ID, Host-ID und Runid auf der
Konsole und im Syslog gemeldet.

`SIGUSR1` an die Priority-PID aktiviert für deren restliche Laufzeit die
Konsolenausgabe jedes fertig verglichenen linken und rechten Dirids samt
Status. Diese Ausgabe bedeutet „Vergleich fertig“, nicht zwingend
„Batch bereits committet“.

Eine getrennte IPC-Verwendung betrifft den lokalen Coarse-Debugmodus:
Der Server teilt dafür ein `multiprocessing.Event` mit seinem
Coarse-Kind. `SIGUSR1` am Localserver schaltet dieses Event um. Es ist kein
Schreib-Lock und gehört nicht zum Priority-Pipe-Protokoll.
Der Server überwacht seinen Job-Worker außerdem über `join(timeout=0.5)`
und liest den gespeicherten Jobstatus in längeren Polling-Abständen.

## 5. Laufzeitbegrenzung in MariaDB: 360 Sekunden

Die Grundkonfiguration liegt in MariaDB selbst. Ausschlussbereinigung und
Vorbereitung der Binarycheck-Jobs verwenden zusätzlich einen Faktor 3 für
Verbindungs-, I/O- und Sitzungs-Statement-Limits. Dafür wird auf diesen
Verbindungen `SET SESSION max_statement_time = @@SESSION.max_statement_time * 3`
ausgeführt. Ein Sitzungswert 0 bleibt dabei unbegrenzt.
`init` und `serverinit` lesen den globalen und eigenen Sitzungswert aus.
Nur wenn einer von 360 Sekunden abweicht, zeigen sie die Einrichtungsanleitung.
Sie ändern den Timeout nicht selbst.

### 5.1 Dauerhaft auf dem Synology-NAS konfigurieren

Auf dem NAS mit ausreichenden Administratorrechten die Datei öffnen:

```sh
sudo vi /var/packages/MariaDB10/etc/my.cnf
```

In `vi` mit `i` den Einfügemodus aktivieren. Im vorhandenen Abschnitt
`[mariadb]` die Einstellung ergänzen oder einen bestehenden Wert ändern:

```ini
[mariadb]
max_statement_time = 360
```

Den Abschnitt nur ergänzen, wenn er noch nicht vorhanden ist. Mit `Esc`
und `:wq` speichern und schließen. Anschließend MariaDB 10 über die
Paketverwaltung neu starten, damit die Datei eingelesen wird. Dadurch
werden bestehende DB-Verbindungen unterbrochen; Scanner vorher geordnet
beenden und anschließend neu starten.

### 5.2 Alternative für das laufende System

In phpMyAdmin oder der MariaDB-Konsole als Administrator, beispielsweise
`root`, anmelden. Administratorlogin beziehungsweise die Berechtigung für
`SET GLOBAL` ist erforderlich. Ohne MariaDB-Neustart ausführen:

```sql
SET GLOBAL max_statement_time = 360;
```

Das wirkt als Vorgabe für neu aufgebaute Verbindungen. Bereits vorhandene
Sitzungen behalten ihren bisherigen Sitzungswert; die Scanner gegebenenfalls
neu starten. Dieser SQL-Befehl allein ist nach einem MariaDB-Neustart
nicht dauerhaft. Für die dauerhafte Einstellung zusätzlich `my.cnf`
anpassen. Zum Prüfen, auch nach dem Neustart:

```sql
SELECT
    @@GLOBAL.max_statement_time AS globales_limit_sekunden,
    @@SESSION.max_statement_time AS sitzungs_limit_sekunden;
```

Die Sitzungsanzeige betrifft dabei die aktuelle phpMyAdmin-/Konsolensitzung,
nicht automatisch eine bereits laufende Scanner-Verbindung.

### 5.3 Grenzen und verbleibende Risikostellen

Das Limit gilt pro SQL-Anweisung, nicht pro Sitzung, Batch, Dateivergleich
oder vollständigem Scan. MariaDB kontrolliert den Abbruch an internen
Prüfpunkten; 360 Sekunden sind keine garantierte harte Endfrist. Ein
Statement-Abbruch beendet nicht automatisch die gesamte umgebende
Transaktion. Ein notwendiges Rollback kann zusätzliche Zeit beanspruchen.
[MariaDB: Statement-Zeitlimit](https://mariadb.com/docs/server/ha-and-performance/optimization-and-tuning/query-optimizations/aborting-statements)

Potentiell längere Abfragen sind insbesondere die rekursive Erstellung der
Detailed-Arbeitsliste, der rekursive Leselauf für die abschließenden
Teilbaumstatistiken und die anfänglichen Paar-/Cache-Joins beim Binarycheck.
Die Elternreparatur besitzt weiterhin einen größeren Lesezugriff, aber
keinen großen Self-Join-Write mehr. Das sind mögliche Risikostellen, keine
Messung, dass jede davon tatsächlich länger als zehn Sekunden läuft.

„Einmal pro Ausführung“ bedeutet: Vorbereitung einer neuen Detailed-Stufe
beziehungsweise abschließender Statistiklauf für diese Wurzel, nicht eine
Abfrage pro Dirid. Beim normalen Binarycheck werden die zwei größeren
Paar-/Cache-Abfragen einmal pro `_run_job()` geladen, bevor die
Dateivergleiche beginnen. Das Priority-Kind führt `_run_job()` für jede
zugewiesene Arbeitsliste erneut aus; das ist kein erneutes Laden pro
20-Ergebnis-Schreibbatch.

Eine langsame reine Leseabfrage kann trotz fehlender Zeilensperren viel CPU,
I/O, Speicher und temporären Plattenplatz beanspruchen. Umgekehrt können
viele kurze Updates zusammen eine lange Transaktion mit lange gehaltenen
Sperren bilden. Das Statement-Limit ersetzt keine Transaktionsplanung.

Bei 80.000 vollständig zu schreibenden normalen Vergleichsergebnissen sind
ungefähr 80.000 Ergebnis-Transaktionen nötig; bei vollständigen
Priority-Batches von je 20 etwa 4000. Kleinere Rest-/Zeitbatches,
Zusatzabfragen, Polling und Retries erhöhen die Anzahl. 80.000 Dirids sind
nicht automatisch 80.000 Vergleichspaare. `SET GLOBAL` wird einmal zur
Umstellung benötigt und muss nicht pro Verbindung oder Datensatz erneut
ausgeführt werden.

SQL-Befehle derselben Verbindung werden nacheinander verarbeitet. Eine
zusätzliche asynchrone Python-Aufgabe macht eine Sitzung nicht zu einem
Kanal für parallel laufende Abfragen; bei unbuffered Cursorn muss das
Ergebnis abgeholt beziehungsweise korrekt beendet sein, bevor ein weiterer
Befehl gesendet wird. Der aktuelle Wert wäre mit
`SELECT @@SESSION.max_statement_time` lesbar; einen historischen alten Wert
müsste der Aufrufer selbst vorher speichern. Die Verwaltungsoperationen mit Faktor 3 verwenden eigene Verbindungen;
der globale Wert wird dadurch nicht verändert.

## 6. Prozessende, SQL-Abbruch und Rollback

Eine MariaDB-Prozesslisten-ID ist eine Datenbank-Verbindungskennung,
keine Betriebssystem-PID. Ein Eintrag wie `localhost:54824` beschreibt den
Verbindungspartner aus Sicht von MariaDB. Im beobachteten NAS-Fall zeigte
`netstat` `CLOSE_WAIT` am MariaDB-Prozess: Der Python-Client war bereits
weg, während seine Abfrage serverseitig noch lief. Die PID von `mariadbd`
ist dann kein gezielt zu beendender Scan-Prozess.

`KILL QUERY ID` beziehungsweise `KILL QUERY <Verbindungs-ID>` beendet nur
das betreffende Statement und kann die Transaktion offen lassen.
`KILL CONNECTION <Verbindungs-ID>` beendet die Verbindung und veranlasst
das Rollback ihrer offenen Transaktion. Das Programm verwendet deshalb
`KILL CONNECTION`. Nach dem Kill wird die Prozessliste erneut abgefragt,
bis die betroffenen IDs verschwinden. Das ist seine operative Bestätigung
für die abgeschlossene Verbindungsbereinigung.
[MariaDB: KILL](https://mariadb.com/docs/server/reference/sql-statements/administrative-sql-statements/kill)

### 6.1 Eindeutige Zuordnung und Smartstart

Neue SQL-Anweisungen innerhalb eines Servers erhalten einen führenden
Kommentar `/*dirwalker:HEX...*/`. Die Hex-Daten enthalten JSON mit Host-ID,
Rechner, PID, Prozessstartkennung und einer pro Serverlauf neuen Gruppe.
Die Gruppe wird über die Umgebung an Spawn-Kinder weitergegeben; jedes
Kind setzt beim Verbindungsaufbau seine eigene PID/Startkennung ein.
Die Kennung ist eine betriebliche Zuordnung, keine kryptographische
Authentifizierung. Die separate Kontrollverbindung kennzeichnet sich
nicht selbst und schließt ihre eigene `CONNECTION_ID()` von Kills aus.

```sh
./dirwalker.py --smartlocalserver
./dirwalker.py --smartlocalserver --hostid 1
```

Ohne ID wird eine eindeutige positive lokale Registrierung gewählt; mit
ID muss diese zum lokalen Benutzer/Rechner passen. Zuerst wird der lokale
Server-Lock erworben. Danach prüft Smartstart mit `SHOW FULL PROCESSLIST`
Verbindungen desselben DB-Benutzers zur konfigurierten Datenbank:

- Klar zugeordnete lokale Abfragen der gewählten Host-ID: Für jede wird
  `Beenden und Rollback abwarten? [yN]` angeboten. Nur `y` bestätigt.
- Gibt es unklar zugeordnete laufende Abfragen, werden ausschließlich deren
  `KILL CONNECTION <ID>;`-Befehle ausgegeben. Es erfolgen keine Kills, keine
  zusätzlichen Hilfen, keine Fehlerjob-Resets und kein Serverstart.
- Bei Ablehnung wird kein neuer Scan gestartet. Bestätigte Kills werden
  erst nach den erforderlichen Bestätigungen ausgeführt; anschließend wird
  bis zu 20 Sekunden auf das Verschwinden gewartet. Bleibt eine ID bestehen,
  wird der Start abgebrochen.
- Danach werden Fehlerjobs ausschließlich der gewählten positiven Host-ID
  einzeln mit `[yN]` zum Reset angeboten. Verwendet wird dieselbe Methode
  `DataBaseCLI.reset_error_job()` wie bei `--jobs-reset-on-error`, ergänzt
  um eine Host-ID-Bedingung im SQL-Update.
- Coarse-/Binarycheck-Fehlerjobs werden auf `insert`, andere Stufen auf
  `pending` gesetzt. Bei Binarycheck wird die Stufe auf `coarse` gesetzt;
  aktive Arbeitslisteneinträge werden `pending`. Fehlermeldungen und
  Start-/Endzeiten werden entsprechend der bestehenden Reset-Methode
  zurückgesetzt. Abgelehnte oder fehlgeschlagene Resets verhindern den Start.

Alte, unmarkierte Abfragen sind nicht sicher zuzuordnen. Auch eine
schlafende Verbindung hat normalerweise keinen sichtbaren SQL-Kommentar
in `PROCESSLIST.Info`. Die aktuelle Suche ist daher kein vollständiges
Register aller jemals geöffneten oder noch schlafenden Transaktionen.
Eindeutig einem anderen Rechner, Host oder Serverlauf zugeordnete Abfragen
werden nicht mitbeendet. Die Sichtbarkeit ist auf die Rechte des
DB-Benutzers beschränkt; für fremde Verbindungen können zusätzliche
Administratorrechte erforderlich sein.

### 6.2 Ctrl-C und SIGTERM

Der gemeinsame Serverpfad behandelt Ctrl-C (`SIGINT`) und `SIGTERM`:

1. Shutdown markieren und auf der Konsole die Frist von 20 Sekunden melden.
2. Dem aktuellen Job-Worker `SIGINT` senden. Das Scan-Kind arbeitet in einer
   eigenen Prozessgruppe und behandelt seine Stop-Signale als Unterbrechung,
   damit vorhandene Aufräumblöcke ausgeführt werden können.
3. Parallel in einem Hintergrundthread die eindeutig zur eigenen
   Servergruppe gehörenden DB-Abfragen mit `KILL CONNECTION` abbrechen und
   auf ihr Verschwinden warten; fremde/unmarkierte Abfragen bleiben erhalten.
4. Auf das Ende des Kindes und die Aufräumarbeiten warten. Ein unterbrochener
   Job wird im normalen Statuspfad als `paused` behandelt, auch wenn das
   Kind nach seiner Unterbrechung mit Exitcode 0 endet.
5. Wenn die Frist ausgeschöpft ist, die Scan-Prozessgruppe einschließlich
   Priority-Enkeln hart beenden und den Server mit Exitcode 130 verlassen.
   Die Konsole meldet, dass ein DB-Rollback noch weiterlaufen kann.

Die Frist begrenzt das lokale Warten; sie garantiert kein innerhalb von
20 Sekunden abgeschlossenes MariaDB-Rollback. Bei einem harten Fristende
kann auch die letzte Statuspersistierung unvollständig bleiben.
Ein gewöhnlicher gespeicherter Jobstatus `stopped` verwendet einen eigenen
Abbruchpfad; die hier beschriebene 20-Sekunden-Frist gehört zur
Signalbehandlung des Servers.

## 7. Diagnose im laufenden Betrieb

Zuerst lesend prüfen:

```sql
SHOW FULL PROCESSLIST;

SELECT VERSION(), @@innodb_lock_wait_timeout;
SELECT @@GLOBAL.max_statement_time, @@SESSION.max_statement_time;
SELECT @@GLOBAL.tx_isolation, @@SESSION.tx_isolation;

SELECT jobid, hostid, status, stage, runid, errmsg
FROM jobs
ORDER BY jobid;

SELECT runid, jobid, task, status, stage, pid, process_host, process_start
FROM jobsrunid
ORDER BY runid DESC
LIMIT 30;
```

Für Sperrdiagnose gegebenenfalls als Administrator ausführen:

```sql
SHOW ENGINE INNODB STATUS;
SELECT * FROM information_schema.INNODB_TRX;
SELECT * FROM information_schema.INNODB_LOCK_WAITS;
```

Der nachweislich blockierende Eintrag wird gezielt beendet, nicht pauschal
der MariaDB-Prozess oder alle Verbindungen eines Benutzers:

```sql
-- <ID> durch die vorher geprüfte Verbindungs-ID ersetzen:
KILL CONNECTION <ID>;
SHOW FULL PROCESSLIST;
```

Solange die ID noch sichtbar ist, kann die Abfrage beziehungsweise ihre
Bereinigung weiterlaufen. Neustarts ohne Beseitigung des Blockierers können
wieder dieselben Lock-Fehler erzeugen.

Für Abfragen oberhalb von zehn Sekunden kann ein Administrator das Slow
Query Log konfigurieren. Dessen Einstellungen und Ziel sollten zuerst
geprüft werden; laufende Abfragen sieht man weiterhin über die Prozessliste.
Die Zahl wirklich langsamer Abfragen muss gemessen werden. Aus der Anzahl
der Dirids allein lässt sie sich nicht ableiten.

## 8. Schema, serverinit und Betrieb

[`sql/migrate_jobsrunid_pid.sql`](../sql/migrate_jobsrunid_pid.sql) ergänzt die Prozessspalten für ältere
Installationen. [`sql/migrate_jobs_stale.sql`](../sql/migrate_jobs_stale.sql) ergänzt den zulässigen Status
`stale`. Fehlende Spalten wie `r.pid` sind ein Schemafehler und werden nicht
durch Server-Neustarts repariert.

`serverinit` prüft das MariaDB-Zeitlimit, stellt `hosts` und die Arbeitsliste
`tmp0_job_work` bei Bedarf bereit und registriert den ausführenden Rechner
als Host-ID 0. Andere Host-Einträge bleiben erhalten. Es löscht keine
Tabellen oder Ergebnisse und ändert aktive Jobs nicht mehr auf `stale`.
Die früheren destruktiven serverinit-Schritte entfallen. Die vollständige
Schema-Prüfung gehört jetzt zum abschließenden `init`-Ablauf.

`init` legt fehlende Kerntabellen an und prüft das MariaDB-Zeitlimit vor
verändernden Inventory-Schritten. Anschließend wird `directories` erstellt
oder nach der bestehenden Rückfrage gelöscht und neu aufgebaut. Wird die
Neuanlage bestätigt oder fehlt die Verzeichnistabelle bereits, werden alte
Dirid-Verweise ungültig: `jobs`, `jobsrunid`, numerische Host-Arbeitslisten,
legacy `job_work`, Vergleichsergebnisse, Priority-Hinweise und gespeicherte
Deep-Doubles-Ergebnisse werden mit `DELETE` geleert. Tabellen und
Host-Registrierungen bleiben erhalten; `hosts.homedirid` wird auf NULL
zurückgesetzt. Bei Ablehnung eines vorhandenen directories-Resets bleiben
Jobs und Historie erhalten. Danach wird Host 0 registriert und das gesamte
Schema geprüft. Alle Worker müssen vor einem solchen `init` gestoppt sein.
DDL und nachfolgendes Leeren sind keine gemeinsame atomare Transaktion:
MariaDB bestätigt DDL eigenständig. Scheitert ein späterer Schritt, darf
ein partieller Reset nicht als erfolgreich abgeschlossene Initialisierung
behandelt werden.

`--drop-directories` löscht und erstellt die Verzeichnistabelle neu; es ist
ebenfalls kein Diagnose- oder Entsperrbefehl. Fehlerjob-Reset und
Verbindungs-Kill löschen dagegen keine Tabellen.

Nach Installation laden bereits laufende Python-Prozesse den geänderten
Code nicht automatisch nach. Server geordnet stoppen und neu starten.
Die neue Stop-Logik greift erst im neu gestarteten Prozess.

## 9. Prüfstand und Implementierungsstellen

Offline-Tests prüfen unter anderem Server-Einzelinstanzen, Hostauswahl,
yN-Entscheidungen, Host-begrenzte Fehler-Resets, IPC, Batchabläufe, Scanstufen
und Schutz fremder DB-Verbindungen. Die Ctrl-C-Tests verwenden echte Signale
mit simulierten DB-Antworten. Das bestätigt keine konkrete Ausführungszeit gegen
eine produktive MariaDB. Aktuelle Ergebnisse mit `make test-dryrun` und
`make test` ermitteln; echte Datenbankprüfungen sind separat opt-in.

Quellstellen im Projekt `/Users/alex/prog/ongh/dirwalker`:

- `DataBaseCore.py`: `connect`, `_repair_directory_parents`,
  `write_run_record`, `_tag_connection`, `cleanup_server_queries`.
- `version25.py`: `prepare_stage_work`, `claim_work_batch`,
  `rebuild_subtree_statistics`, `_acquire_runid`, `claim_job`,
  `_retry_on_lock_error`, `Binarycheck` und dessen Pipe-Protokoll.
- `server_operations.py`: `_server_instance_lock`, `run_server`,
  `_find_smart_local_host`, `_reset_smart_errors`, `run_spawn`,
  `_child_entry`, `handle_signal`, `_force_stop_child`.
- `DataBaseCLI.py`: `serverinit`, Hostregistrierung, `insert_priority`,
  `reset_error_job` und Verwaltungsoperationen.
- `sqlstructure.py`: Tabellen, Constraints, Strukturprüfung und SQL-Zeitlimits.
- `client_operations.py`: `--smartlocalserver [--hostid HOSTID]`,
  `--jobs-reset-on-error`, `--insert-priority` und CLI-Ausgabe.


## 11. Flüchtige Verzeichnisse und Verbindungsaufbau-Timeout

Ein Coarse-Arbeitseintrag kann auf ein Verzeichnis zeigen, das nach dem
Einreihen entfernt oder durch eine Datei ersetzt wurde. Das tritt etwa
bei laufend veränderten Photos-Library-Caches auf. Beim Öffnen des
Verzeichnisses werden `FileNotFoundError` und `NotADirectoryError` für
Unterverzeichnisse deshalb mit einer Konsolenmeldung behandelt: kein neuer
Verzeichniseintrag, keine Kinder und Bereinigung der veralteten
`directories`-Einträge dieses Teilbaums innerhalb des vorhandenen,
hostbezogenen Batch-Abschlusses. Der nächste Arbeitseintrag wird weiter
bearbeitet. Verschwindende einzelne Verzeichniseinträge während der
Iteration werden übersprungen. Andere I/O-Fehler werden nicht pauschal
verschluckt. Verschwindet die angeforderte Scan-Wurzel selbst, ist das
weiterhin ein Fehler und kein erfolgreicher leerer Scan.

Der optionale lokale glob-Modus prüft zuerst, ob das Verzeichnis noch
existiert; glob allein könnte für einen verschwundenen Pfad eine leere
Liste liefern und damit ein falsches leeres Verzeichnis vortäuschen.

`2003: Can't connect ... (60 Operation timed out)` betrifft den Aufbau
einer Verbindung zu MariaDB, etwa `synol:5799`. Dabei hat noch keine
SQL-Abfrage begonnen. `max_statement_time = 360` greift daher nicht.
Normale Verbindungsaufbauten erhalten explizit `connect_timeout=10`;
Verwaltungsoperationen mit Faktor 3 verwenden 30 Sekunden. Die separate
Kontrollverbindung behält fünf Sekunden. Der Standard für Lese-/Schreib-Timeouts
ist `DB_IO_TIMEOUT=30`, bei Faktor 3 also 90 Sekunden.
Der Parameter wird von beiden unterstützten Treibern akzeptiert. Das
begrenzt den Verbindungsaufbau, nicht die gesamte Scan- oder SQL-Laufzeit;
DNS-Auflösung und mehrere Zieladressen können zusätzliche Zeit verursachen.

Bei Fehler 2002, 2003 oder 2005 im Server-Polling zeigt der Server eine
verständliche Meldung mit Zielhost/Port, weist auf den Unterschied zum
Statement-Limit hin und versucht es im nächsten Polling erneut. Es wird
nicht behauptet, dass der Code eine ausgefallene Netzwerkverbindung oder
einen gestoppten MariaDB-Dienst selbst repariert. Ursache weiterhin auf
NAS, Netzwerk und Namensauflösung prüfen. Ein bereits durch den früheren
Verzeichnisfehler auf `error` gesetzter Job muss nach Installation und
Neustart gezielt mit `--jobs-reset-on-error` beziehungsweise der yN-Frage
von `--smartlocalserver --hostid 1` freigegeben werden.

Treiberreferenzen: [MySQL-Verbindungsargumente](https://dev.mysql.com/doc/connectors/en/connector-python-connectargs.html)
und [MariaDB-Verbindungsargumente](https://mariadb.com/docs/connectors/mariadb-connector-python/api/connection).


## 12. Gespeicherter Ausschluss von reinen Nullbyte-Unterbäumen

`directories.zero_content_only` ist ein abgeleitetes Boolean-Flag mit
Default FALSE. TRUE bedeutet: Alle regulären Dateien dieses Verzeichnisses
und seiner vollständigen Unterbäume sind lesbare 0-Byte-Dateien, es gibt
keine besonderen Einträge und keine unbekannten oder fehlenden Teilbereiche.
Auch ein vollständig leeres Verzeichnis erfüllt diese Regel. Ein einzelnes
nichtleeres File, ein Symlink/Spezialeintrag, fehlende Scanwerte, fehlende
Kinder oder ein Berechtigungsfehler verhindern den Ausschluss. Die Anzahl
der Dateien allein oder eine Teilbaumgröße von null reichen nicht aus.

Die Bottom-up-Berechnung verwendet `localfilecount`, `emptyfilecount`,
`localfilesize`, `subdircount`, `otherentrycount`, `nopermdirs` und die
vollständigen hostbezogenen Elternbeziehungen. Zyklen bleiben unbekannt.
Neue Detailscans prüfen das Öffnen regulärer 0-Byte-Dateien. Dabei verhindern
O_NOFOLLOW/O_NONBLOCK das Folgen eines inzwischen ersetzten Symlinks oder
Blockieren an einer inzwischen eingesetzten FIFO. Bei unvollständigem Lesen
wird `emptyfilecount` als NULL gespeichert, damit das Flag nicht auf Basis
unvollständiger Daten gesetzt werden kann.

Zu Beginn einer Scan-Stufe werden gesetzte Flags des betroffenen Hosts in
einer eigenen kurzen Transaktion zurückgenommen. Erst nach erfolgreicher
Neuberechnung der Teilbaumstatistiken werden sie aus den vorhandenen
Metadaten erneut berechnet. `refresh_zero_content_flags()` liest die
Metadaten einmal, berechnet ohne Python-Rekursion und schreibt nur geänderte
Flags in Batches von maximal 200. Das ist kein erneuter Dateisystemscan;
Metadaten älterer Läufe können bei inzwischen geänderten Rechten oder
Dateiinhalten nur durch einen neuen Detailscan aktualisiert werden.

Die Datenbankabfragen der Duplikat-Kandidatenliste, Ebenenanalyse,
HTML-Baumdaten, bestätigten Vergleichspaare und Dirid-Pfadauflösung filtern
mit `zero_content_only = FALSE`, bevor Pfade abgeholt werden. Die Anzeige
muss daher nicht mehr versuchen, anhand verkürzter Zeilen mit Datei- und
Unterverzeichnisanzahl erneut auf Leere zu schließen. Binarycheck verwendet
nur einen Satz der gesetzten `(hostid, dirid)`-Flags; dieser wird nicht pro
Ergebnis erneut berechnet. Bereits eingereihte Vergleiche zweier ausgeschlossener
Bäume können als `skipped_empty` abgeschlossen werden, damit der Fortschritt
nicht hängen bleibt. Neu entdeckte ausgeschlossene Unterpaare werden nicht
als positive Resultate gespeichert. Historische Resultatzeilen werden nicht
pauschal gelöscht; die SQL-Filter halten sie aus den Ergebnislisten heraus.

### Bestehende Datenbank migrieren

Scans vor dem Schemawechsel stoppen. In der ausgewählten Datenbank das
Skript [`sql/migrate_directories_zero_content.sql`](../sql/migrate_directories_zero_content.sql) ausführen. Es ergänzt die
Boolean-Spalte und den Index `(hostid, zero_content_only)`. Kein Verzeichnis,
Job oder Historieneintrag wird dabei gelöscht. Neue Zeilen und alte noch
nicht klassifizierte Zeilen bleiben zunächst sichtbar.

Nach Installation des neuen Codes und Laden der üblichen DB-Umgebung einmal
die Flags für vorhandene Scan-Metadaten berechnen:

```sh
python3 -c 'import dirwalker; print(dirwalker.database_connection.refresh_zero_content_flags())'
```

Der ausgegebene Wert ist die Anzahl geänderter Flags. Anschließend können
die Server wieder gestartet und Berichte neu erzeugt werden. Neue vollständige
Detailscans aktualisieren die Flags selbst. Die Migration wurde als Skript
bereitgestellt, nicht gegen die produktive Datenbank ausgeführt.

## 13. Änderungen seit den früheren Abläufen

- [rescan.md](rescan.md) beschreibt die Änderungskontrolle und markierte Rescans
  einschließlich Lesefehlern und Bereinigung von `tmp_modified`.
- [age-rescan.md](age-rescan.md) beschreibt `rescan-after-days` und dessen
  drei Stufen. Die Schemaaktualisierung erfolgt manuell per SQL-Skript.
- [binarycheck-freshness.md](binarycheck-freshness.md) beschreibt `hashed_at`
  und die Rückfragen vor erneutem Einreihen abgeschlossener Paare.
- `--dirs-purge-from-exclusion-lists` verarbeitet die Host-IDs nacheinander in
  begrenzten SQL-Batches. Ein Timeout 2055 ist ein Verbindungs-/I/O-Problem,
  kein Nachweis für einen MariaDB-Statement-Abbruch.
- `check` prüft registrierte Host-Arbeitstabellen und unterschiedliche Job-IDs
  desselben Root-Pfads. Mehrere Runids desselben Jobs gelten nicht als Konflikt.
- Nach einer bearbeiteten Aufgabe pollt der Server einmal nach zehn Sekunden;
  bei anschließendem Leerlauf gilt wieder das normale Intervall.
