# Umsetzungsplan: Inhalte erfolgreich erzeugter Dateien in MariaDB speichern

Status: geplant, noch nicht umgesetzt.

Abschnitt 11 beschreibt eine Ersatzlösung für den Fall, dass für die Umsetzung keine Kapazität vorhanden ist: Die
Anwendung bleibt unverändert, und die Dateien werden per Skript manuell in die Datenbank importiert.

## 1. Ziel und Ausgangslage

Bisher endet der Weg einer Datei, sobald Cloud-API 2 für ihre TaskId `SUCCESS` meldet: Der `StatusConsumer` verschiebt sie
in `StatusConsumer.settle` in die `donebox` und entfernt ihren TaskId-Marker. Künftig soll zusätzlich der **Inhalt jeder
erfolgreich erzeugten Datei** in einer Tabelle der **MariaDB** gespeichert werden.

Randbedingungen aus der Analyse:

| Nr. | Randbedingung | Folge für den Entwurf |
|---|---|---|
| R1 | Eine Datei mit `SUCCESS` ist in der Cloud erfolgreich erzeugt. | Ein Fehler beim Speichern darf sie **nicht** in die `errorbox` bringen; die `errorbox` bedeutet „Erzeugung fehlgeschlagen“. |
| R2 | Eine Aufgabe kann zehntausende Dateien umfassen. | Gespeichert wird in Batches, nicht mit einem Datenbankaufruf je Datei. |
| R3 | Eine langsame oder nicht erreichbare Datenbank darf den Consumer nicht blockieren. | Der Consumer führt keine Datenbankaufrufe aus. |
| R4 | Vor dem Ende einer Aufgabe soll ein letzter Speicherversuch stattfinden, höchstens 30 Sekunden lang. | Der Supervisor wartet vor der Abmeldung auf eine letzte Speicherrunde. |
| R5 | Der Inhalt wird als **Originaltext** gespeichert. | Keine Normalisierung des JSON beim Speichern. |

## 2. Betrachtete Varianten

| Variante | Beschreibung | Bewertung |
|---|---|---|
| A | Consumer speichert synchron vor dem Verschieben; bei Fehler kommt die Datei in die `errorbox`. | **Verworfen**: verletzt R1 (die Datei ist erzeugt) und R3 (Consumer wartet auf die Datenbank). |
| B | Consumer speichert synchron; bei Fehler bleibt die Datei mit Marker in der `pendingbox` und wird später erneut versucht. | **Verworfen**: Bei längerem Datenbankausfall endet die Aufgabe mit `TIMEOUT`, obwohl alles erzeugt ist; die Restdateien blockieren den nächsten Start (`PENDINGBOX_NOT_EMPTY`); verletzt R3. |
| C | Consumer speichert synchron; bei Fehler kommt die Datei trotzdem in die `donebox`, dazu ein Marker zum Nachspeichern. | **Verworfen**: erfüllt R1, aber nicht R3; jede Datei wartet bei Störung bis zum Timeout. Zwei getrennte Mechanismen (Normalfall und Nachholen). |
| D | Consumer schreibt nur einen Marker; **ein** globaler Speicher-Thread für die ganze Anwendung speichert in Batches. | **Verworfen**: Er müsste die Ordner aller Benutzer:innen durchsuchen; sein Lebenszyklus ist unabhängig von den Aufgaben, der letzte Speicherversuch vor dem Ende einer Aufgabe ließe sich nur umständlich abstimmen; passt nicht zum Sandbox-Prinzip. |
| **E** | Consumer schreibt nur einen Marker; **je Aufgabe ein** Speicher-Thread in der Sandbox speichert in Batches; der Supervisor wartet vor der Abmeldung auf eine letzte Runde (höchstens 30 s). | **Gewählt.** |

## 3. Entscheidung und Begründung

Umgesetzt wird **Variante E**: Entkopplung über Marker in der `donebox` (Outbox-Prinzip) und ein Speicher-Thread je
Aufgabe.

Gründe:

1. **Fachlich richtig (R1)**: Die `donebox` bleibt „erfolgreich erzeugt“, unabhängig vom Zustand der Datenbank. Ob eine
   Datei schon gespeichert ist, zeigt allein ihr Marker.
2. **Consumer wird nie blockiert (R3)**: Er führt nur lokale Dateioperationen aus (Verschieben, Marker schreiben), die
   Millisekunden dauern. Statusabfragen, Timeout-Erkennung und Abbruch bleiben auch bei gestörter Datenbank pünktlich.
3. **Wenige Datenbankaufrufe (R2)**: Der Speicher-Thread fasst bis zu 500 Einträge zu einem Batch zusammen; zehntausend
   Dateien ergeben einige Dutzend Statements statt zehntausend.
4. **Ausfallsicher**: Marker liegen auf der Platte. Fällt die Datenbank aus, sammeln sie sich nur an und werden nach der
   Wiederherstellung gespeichert. Das Einfügen ist über die TaskId idempotent; doppelte Versuche schaden nicht.
5. **Ein Mechanismus statt zwei**: Normalfall und Nachholen nach Fehlern sind derselbe Ablauf.
6. **Passt zur Architektur**: `anforderungen.md` verlangt je Aufgabe einen isolierten Kontext und eigene Threads. Der
   Speicher-Thread entsteht und endet mit der Aufgabe, durchsucht nur die Ordner seiner Benutzer:in und Art, und eine
   gestörte Aufgabe bremst die Speicherung anderer Aufgaben nicht.
7. **Letzter Versuch vor dem Ende (R4)**: Der Supervisor wartet vor dem Schreiben des Endzustands auf die letzte
   Speicherrunde. Ist die Datenbank verfügbar, sind beim Endzustand `COMPLETED` in der Cloud alle Inhalte gespeichert.

## 4. Zielbild des Ablaufs

### 4.1 Consumer bei `SUCCESS`

1. Datei in die `donebox` verschieben (`UserFolders.moveToDone`, wie bisher).
2. Speicher-Marker `donebox/.zu_speichern/<Dateiname>` schreiben, und zwar zum tatsächlichen Namen in der `donebox`
   (beim Verschieben kann ein Zusatz entstehen). Inhalt des Markers, als kleines JSON:
   `{"jobId":"…","taskId":"…","userId":"…","jobType":"…"}`.
3. Als erfolgreich zählen (`recordSucceeded`), wie bisher.

Kein Datenbankaufruf. Der Ordner `.zu_speichern` zählt wie `.taskids` und `.fehler` nicht als Inhalt der `donebox`.

### 4.2 Speicher-Thread je Aufgabe (`writer-<Aufgabennummer>`)

Ein dritter Virtual Thread der Sandbox neben Producer und Consumer. Er läuft in einer Schleife im Abstand von
`pipeline.db.store-interval` (Vorgabe 2 s):

1. Bis zu `pipeline.db.batch-size` (500) Marker aus `<userId>/<jobType>/donebox/.zu_speichern/` lesen, begrenzt auf
   insgesamt `pipeline.db.batch-max-bytes` (8 MiB) Dateiinhalt, damit `max_allowed_packet` der MariaDB nicht
   überschritten wird.
2. Zu jedem Marker den Inhalt der Datei aus der `donebox` lesen (Originaltext).
3. Einen Batch-INSERT in einer Transaktion ausführen (siehe 5.2).
4. Nach dem Commit die Marker dieses Batches löschen.

Fehlerbehandlung:

| Fehler | Behandlung |
|---|---|
| Vorübergehend (keine Verbindung, Timeout, Deadlock; SQLState-Klassen `08`, `40`, Treiber-Timeouts) | Marker bleiben, nächste Runde versucht es erneut; Log-Warnung, keine Wiederholung je Datei im selben Durchlauf. |
| Batch scheitert dauerhaft (z. B. ein Eintrag zu groß, ungültiger Inhalt; SQLState-Klassen `22`, `23` außer doppeltem Schlüssel) | Einträge des Batches einzeln wiederholen, um den fehlerhaften zu finden. Dieser bleibt mit Fehlergrund im Log stehen, sein Marker bleibt erhalten und wird in dieser Aufgabe nicht erneut versucht; er zählt als „nicht gespeichert“. |
| Datei zu einem Marker fehlt (von Hand entfernt) | Marker entfernen, Warnung im Log. |

Der Speicher-Thread reagiert **nicht** auf das Abbruchsignal wie Producer und Consumer: Auch nach `TERMINATED`, `ERROR`
oder `TIMEOUT` sind die Dateien in der `donebox` erfolgreich erzeugt und sollen gespeichert werden. Er endet erst nach
der letzten Runde (4.3).

Restmarker früherer Aufgaben derselben Benutzer:in und Art liegen im selben Ordner und werden automatisch mitverarbeitet.

### 4.3 Supervisor: letzte Speicherrunde vor der Abmeldung

Neue Reihenfolge in `TaskManager.supervise`:

1. Auf Producer und Consumer warten (wie bisher).
2. Dem Speicher-Thread signalisieren, dass keine neuen Marker mehr entstehen.
3. Höchstens `pipeline.db.final-store-timeout` (30 s) auf ihn warten. Er führt Runden aus, bis keine verarbeitbaren
   Marker mehr übrig sind.
4. Ist er nach 30 s nicht fertig: ihn unterbrechen; die Marker bleiben liegen. Lokaler Stand und Log nennen die Zahl der
   nicht gespeicherten Dateien. Der Endzustand bleibt unverändert.
5. Endzustand über Cloud-API 3 schreiben, aus der TaskRegistry entfernen, Belegung freigeben (wie bisher).

Sonderfälle:

| Fall | Verhalten |
|---|---|
| Abbruch von außen (`TERMINATED`) | Letzte Speicherrunde wie im Normalfall. |
| Beenden der Anwendung (`ABORTED`) | Keine letzte Runde; die Marker bleiben und werden von der nächsten Aufgabe derselben Benutzer:in und Art gespeichert. |
| Absturz | Wie beim Beenden: Marker bleiben auf der Platte. |

### 4.4 Lokaler Stand

`TaskSnapshot` erhält zwei Zähler: `stored` (in dieser Aufgabe gespeichert) und `notStored` (Marker, die beim Ende noch
übrig sind). Nach dem Ende der Aufgabe stehen sie im Log der Abmeldung.

## 5. Datenbank

### 5.1 Tabelle

```sql
CREATE TABLE testdaten_ergebnis (
    task_id        VARCHAR(64)  NOT NULL,
    job_id         VARCHAR(64)  NOT NULL,
    user_id        VARCHAR(64)  NOT NULL,
    job_type       VARCHAR(32)  NOT NULL,
    datei_name     VARCHAR(255) NOT NULL,
    inhalt         JSON         NOT NULL,
    gespeichert_am TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (task_id),
    INDEX idx_testdaten_ergebnis_job (job_id),
    INDEX idx_testdaten_ergebnis_user_typ (user_id, job_type)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
```

| Spalte | Herkunft | Bemerkung |
|---|---|---|
| `task_id` | TaskId aus Cloud-API 1 | Primärschlüssel; macht das Einfügen idempotent |
| `job_id` | Aufgabennummer (BatchgenAuftrag) | |
| `user_id`, `job_type` | Aufgabe | Ordner `<userId>/<jobType>` |
| `datei_name` | Name in der `donebox` | |
| `inhalt` | Dateiinhalt als Originaltext | In MariaDB ist `JSON` ein Alias für `LONGTEXT` mit Prüfung `JSON_VALID`; der Text wird unverändert gespeichert, Formatierung und Leerzeichen bleiben erhalten. Dateien in UTF-16/UTF-32 werden beim Lesen in Text umgewandelt. |
| `gespeichert_am` | Datenbank | Zeitpunkt des ersten erfolgreichen Speicherns |

### 5.2 Schreiben

```sql
INSERT INTO testdaten_ergebnis (task_id, job_id, user_id, job_type, datei_name, inhalt)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE task_id = task_id;
```

- Ein bereits gespeicherter Eintrag bleibt unverändert; ein zweiter Versuch ist harmlos.
- Bewusst **nicht** `INSERT IGNORE`: Es würde auch andere Fehler (z. B. abgeschnittene Werte) in Warnungen verwandeln.
- Ausführung als JDBC-Batch (`JdbcTemplate.batchUpdate`) in einer Transaktion je Batch.

### 5.3 Technik

| Bereich | Wahl | Begründung |
|---|---|---|
| Zugriff | Spring JDBC (`spring-boot-starter-jdbc`, `JdbcTemplate`/`JdbcClient`) | Es wird nur in eine Tabelle eingefügt; JPA wäre überdimensioniert. Blockierende Aufrufe sind auf Virtual Threads unproblematisch, R2DBC ist nicht nötig. |
| Treiber | MariaDB Connector/J 3.x (`org.mariadb.jdbc:mariadb-java-client`) | offizieller Treiber; Verhalten mit Virtual Threads beim Test prüfen (`-Djdk.tracePinnedThreads=full`) |
| Verbindungspool | HikariCP (Spring-Boot-Standard), `maximum-pool-size` 10, `connection-timeout` 5 s | Jeder Speicher-Thread hält eine Verbindung nur für die Dauer eines Batches. |
| Timeouts | Statement-Timeout 10 s (`JdbcTemplate.setQueryTimeout`), Socket-Timeout im JDBC-URL | Eine hängende Datenbank hält den Speicher-Thread höchstens so lange auf; der Consumer ist nie betroffen. |
| Schema | Flyway (`flyway-core`, `flyway-mysql`), Migration `V1__testdaten_ergebnis.sql` | Schema versioniert im Projekt |

### 5.4 Konfiguration

```yaml
spring:
  datasource:
    url: jdbc:mariadb://localhost:3306/testdaten?socketTimeout=15000
    username: …
    password: …          # aus Umgebungsvariable oder Secret, nicht im Repository
    hikari:
      maximum-pool-size: 10
      connection-timeout: 5000

pipeline:
  db:
    store-interval: 2s         # Abstand der Speicherrunden je Aufgabe
    batch-size: 500            # höchstens so viele Einträge je Batch
    batch-max-bytes: 8MB       # höchstens so viel Dateiinhalt je Batch
    final-store-timeout: 30s   # Wartezeit des Supervisors auf die letzte Speicherrunde
```

## 6. Änderungen im Code

| Komponente | Änderung |
|---|---|
| neues Paket `store` | Schnittstelle `ResultStore` (`store(List<StoredResult>)`), Umsetzung `JdbcResultStore` (MariaDB, Batch, Fehlereinordnung vorübergehend/dauerhaft), Record `StoredResult(taskId, jobId, userId, jobType, dateiName, inhalt)`, Runnable `ResultWriter` (Speicher-Thread) |
| `UserFolders` | Speicher-Marker schreiben, auflisten (begrenzt), lesen und löschen; `.zu_speichern` zählt nicht als Inhalt der `donebox` |
| `StatusConsumer.settle` | bei `SUCCESS` nach dem Verschieben den Speicher-Marker schreiben; kein Datenbankzugriff |
| `Sandbox` | dritter Thread `writer` |
| `TaskManager` | `launch` erzeugt den Speicher-Thread; `supervise` signalisiert das Ende und wartet höchstens 30 s; `shutdown` ohne letzte Runde |
| `TaskContext`, `TaskSnapshot` | Zähler `stored`, `notStored`; Signal „keine neuen Marker“ |
| `PipelineProperties` | Gruppe `pipeline.db.*` mit Prüfungen |
| `pom.xml`, `application.yml` | Abhängigkeiten JDBC, MariaDB-Treiber, Flyway; Datenquelle und `pipeline.db.*` |
| `src/main/resources/db/migration` | `V1__testdaten_ergebnis.sql` |

`ResultStore` als Schnittstelle (wie `CloudClient`) erlaubt Tests mit einer Umsetzung im Speicher, ohne Datenbank.

## 7. Tests

| Ebene | Inhalt |
|---|---|
| Unit | `UserFolders`: Marker schreiben, auflisten, löschen; `.zu_speichern` zählt nicht als Inhalt. `ResultWriter` mit Speicher-Umsetzung: Batch-Bildung nach Anzahl und Größe, Wiederholung nach vorübergehendem Fehler, Isolierung eines dauerhaft fehlerhaften Eintrags, letzte Runde, Restmarker früherer Aufgaben |
| Komponente (`TaskManagerTest`) | Supervisor wartet auf die letzte Runde; Grenze von 30 s (im Test kurz konfiguriert); gestörte Datenbank verzögert den Consumer nicht und bringt keine Datei in die `errorbox` |
| Datenbank | `JdbcResultStore` gegen MariaDB in Testcontainers: idempotentes Einfügen, Batch, Originaltext bleibt unverändert, Fehlereinordnung |
| Integration | Ende-zu-Ende mit MariaDB-Container: alle `SUCCESS`-Inhalte in der Tabelle; Datenbank während der Aufgabe gestoppt: Aufgabe endet trotzdem `COMPLETED`, Marker bleiben, die nächste Aufgabe derselben Art speichert sie nach |
| Black-Box | `PipelineBlackBoxIT` um einen MariaDB-Container erweitern |

Voraussetzung für Testcontainers: Docker auf Entwicklungsrechnern und im Build.

## 8. Dokumentation

README (Konfiguration, Ordner, Ablauf), `funktionsweise_sequenz.md` und `_zh.md` (Abschnitte 3, 6 und 7),
`implementierung.md` und `_zh.md` (neues Paket, Consumer, Supervisor), Diagramme `sandbox_sequenz.mmd` und
`laufzeitstruktur.mmd`, `anforderungen.md` (Speicherung in MariaDB ergänzen).

## 9. Risiken und bekannte Grenzen

| Risiko / Grenze | Einordnung |
|---|---|
| Absturz zwischen dem Verschieben in die `donebox` und dem Schreiben des Markers | Die Datei liegt in der `donebox`, ist aber weder gespeichert noch markiert. Das Zeitfenster beträgt Millisekunden; vergleichbare Fenster gibt es bereits (Verschieben und Löschen des TaskId-Markers). Als bekannte Grenze dokumentieren. |
| Restmarker werden nur verarbeitet, wenn dieselbe Benutzer:in die Art erneut startet | Startet sie diese Art nicht mehr, bleiben die Marker liegen. Bei Bedarf später ein manueller Auslöser (REST-Endpunkt). |
| Belegung bleibt während der letzten Runde bis zu 30 s bestehen | Ein neuer Start derselben Art wartet in dieser Zeit höchstens 10 s und wird sonst mit `409 USER_TASK_RUNNING` abgewiesen. |
| Datensätze erscheinen einige Sekunden nach der Datei in der `donebox` | Durch den Speicher-Abstand (2 s) bedingt; spätestens am Ende der Aufgabe nachgezogen. |
| Sehr große Dateien | Ein Eintrag größer als `max_allowed_packet` kann nicht gespeichert werden; er wird als dauerhaft fehlerhaft isoliert und gemeldet. `max_allowed_packet` der Zieldatenbank prüfen. |
| Viele gleichzeitige Aufgaben | Speicher-Threads teilen sich den Pool von 10 Verbindungen; wegen kurzer Batches unkritisch, bei Bedarf Pool vergrößern. |
| Später mehrere Instanzen (Pods) | Marker liegen im lokalen Dateisystem; bei verteiltem Betrieb gemeinsam mit der geplanten Ablösung der Ordner durch Tabellen neu bewerten. |

## 10. Aufwandsschätzung

Annahmen: eine Entwicklerin oder ein Entwickler mit Kenntnis des Projekts; MariaDB-Instanz, Zugangsdaten und Docker
sind verfügbar; Bereitstellung und Berechtigungen der Datenbank sind nicht enthalten.

| Arbeitspaket | Aufwand (Personentage) |
|---|---|
| Datenbankanbindung: Abhängigkeiten, Konfiguration, Flyway-Migration, `JdbcResultStore` mit Batch, idempotentem Einfügen und Fehlereinordnung | 1,0 |
| Speicher-Marker in `UserFolders`, Anpassung des Consumers | 0,5 |
| `ResultWriter`: Schleife, Batch nach Anzahl und Größe, Wiederholung, Isolierung fehlerhafter Einträge, letzte Runde | 1,0 |
| Sandbox, `TaskManager`, Supervisor: dritter Thread, 30-s-Grenze, Zähler im lokalen Stand, Beenden der Anwendung | 0,75 |
| Tests: Unit, Komponente, Testcontainers-MariaDB, Integration, Black-Box | 1,75 |
| Dokumentation (deutsch und chinesisch) und Diagramme | 1,0 |
| Review, Abnahme, Puffer | 0,5 |
| **Summe** | **6,5 Personentage (etwa 1,5 Wochen)** |

Vorgeschlagene Reihenfolge: Datenbankanbindung → Marker und Consumer → `ResultWriter` → Supervisor → Tests → Dokumentation.
Nach dem dritten Schritt ist die Speicherung bereits lauffähig; Supervisor-Grenze und Tests machen sie belastbar.

Zum Vergleich: Die Ersatzlösung aus Abschnitt 11 kostet je nach Skriptvariante 0,5 bis 2 Personentage.

## 11. Ersatzlösung bei fehlender Kapazität: Import per Skript

### 11.1 Einsatzfall

Kann Variante E mangels Kapazität nicht umgesetzt werden, bleibt die Anwendung unverändert. Nach dem Ende der Aufgaben
importiert eine Person die Dateien aus den `donebox`-Ordnern per Skript in die MariaDB, auch zehntausende auf einmal.
Das Skript lässt sich beliebig oft ausführen; bereits importierte Dateien werden übersprungen.

| Merkmal | Variante E (automatisch) | Ersatzlösung (Skript) |
|---|---|---|
| Auslöser | Speicher-Thread je Aufgabe | manueller Skriptlauf |
| Verzögerung bis zum Datensatz | Sekunden | bis zum nächsten Skriptlauf |
| `task_id`, `job_id` im Datensatz | vorhanden | nicht verfügbar (siehe 11.2) |
| Änderung der Anwendung | ja | nein |
| Aufwand | 6,5 Personentage | 0,5 bis 2 Personentage je Skriptvariante (11.5) |

### 11.2 Verfügbare Angaben

Ohne Änderung der Anwendung enthält die `donebox` nur die Dateien selbst; TaskId und Aufgabennummer werden dort nicht
abgelegt.

| Angabe | Herkunft |
|---|---|
| `user_id`, `job_type` | Pfad `<userId>/<jobType>/donebox/` |
| `datei_name`, Inhalt (Originaltext) | Datei in der `donebox` |
| Zeitpunkt | Änderungszeit der Datei (ungefähr der Zeitpunkt der erfolgreichen Erzeugung) |
| `task_id`, `job_id` | **nicht verfügbar** |

### 11.3 Angepasste Tabelle

Da `task_id` fehlt, kann sie nicht Primärschlüssel sein. Für die Ersatzlösung ersetzt die folgende Tabelle die aus 5.1:

```sql
CREATE TABLE testdaten_ergebnis (
    id             BIGINT       NOT NULL AUTO_INCREMENT,
    task_id        VARCHAR(64)  NULL,      -- beim Skript-Import leer; Variante E füllt sie später
    job_id         VARCHAR(64)  NULL,
    user_id        VARCHAR(64)  NOT NULL,
    job_type       VARCHAR(32)  NOT NULL,
    datei_name     VARCHAR(255) NOT NULL,
    inhalt         JSON         NOT NULL,  -- Originaltext
    inhalt_sha256  CHAR(64)     NOT NULL,
    datei_zeit     TIMESTAMP(3) NOT NULL,
    gespeichert_am TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_testdaten_ergebnis_task (task_id),
    UNIQUE KEY uk_testdaten_ergebnis_datei (user_id, job_type, datei_name, inhalt_sha256)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
```

| Element | Zweck |
|---|---|
| `id` | künstlicher Primärschlüssel |
| `task_id`, `job_id` (nullable) | bleiben beim Skript-Import leer; ein eindeutiger Index erlaubt in MariaDB mehrere `NULL`-Werte |
| `inhalt_sha256` | SHA-256 des Originaltexts |
| `uk_testdaten_ergebnis_datei` | macht den Import wiederholbar: Dieselbe Datei (Benutzer:in, Art, Name, Inhalt) wird nur einmal gespeichert; eine gleichnamige Datei mit anderem Inhalt (spätere Erzeugung) wird als neue Zeile gespeichert |
| `datei_zeit` | Änderungszeit der Datei |

Verträglichkeit mit Variante E: Wird Variante E später doch umgesetzt, kann sie diese Tabelle weiterverwenden und füllt
`task_id` und `job_id`. Ihr `INSERT … ON DUPLICATE KEY UPDATE id = id` greift dann bei beiden eindeutigen Schlüsseln:
Eine Datei, die das Skript bereits importiert hat, wird nicht ein zweites Mal gespeichert. Folge: Wird dieselbe Datei
mit identischem Inhalt in zwei verschiedenen Aufgaben erzeugt, bleibt nur der erste Datensatz erhalten.

### 11.4 Gemeinsame Anforderungen an jedes Skript

Unabhängig von der gewählten Skriptvariante gilt:

1. **Eingabe**: `<base-directory>/<userId>/<jobType>/donebox/`; einschränkbar nach Benutzer:in (`--benutzer`), Art
   (`--art`) und Änderungszeit (`--seit`). Unterordner (`.taskids`, `.fehler`) und andere Ordner (`inbox`,
   `pendingbox`, `errorbox`) werden nicht gelesen.
2. **Inhalt**: Originaltext; UTF-8, UTF-16 und UTF-32 mit BOM werden erkannt. Der Inhalt wird auf gültiges JSON geprüft;
   ungültige Dateien werden übersprungen und im Bericht genannt.
3. **Batches**: höchstens 500 Dateien oder 8 MiB Inhalt je Batch, eine Transaktion je Batch.
4. **Wiederholbar**: Einfügen mit
   ```sql
   INSERT INTO testdaten_ergebnis (user_id, job_type, datei_name, inhalt, inhalt_sha256, datei_zeit)
   VALUES (?, ?, ?, ?, ?, ?)
   ON DUPLICATE KEY UPDATE id = id;
   ```
5. **Fehler**: Bei einem Datenbankfehler wird der laufende Batch zurückgerollt und das Skript beendet; nach Behebung
   wird es einfach erneut gestartet.
6. **Probelauf**: `--dry-run` zählt nur und schreibt nichts.
7. **Bericht** am Ende: neu importiert, bereits vorhanden, übersprungen (mit Grund).
8. **Dateien bleiben in der `donebox`** (Empfehlung). Alternative: nach dem Import in ein Archiv verschieben, z. B.
   `donebox/.importiert/`; das hält die `donebox` klein, macht das Skript aber aufwendiger und fehleranfälliger.

Laufzeit: Zehntausend Dateien ergeben einige Dutzend Batches; ein Lauf dauert je nach Dateigröße und Netz meist wenige
Minuten.

### 11.5 Skriptvarianten

| Variante | Vorgehen | Voraussetzungen | Vorteile | Nachteile | Aufwand (PT) |
|---|---|---|---|---|---|
| **S1: Python, direkt in die Datenbank** | Skript durchläuft die Ordner und schreibt mit PyMySQL in Batches | Python 3 und PyMySQL (`pip install pymysql`) auf einem Rechner mit Zugriff auf die Dateien; Netzwerkzugang zur MariaDB | am einfachsten; gut wartbar; Kodierung und JSON-Prüfung einfach | Python und Treiber müssen installiert sein | 1 |
| **S2: PowerShell, direkt in die Datenbank** | wie S1, mit Windows PowerShell und MySqlConnector (.NET-DLL) | Windows-Rechner mit Zugriff auf die Dateien und Netzwerkzugang zur MariaDB | kein Python nötig; vertraut im Windows-Betrieb | zusätzliche DLL; große Dateien und Kodierungen sind in PowerShell 5.1 umständlicher | 1 – 1,5 |
| **S3: SQL-Datei erzeugen, Ausführung durch DBA** | Skript (Python oder PowerShell) schreibt alle Dateien in eine `.sql`-Datei, Inhalte Base64-kodiert (`CONVERT(FROM_BASE64('…') USING utf8mb4)`); DBA führt sie mit `mariadb < import.sql` aus | Zugriff auf die Dateien und Schreibrecht auf die Datenbank können bei verschiedenen Personen liegen | Lesen und Schreiben getrennt; DBA kann die SQL-Datei vor der Ausführung prüfen; Base64 vermeidet Probleme beim Maskieren | SQL-Datei etwa 1,4-mal so groß wie die Inhalte; bei zehntausenden großen Dateien sehr groß; zusätzliche Übergabe | 1 |
| **S4: TSV erzeugen, `LOAD DATA LOCAL INFILE`** | Skript schreibt alle Dateien zeilenweise in eine TSV-Datei (Inhalt Base64); `LOAD DATA LOCAL INFILE` lädt sie in eine Hilfstabelle; `INSERT … SELECT FROM_BASE64(…)` überträgt in die Zieltabelle | `local_infile` auf Server und Client aktiviert | bei sehr großen Mengen (über hunderttausend Dateien) am schnellsten | zusätzliche Hilfstabelle; `local_infile` ist aus Sicherheitsgründen oft abgeschaltet | 1 – 1,5 |
| **S5: reines SQL mit `LOAD_FILE`** | ein Skript listet die Dateipfade auf und erzeugt `INSERT … LOAD_FILE('/pfad')`; die Datenbank liest die Dateien selbst | Dateien liegen auf dem Datenbankserver; Recht `FILE`; `secure_file_priv` zeigt auf das Datenverzeichnis | kein Treiber nötig | Voraussetzungen fast nie erfüllt; `LOAD_FILE` liefert bei Fehlern still `NULL`, das muss eigens geprüft werden; SQL kann Verzeichnisse nicht selbst auflisten | 0,5 (selten einsetzbar) |
| **S6: Java-Kommandozeilenwerkzeug im Projekt** | eigener Einstiegspunkt wie `FakeCloudBackendApplication`, mit Jackson und JDBC | Rechner, der das Anwendungs-JAR ausführen kann; Netzwerkzugang zur MariaDB | dieselbe Sprache wie das Projekt; Grundlage für eine spätere Umsetzung von Variante E | größter Entwicklungsaufwand; Änderung an Projekt und Build | 1,5 – 2 |

Die Aufwände enthalten Skript, Test gegen eine MariaDB-Instanz und eine kurze Bedienungsanleitung.

### 11.6 Auswahlhilfe

| Situation | Empfohlene Variante |
|---|---|
| Rechner mit Zugriff auf die Dateien darf Python installieren und erreicht die MariaDB | **S1** (Standardempfehlung) |
| nur Windows, keine Installation von Python erlaubt | **S2** |
| Dateizugriff und Schreibrecht auf die Datenbank liegen bei verschiedenen Personen, oder Schreiben in die Datenbank ist dem DBA vorbehalten | **S3** |
| mehr als hunderttausend große Dateien auf einmal, `local_infile` erlaubt | **S4** |
| Dateien liegen auf dem Datenbankserver selbst | **S5** möglich |
| Variante E soll später folgen, und es ist etwas mehr Zeit vorhanden | **S6** |

### 11.7 Offene Punkte der Ersatzlösung

1. Auf welchem Rechner läuft das Skript, und darf dort Python installiert werden?
2. Erreicht dieser Rechner die MariaDB über das Netzwerk?
3. Muss das Schreiben in die Datenbank durch einen DBA erfolgen?
4. Wird die angepasste Tabelle aus 11.3 bestätigt?
5. Bleiben die Dateien nach dem Import in der `donebox`, oder werden sie archiviert?
