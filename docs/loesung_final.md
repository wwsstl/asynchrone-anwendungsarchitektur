# Finale Lösung: Asynchrone Testdaten-Pipeline mit Spring Boot

Dieses Dokument fasst die **verbindlichen Architekturentscheidungen** zusammen. Es
basiert auf der Optionsanalyse in `loesung.md` und den Entscheidungen aus
`feedback_loesung.md`. Anders als `loesung.md` werden hier **keine Alternativen mehr
gegenübergestellt**, sondern jeweils die gewählte Lösung beschrieben — weiterhin ohne
Programmcode; das ist Gegenstand des nächsten Schritts (Paket-/Klassenentwurf).

---

## 1. Getroffene Grundsatzentscheidungen (Übersicht)

| Thema | Entscheidung |
|---|---|
| Java-Version | Java 21 (Virtual Threads verbindlich) |
| Spring-Boot-Version | Spring Boot 4 |
| JSON-Bibliothek | Jackson 3 (in Spring Boot 4 integriert) |
| Thread-Strategie | Virtual Threads durchgängig für Producer- und Verarbeitungs-Workloads |
| Status-Verwaltung | Status-Pool als `ConcurrentHashMap` je Sandbox + dediziertem Consumer-Thread (Virtual Thread) pro Benutzer, der periodisch nur den eigenen Pool prüft (Weiterentwicklung von Option B aus `loesung.md`, Abschnitt 7 — Umsetzung pro Sandbox statt zentral, siehe Abschnitt 4.2, Grund: Wartbarkeit) |
| Cloud-API 2 | Unterstützt Sammel-/Bulk-Statusabfrage mehrerer TaskIds pro Aufruf → wird genutzt |
| Persistenz | Rein In-Memory (Phase 1), keine Datenbank |
| REST-API-Vertrag | Nur Start / Abbruch / Status (kein SSE-Streaming) |
| Web-Stack | Spring WebFlux (Netty) für REST-API und Cloud-Client — ein einheitlicher, reaktiver Stack; kein Spring MVC |
| Framework-Wahl | Ausschließlich eigenes Producer/Consumer/Status-Pool-Modell; **kein** Spring Batch, auch nicht punktuell |
| Registry | `ConcurrentHashMap`-Registry, gekapselt hinter einem eigenen `TaskRegistry`-Interface (spätere Redis-Fähigkeit erhalten; keine Spring-Cache-Abstraktion, siehe Abschnitt 4.3) |
| Cloud-Client | `WebClient` (nicht-blockierend) |
| Dateiverwaltung | Java NIO.2, Ordnerkonvention pro Benutzer |

Alle nachfolgenden Abschnitte beschreiben die daraus resultierende, konsistente
Gesamtarchitektur.

---

## 2. Finale Architekturübersicht

| # | Schicht im Diagramm | Finale Realisierung |
|---|---|---|
| 1 | Externe Steuerungsebene | Reaktiver REST-Controller (Spring WebFlux, `spring-boot-starter-webflux`) mit genau drei Operationen: Start, Abbruch, Status |
| 2 | Task-Scheduling & Isolationszentrum (Task Manager) | Service, der pro Benutzer eine vollständige Sandbox aufbaut: TaskContext, eigener Status-Pool-Bereich, eigener Producer-Thread **und** eigener Consumer-Thread (Details Abschnitt 4.2) |
| 3 | TaskRegistry | `ConcurrentHashMap`-basierte Registry-Bean hinter dem `TaskRegistry`-Interface |
| 4 | Sandbox pro Benutzer | TaskContext (Atomic-Felder) + eigener Status-Pool (`ConcurrentHashMap`) + eigener Producer-Thread + eigener Consumer-Thread, alle als Virtual Threads |
| 5 | Cloud-Dienste / Dateisystem | `WebClient` (inkl. Bulk-Statusabfrage) + Java NIO.2 |

Damit entspricht die finale Umsetzung wieder **1:1 der Struktur des ursprünglichen
Architekturdiagramms**: Jede Sandbox hat weiterhin ihren eigenen „Erzeuger-Thread (A)"
und ihren eigenen „Verbraucher-Überwachungsthread (A)". Der einzige Unterschied zur
naiven Diagramm-Lesart betrifft die interne Datenstruktur des Status-Pools selbst
(`ConcurrentHashMap` statt `DelayQueue`, siehe Abschnitt 4.6) — nicht mehr die
Thread-Topologie.

---

## 3. Technologie-Basis

- **Java 21** — Grundlage für Virtual Threads (`Executors.newVirtualThreadPerTaskExecutor()`
  bzw. Spring Boots eingebaute Unterstützung über `spring.threads.virtual.enabled`).
- **Spring Boot 4** — `spring-boot-starter-parent` 4.1.x löst die frühere Basis
  (3.3.4) ab; Baseline ist weiterhin mit Java 21 kompatibel.
- **Jackson 3** — über `spring-boot-starter-jackson` aus der
  Spring-Boot-4-Abhängigkeitsverwaltung. Jackson 3 liegt in den Packages
  `tools.jackson.*` (Jackson 2: `com.fasterxml.jackson.*`); der Code verwendet
  ausschließlich die neuen Packages.
- **`spring-boot-starter-webflux`** — einziger Web-Stack der Anwendung (Netty): für
  die reaktive REST-Steuerungs-API (Start/Abbruch/Status) und als Basis für
  `WebClient` (nicht-blockierender Cloud-Aufruf). Spring MVC wird nicht verwendet.
- **`spring-boot-webclient`** — die `WebClient`-Autokonfiguration
  (`WebClient.Builder` mit den Jackson-3-Codecs). In Spring Boot 4 ist sie ein
  eigenes Modul, das `spring-boot-starter-webflux` nicht mitbringt; ohne dieses
  Modul fehlt der `WebClient.Builder`, und die Anwendung startet nicht.
- **Kein Spring Batch** — keine entsprechende Abhängigkeit wird aufgenommen.
- **Keine Datenbank/kein Cache-Server** — Phase 1 bleibt vollständig In-Memory; die
  Registry- und Pool-Abstraktionen werden dennoch so geschnitten, dass ein späterer
  Austausch (Redis, DB, MQ) ohne Änderung der Aufrufstellen möglich bleibt.

---

## 4. Komponenten (finaler Zuschnitt)

### 4.1 REST-API

Drei Endpunkte, reaktiv über Spring WebFlux (annotierter Controller, Rückgabe als
`Mono`):

- Aufgabe für einen Benutzer starten
- Aufgabe eines Benutzers abbrechen
- Status/Fortschritt einer Aufgabe abfragen

Kein Streaming-Endpunkt (SSE wurde bewusst nicht aufgenommen). Die Endpunkte rufen
ausschließlich den Task Manager auf und enthalten selbst keine Geschäftslogik.

Der Task Manager arbeitet blockierend (Dateisystem, Sperren, begrenztes Warten auf die
Threads eines gerade abgebrochenen Tasks). Damit der Netty-Event-Loop nie blockiert,
verlagert der Controller jeden Aufruf auf einen eigenen Scheduler, der je Aufruf einen
Virtual Thread startet (`Mono.fromCallable(…).subscribeOn(…)`). Fehler des Task
Managers erreichen als `Mono.error` die zentrale Fehlerabbildung (400/404/409).

### 4.2 Task Manager (mit dediziertem Consumer-Thread pro Sandbox)

**Aufgabe:** Sandbox pro Benutzer vollständig eigenständig aufbauen — inklusive
eines eigenen Consumer-Threads, statt Status-Pools zentral zu bündeln. Diese
Entscheidung wurde bewusst zugunsten der **Wartbarkeit** getroffen: Jede Sandbox ist
dadurch ein in sich geschlossener, unabhängig nachvollziehbarer Baustein — Fehler-
suche, Logging und ggf. spätere Erweiterungen betreffen immer nur die Threads eines
einzelnen Benutzers, ohne eine gemeinsam genutzte Infrastrukturkomponente
mitzudenken.

Finaler Ablauf beim Start einer Aufgabe:

1. `TaskContext` anlegen (Atomic-Felder für Abbruch/Fehlerzähler, Startzeitpunkt für
   Timeout-Prüfung).
2. Eigenen, sandbox-exklusiven Status-Pool anlegen (eigene `ConcurrentHashMap`-
   Instanz, keine gemeinsame Map mit anderen Benutzern).
3. Producer als eigenen Virtual-Thread-Loop starten (liest `inbox`, übermittelt
   Batches an Cloud-API 1, trägt TaskIds in den eigenen Status-Pool ein).
4. **Consumer als eigenen Virtual-Thread-Loop starten** (prüft periodisch nur den
   eigenen Status-Pool, siehe 4.6/4.8) — keine Registrierung bei einer zentralen
   Komponente mehr nötig.
5. Sandbox-Referenz (TaskContext, Pool-Referenz, Thread-Handles) in der TaskRegistry
   ablegen (für Status-Abfragen/Abbruch von außen).

Beim Abbruch/Abschluss einer Aufgabe werden Producer- und Consumer-Thread der
Sandbox beendet. Ein abgeschlossener Task **verbleibt mit seinem Endzustand in der
Registry** (Task Manager und TaskRegistry sind Singletons über die gesamte Laufzeit;
nach *n* Starts enthält die Registry *n* Tasks). Ein Task wird nur durch den **externen
Abbruch** (`POST /api/users/{userId}/tasks/{taskId}/cancel`) aus der Registry gelöscht:
Der Abbruch setzt das Abbruchsignal (Producer und Consumer beenden sich) und entfernt die
`taskId` aus der Registry; danach liefern Status und weiterer Abbruch HTTP 404. Auch ein
bereits beendeter Task lässt sich so löschen.
Pro Benutzer läuft höchstens ein Task gleichzeitig, da es nur eine `inbox` gibt
(zweiter Start → HTTP 409).

**Bewertung:** Ein dedizierter Consumer-Thread pro Benutzer wäre bei klassischen
Thread-Pools (Plattform-Threads) potenziell teuer gewesen (viele gleichzeitig
blockierte/wartende Threads bei vielen aktiven Benutzern, vgl. die Abwägung in
`loesung.md`, Abschnitt 11). Da durchgängig **Virtual Threads** eingesetzt werden,
entfällt dieser Nachteil weitgehend: Ein zusätzlicher, überwiegend schlafender
Virtual Thread pro Benutzer verursacht keinen nennenswerten Ressourcen-Overhead.
Die Entscheidung für den dedizierten Thread ist damit sowohl aus Wartbarkeits- als
auch aus Ressourcensicht tragfähig.

### 4.3 TaskRegistry

`ConcurrentHashMap`-basierte Ablage aktiver Sandbox-Referenzen, gekapselt hinter dem
schmalen, eigenen Interface `TaskRegistry` (`register`, `find`, `remove`, `all`,
`size`); die Endzustände beendeter Tasks hält analog `TaskHistory`. Rein In-Memory in
Phase 1 (Entscheidung bestätigt); die Interfaces bleiben bestehen, damit ein späterer
Wechsel auf einen verteilten Speicher (z. B. Redis) keine Änderung an den
Aufrufstellen erfordert.

Die ursprünglich vorgesehene Spring-Cache-Abstraktion (`loesung.md`, Abschnitt 4,
Option B) wird bewusst **nicht** verwendet: Das Register hält lebende Sandboxen mit
laufenden Threads, die weder serialisierbar sind noch sinnvoll in einem verteilten
Cache liegen können. Spring Cache ist für wiederherstellbare Werte gedacht, deren
Verlust nur einen erneuten Aufruf kostet; ein Eintrag im Register ist dagegen die
einzige Referenz auf einen laufenden Task. Ein fachliches Interface beschreibt die
benötigten Operationen genauer und lässt sich ebenso gegen eine Redis-Implementierung
tauschen.

### 4.4 TaskContext

Pro Sandbox ein eigenständiges Objekt (kein Spring-Singleton) mit:

- `AtomicBoolean` für das Abbruchsignal,
- `AtomicInteger` für den Fehlerzähler inkl. Schwellenwert-Prüfung,
- Startzeitpunkt zur Timeout-Erkennung.

Die Timeout-Prüfung wird **in den periodischen Zyklus des sandbox-eigenen Consumer-
Threads integriert** (siehe 4.6/4.8): Bei jedem Durchlauf prüft der Consumer-Thread
zusätzlich zum Status-Pool auch, ob die maximale Laufzeit seiner eigenen Sandbox
überschritten ist, und setzt in diesem Fall das Abbruchsignal. Ein separater
`ScheduledExecutorService` pro Aufgabe ist damit nicht nötig — die Prüfung läuft
„nebenbei" im ohnehin vorhandenen Consumer-Loop mit.

### 4.5 Cloud-Client

`WebClient` für beide Cloud-Aufrufe:

- **Cloud-API 1** (Erstellungsauftrag übermitteln): ein Aufruf pro Producer-Batch.
- **Cloud-API 2** (Statusabfrage): **Bulk-Aufruf** mit mehreren TaskIds gleichzeitig,
  da die Cloud-API dies laut Entscheidung unterstützt (siehe 4.6).

Timeout- und Retry-Konfiguration je Aufrufart, unabhängig vom fachlichen
Fehlerzähler im `TaskContext`.

### 4.6 Status-Pool (finale Umsetzung)

- Datenstruktur: **eigene** `ConcurrentHashMap<TaskId, PollEntry>` je Sandbox
  (Referenz auf Ursprungsdatei, nächster Prüfzeitpunkt, bisherige Versuche) — keine
  gemeinsame, benutzerübergreifende Map.
- Kein zentraler Scheduler: Der in 4.2/4.8 beschriebene, sandbox-eigene
  Consumer-Thread (Virtual Thread) führt selbst in einer Schleife periodisch einen
  Sweep über seinen eigenen Pool durch (kurze Wartezeit zwischen den Durchläufen,
  danach fällige Einträge sammeln).
- Die pro Durchlauf gesammelten, fälligen TaskIds werden **in einem einzigen
  Bulk-Aufruf** an Cloud-API 2 übergeben (Entscheidung: Bulk-Statusabfrage wird
  unterstützt und genutzt). Da der Bulk-Aufruf ohnehin nur die TaskIds der eigenen
  Sandbox enthält, ist die Isolation zwischen Benutzern hier automatisch gegeben.
- Ergebnisbehandlung (Datei verschieben, Fehlerzähler erhöhen, Eintrag entfernen oder
  mit neuem Prüfzeitpunkt im Pool belassen) erfolgt im selben Consumer-Thread bzw.
  bei Bedarf in weiteren, von diesem Thread angestoßenen Virtual Threads, da das
  Verschieben von Dateien blockierende NIO.2-Operationen sind.
- „Status unverändert" (`PENDING`) erfordert keine Sonderbehandlung: Der Eintrag
  bleibt einfach unverändert im Pool, bis ein Endzustand erreicht ist oder das
  `TaskContext`-Abbruchsignal gesetzt wird.

### 4.7 Producer

- Liest die `inbox` batchweise mit Java NIO.2 (`Files.newDirectoryStream`/`Files.walk`).
- Konvertiert Dateien mit Jackson 3 nach JSON.
- Übermittelt jeden Batch nicht-blockierend über `WebClient` an Cloud-API 1.
- Trägt die zurückgegebenen TaskIds in den sandbox-eigenen Bereich des Status-Pools
  ein und liest **sofort weiter**, unabhängig davon, wie viele TaskIds aus früheren
  Batches im Pool noch offen sind (kein Warten auf Verarbeitung vorheriger Batches).
- Läuft als eigener Virtual-Thread-Loop pro Sandbox; prüft vor jedem Zyklus das
  Abbruchsignal im `TaskContext`.

### 4.8 Consumer (dedizierter Thread pro Sandbox)

Entspricht direkt der im Diagramm beschriebenen Rolle „Verbraucher-
Überwachungsthread (A)": ein eigener Virtual Thread je Sandbox, der in einer
Schleife periodisch den eigenen Status-Pool prüft (siehe 4.6), Cloud-API 2 per
Bulk-Aufruf befragt und die Ergebnisse verarbeitet:

- `SUCCESS` → Datei per NIO.2 nach `donebox` verschieben, Eintrag aus Pool entfernen.
- `ERROR` → Datei nach `errorbox` verschieben, Fehlerzähler im `TaskContext` erhöhen,
  Schwellenwert prüfen (ggf. Abbruchsignal setzen), Eintrag aus Pool entfernen.
- `PENDING` → Eintrag bleibt im Pool, nächster Prüfzeitpunkt wird aktualisiert.
- Timeout (siehe 4.4) → wie `ERROR` behandelt.

### 4.9 Lokale Dateiverwaltung

Ordnerkonvention `<basisverzeichnis>/<userId>/{inbox,errorbox,donebox}`, automatisches
Anlegen bei erstem Zugriff, Zugriff ausschließlich über NIO.2. Kapselung hinter einem
`UserFolderResolver`, um eine spätere Ablösung durch DB-Statusfelder (nicht Teil von
Phase 1) nicht zu blockieren.

### 4.10 Thread-/Ressourcenmodell (final)

- **Virtual Threads durchgängig** für: Producer-Thread und Consumer-Thread je
  Sandbox, sowie für die Ergebnisverarbeitung nach einer Statusabfrage (Datei
  verschieben, Pool-/Context-Update).
- **Kein zentraler Scheduler und kein klassischer `ThreadPoolTaskExecutor`** mit
  fester Poolgröße für die Fachlogik nötig — jede Sandbox erhält ihre eigenen zwei
  Virtual Threads (Producer, Consumer), vollständig unabhängig von anderen
  Sandboxen.
- **REST-Aufrufe:** Der Netty-Event-Loop nimmt die Anfragen entgegen; die
  blockierenden Aufrufe des Task Managers laufen je Anfrage auf einem eigenen Virtual
  Thread (Reactor-Scheduler über `Executors.newVirtualThreadPerTaskExecutor()`,
  siehe 4.1), ebenfalls ohne feste Poolgröße.
- Die in `loesung.md` (Abschnitt 11) diskutierte Sorge vor „vielen Threads bei
  vielen gleichzeitigen Benutzern" bezog sich auf klassische Plattform-Threads und
  ist mit Virtual Threads nicht mehr relevant — ein zusätzlicher, überwiegend
  schlafender Consumer-Thread pro Benutzer ist ressourcenseitig unkritisch und
  bringt dafür einen klaren Wartbarkeitsvorteil (siehe 4.2).

---

## 5. Explizit verworfene Alternativen (Nachvollziehbarkeit)

Damit die Entscheidung nachvollziehbar bleibt, hier die aus `loesung.md`
verworfenen Optionen mit Kurzbegründung:

| Verworfene Option | Grund |
|---|---|
| `DelayQueue` als Status-Pool | Passt schlecht zu „Status unverändert → weiter prüfen" und zu unabhängig eintreffenden Batches; serielle Entnahme durch einen Consumer-Thread limitiert Durchsatz |
| Zentraler Sweep-Scheduler (statt Consumer-Thread pro Sandbox) | Zunächst erwogen, um Thread-Anzahl zu reduzieren; aus Wartbarkeitsgründen zugunsten eines dedizierten Consumer-Threads je Sandbox verworfen (siehe 4.2) — dank Virtual Threads ohne relevanten Ressourcennachteil |
| Spring Batch (auch punktuell, z. B. nur `ItemReader`) | Chunk-orientiertes Modell passt nicht zum unabhängig weiterlaufenden Producer; Entscheidung: komplett eigenes Modell |
| Spring MVC für die Steuerungs-API | Zunächst gewählt, später zugunsten eines einheitlichen Web-Stacks durch WebFlux ersetzt: Der Cloud-Client (`WebClient`) setzt ohnehin auf WebFlux, zwei Web-Stacks nebeneinander entfallen |
| Sofortige DB-/Redis-Anbindung | Phase 1 bleibt In-Memory; Abstraktionen (`TaskRegistry`, `UserFolderResolver`, Pool-Interface) halten die Tür offen |
| Spring-Cache-Abstraktion für die Registry | Ungeeignet für lebende Sandboxen mit Threads; stattdessen eigenes `TaskRegistry`-Interface (siehe 4.3) |

---

## 6. Nächste Schritte

1. `pom.xml` aktualisieren: `spring-boot-starter-parent` auf Spring-Boot-4-Version
   heben, Jackson-3-Kompatibilität prüfen (Artefakt-/Package-Koordinaten), Java-Version
   bei 21 belassen.
2. Paket-/Klassenstruktur entwerfen (Task Manager, TaskRegistry, TaskContext,
   Status-Pool je Sandbox, Producer-Thread, Consumer-Thread, Cloud-Client,
   Dateiverwaltung, REST-Controller).
3. Konfigurationswerte festlegen (Sweep-Intervall, Batch-Größe des Producers,
   Fehlerschwellenwert, Timeout-Dauer).
4. Erst danach: Beginn der Implementierung (Code) auf Basis dieses Dokuments.
