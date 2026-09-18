# Lösungsskizze: Asynchrone Testdaten-Pipeline mit Spring Boot

Dieses Dokument leitet aus den funktionalen und nicht-funktionalen Anforderungen
(`anforderungen.md`) sowie dem Architekturdiagramm
(`asynchrone-anwendungsarchitektur_deu.puml`) mögliche Lösungsbausteine ab. Es werden
für jede Komponente Implementierungsoptionen mit Vor-/Nachteilen aufgelistet, damit auf
Basis dieses Dokuments eine Technologie-/Musterentscheidung getroffen werden kann.
**Es ist bewusst noch kein Code enthalten.**

---

## 1. Grobstruktur der Lösung

Das Diagramm beschreibt fünf logische Schichten, die 1:1 auf Spring-Boot-Bausteine
abgebildet werden können:

| # | Schicht im Diagramm | Verantwortlichkeit | Mögliche Spring-Boot-Realisierung |
|---|---|---|---|
| 1 | Externe Steuerungsebene (UserA/UserB) | Start/Abbruch von Aufgaben von außen | REST-Endpunkte (`@RestController`) |
| 2 | Task-Scheduling & Isolationszentrum (Task Manager) | Erzeugt/verwaltet Sandboxen pro Benutzer | Service-Bean, orchestriert Executor + Registry |
| 3 | TaskRegistry | Globales Verzeichnis aktiver Aufgaben | `ConcurrentHashMap`-basierte Registry-Bean (Singleton) |
| 4 | Sandbox pro Benutzer (TaskContext, Queue, Producer, Consumer) | Isolierte Ausführung je Benutzer | Pro Task instanziierte POJOs, keine Spring-Singletons |
| 5 | Cloud-Dienste / lokales Dateisystem | Externe Schnittstellen | `WebClient` (WebFlux) + Java NIO.2 |

Die folgenden Abschnitte behandeln jede Komponente einzeln und stellen Optionen zur
Auswahl.

---

## 2. REST-API-Schicht (Externe Steuerungsebene)

**Aufgabe laut Anforderung:** Aufgaben pro Benutzer starten, Status abfragen, extern
abbrechen (funktional: "Unabhängige Benutzeraufgaben", "Externer Abbruch").

Optionen:

- **Option A – Spring MVC (`spring-boot-starter-web`)**
  Klassische, synchrone Controller-Schicht für Start-/Abbruch-/Status-Endpunkte.
  Einfach, gut bekannt, ausreichend, da die eigentliche Arbeit asynchron in
  Hintergrund-Threads läuft und der Request selbst nur „Start"/„Stop" auslöst.
- **Option B – Reaktive Controller (WebFlux, `RouterFunction`/`@RestController` mit
  `Mono`/`Flux`)**
  Konsequent nicht-blockierend bis in die API-Schicht; sinnvoll, falls später auch
  Status-Streaming (z. B. Server-Sent Events) angeboten werden soll.
- **Empfehlung:** Da `spring-boot-starter-web` und `spring-boot-starter-webflux`
  bereits gemeinsam in der `pom.xml` vorgesehen sind, kann Option A für die
  Steuerungs-API und WebFlux gezielt nur für den Cloud-Client (Abschnitt 6) genutzt
  werden – das vermeidet die Komplexität eines vollständig reaktiven Stacks, erfüllt
  aber die Anforderung „nicht-blockierender HTTP-Client".

Endpunkt-Kandidaten:
- Aufgabe starten (`POST /users/{userId}/tasks`)
- Aufgabe abbrechen (`POST /users/{userId}/tasks/{taskId}/cancel`)
- Status/Fortschritt abfragen (`GET /users/{userId}/tasks/{taskId}`)

---

## 3. Task Manager (Aufgaben-Scheduling & Isolationszentrum)

**Aufgabe:** Sandbox pro Benutzer dynamisch anlegen, TaskContext/Queue/Producer/
Consumer verdrahten, in Registry eintragen.

Optionen zur Umsetzung als Spring-Bean:

- **Option A – Zentraler „TaskOrchestrator"-Service**
  Ein Singleton-Service, der bei Eingang eines Start-Requests eine neue Sandbox-Instanz
  baut (TaskContext, Queue, Producer-Runnable, Consumer-Runnable) und diese über
  `ThreadPoolTaskExecutor`/Virtual-Thread-Executor startet. Entspricht direkt dem
  Diagrammschritt „TaskManager → ContextA/QueueA/ProducerA/ConsumerA".
- **Option B – Factory + Registry getrennt**
  Eine `TaskSandboxFactory` erzeugt die Sandbox-Objekte, eine separate
  `TaskRegistryService` verwaltet nur die Ablage/Lookup. Bessere Trennung der
  Verantwortlichkeiten (Erzeugen vs. Verwalten), leichter unit-testbar.
- **Empfehlung:** Option B, da sie mit der Anforderung „Zentrales Task-Management" und
  „Framework-Evolution" (späterer Umstieg auf Spring Batch) besser harmoniert – Factory
  und Registry lassen sich unabhängig austauschen.

---

## 4. TaskRegistry (Globales Aufgabenregister)

**Aufgabe:** Alle aktiven Sandbox-Referenzen prozessweit auffindbar machen (für Status-
Abfragen und externen Abbruch).

Optionen:

- **Option A – `ConcurrentHashMap<String, TaskContext>` als Spring-Singleton-Bean**
  Entspricht exakt der Anforderung „Verzicht auf externe Middleware" in der initialen
  Deployment-Phase. Schlüssel z. B. `userId` oder `taskId`.
- **Option B – Caching-Abstraktion (Spring Cache Abstraction, `ConcurrentMapCacheManager`)**
  Gleiche Grundlage wie A, aber hinter Spring-Cache-Interface gekapselt, was den
  späteren Wechsel auf einen verteilten Cache (Redis) erleichtert, ohne die
  Aufrufstellen zu ändern.
- **Empfehlung:** Option B als schlanke Zwischenschicht – erfüllt sowohl die
  Anforderung „Leichtgewichtiger Single-Server-Betrieb" als auch „austauschbare
  Middleware-Komponenten" (spätere Cluster-Skalierung über Redis), ohne dass zum
  jetzigen Zeitpunkt eine externe Abhängigkeit hinzukommt.

---

## 5. TaskContext (Isolierter Zustand je Benutzer/Sandbox)

**Aufgabe:** Kapselt Abbruchsignal, Fehlerzähler und Metadaten einer Aufgabe, komplett
unabhängig von anderen Benutzer-Sandboxen.

Optionen für die Zustandsfelder:

- **Abbruchsignal:** `AtomicBoolean cancelled` – wird vom Producer/Consumer vor jedem
  Zyklus geprüft (kooperative Cancellation, kein hartes Thread-`interrupt()` nötig,
  aber optional zusätzlich als Sicherheitsnetz einsetzbar).
- **Fehlerzähler:** `AtomicInteger errorCount` mit konfigurierbarem Schwellenwert
  (Circuit-Breaker-artiges Abbruchkriterium bei „zu vielen Fehlern").
- **Timeout-Überwachung:**
  - Option A: Start-Zeitstempel (`Instant`) im TaskContext, periodische Prüfung durch
    Consumer oder einen separaten Watchdog-Task.
  - Option B: `ScheduledExecutorService`, der nach Ablauf der Timeout-Dauer das
    Abbruchsignal selbst setzt.
- **Empfehlung:** Kombination aus AtomicBoolean/AtomicInteger (Pflicht laut
  Anforderung) plus Option B für Timeout, da dies das Abbruchverhalten unabhängig vom
  Consumer-Zyklus zuverlässig auslöst.

Dieses Objekt wird **nicht** als Spring-Bean verwaltet, sondern pro Task manuell
instanziiert – das erfüllt die geforderte „dynamische Ressourcenisolierung" (jede
Sandbox ist ein eigenständiges Objekt, kein geteilter Singleton-Zustand).

---

## 6. Cloud-Anbindung (Producer → Cloud-API 1, Consumer → Cloud-API 2)

**Aufgabe:** Nicht-blockierende Kommunikation mit den beiden Cloud-Endpunkten
(Generierung übermitteln / Status abfragen).

Optionen:

- **Option A – Spring WebFlux `WebClient`**
  In der `pom.xml` bereits vorgesehen. Bietet integrierte Timeout-, Retry- und
  Backpressure-Unterstützung; passt zur Anforderung „nicht-blockierender HTTP-Client".
- **Option B – Java 11+ `HttpClient` (nativ, asynchron via `sendAsync`)**
  Keine zusätzliche Abhängigkeit nötig, ebenfalls nicht-blockierend, aber weniger
  komfortable Integration mit Spring (z. B. Retry/Backoff müsste selbst gebaut werden).
- **Empfehlung:** Option A (`WebClient`), da bereits als Abhängigkeit eingebunden und
  explizit in den Anforderungen als Beispiel genannt; ermöglicht zudem deklaratives
  Retry/Timeout-Handling für die Statusabfrage (Consumer) und für den Übermittlungs-
  Aufruf (Producer).

Ergänzende Bausteine:
- **Timeout-Konfiguration** pro Aufruf (z. B. via `WebClient`-`Timeout`-Filter), um das
  Timeout-Kriterium aus den Anforderungen technisch abzubilden.
- **Retry/Backoff** (`reactor-retry` bzw. `Mono.retryWhen`) für transiente Fehler bei
  Cloud-API-Aufrufen, unabhängig vom fachlichen Fehlerzähler im TaskContext.

---

## 7. Interne Zustandsverwaltung (TestdatenAnlegenStatusPool)

### 7.1 Präzisierung des Problems (Rückmeldung eingearbeitet)

Eine reine „Warteschlange" im engeren Sinn (FIFO, ein Element wird konsumiert und ist
danach weg) bildet das tatsächliche Verhalten nur unvollständig ab:

- Ein Datensatz mit Status `PENDING` ist **nicht abgearbeitet**, sondern muss so lange
  erneut geprüft werden, bis ein Endzustand (`SUCCESS`/`ERROR`/Timeout) erreicht ist.
  Das Element „verlässt" das System also nicht nach einer Abfrage, sondern bleibt
  fachlich weiter aktiv – das ist eher das Verhalten eines **Pools aktiver Aufgaben**
  als das einer Warteschlange abgearbeiteter Nachrichten.
- Auch die Namensgebung im Diagramm/den Anforderungen
  („TestdatenAnlegen**Status*Pool*"", nicht „…Queue") deutet bereits darauf hin, dass
  eine verwaltete Menge gleichzeitig offener Aufgaben gemeint ist und keine reine
  Durchlauf-Warteschlange.
- Zusätzlich verschärft die zweite Rückmeldung das Bild: Der Producer übermittelt
  batchweise **mehrere Datensätze pro Cloud-Aufruf** und macht anschließend sofort mit
  dem nächsten Batch weiter, unabhängig davon, wie viele Datensätze aus früheren
  Batches im Pool noch offen sind. Der Pool muss also mit einer **wachsenden, variabel
  großen Menge gleichzeitig offener, voneinander unabhängiger Prüfaufträge**
  zurechtkommen – nicht mit einer strikten Reihenfolge „Erster rein, erster raus".

Diese Beobachtungen sprechen dafür, den Begriff „Warteschlange" für diese Komponente
zu verlassen und stattdessen von einem **Status-Pool** (aktive Menge, keyed nach
TaskId) zu sprechen, dessen Einträge unabhängig voneinander wiederholt geprüft werden,
bis sie einen Endzustand erreichen.

### 7.2 Optionen für die Umsetzung des Status-Pools

- **Option A – `DelayQueue<Delayed>` (ursprünglicher Vorschlag)**
  Funktioniert korrekt, solange „PENDING → mit neuer Verzögerung wieder einfügen"
  sauber implementiert wird (Element wird nicht verworfen, sondern mit neuem
  Fälligkeitszeitpunkt erneut eingefügt). Grenzen:
  - Ein einzelner Consumer-Thread, der per `take()` blockierend das jeweils fällige
    Element entnimmt, verarbeitet die Cloud-Abfragen faktisch **seriell** – bei vielen
    gleichzeitig fälligen Einträgen (z. B. nach Ablauf eines gemeinsamen Delays für
    einen ganzen Batch) entsteht eine Warteschlange von Abfragen, die trotz
    nicht-blockierendem `WebClient` durch die serielle Entnahme künstlich verzögert
    wird.
  - Die Semantik „ein Element wird entnommen, geprüft und ggf. wieder eingefügt" muss
    für jeden Aufrufer selbst nachgebaut werden; das Konzept eines dauerhaft
    „offenen" Datensatzes ist der Datenstruktur selbst nicht inhärent.
- **Option B – Status-Pool als `ConcurrentHashMap<TaskId, PollEntry>` mit
  periodischem Sweep (empfohlen)**
  Alle offenen Datensätze liegen als Einträge (TaskId, Dateireferenz, nächster
  Prüfzeitpunkt, bisherige Versuche) in einer nebenläufigen Map. Ein einzelner
  Scheduler (`@Scheduled(fixedDelay=…)` oder `ScheduledExecutorService.
  scheduleAtFixedRate(...)`) durchläuft in jedem Takt alle fälligen Einträge und stößt
  für jeden (nicht-blockierend, parallel über `WebClient`/`Flux.merge`) eine
  Statusabfrage an.
  - „Status unverändert" wird damit **trivial** abgebildet: Der Eintrag bleibt
    einfach unverändert in der Map, bis ein Endzustand eintritt – es ist keine
    explizite Re-Insert-Logik nötig, wie sie eine Queue erfordert.
  - Neue Batches des Producers fügen einfach weitere Einträge hinzu; es gibt keine
    Reihenfolgeabhängigkeit zwischen „alten" und „neuen" Batches, was genau dem in
    der Rückmeldung beschriebenen Verhalten (Producer läuft weiter, unabhängig vom
    Fortschritt älterer Batches im Pool) entspricht.
  - Begünstigt zusätzlich eine spätere Optimierung: Falls Cloud-API 2 eine
    Sammel-/Batch-Statusabfrage (mehrere TaskIds pro Aufruf) unterstützt, lässt sich
    das im Sweep-Schritt leicht nutzen (alle fälligen TaskIds gruppieren, in einem
    Aufruf abfragen), was bei Option A (DelayQueue, Einzel-Entnahme) nicht ohne
    Weiteres möglich wäre.
- **Option C – Hybrid: Map als „Source of Truth" + `DelayQueue`/`ScheduledExecutorService`
  nur als Weckmechanismus**
  Die eigentlichen Aufgabendaten liegen in einer Map (wie Option B); zusätzlich wird
  pro Eintrag ein leichter „Weck"-Marker mit Fälligkeitszeitpunkt in einer
  `DelayQueue` geführt, um ohne periodisches Scannen der gesamten Map exakt dann
  aktiv zu werden, wenn wirklich ein Eintrag fällig ist. Effizienter bei sehr vielen
  gleichzeitig offenen, aber selten fälligen Einträgen – dafür architektonisch
  komplexer (zwei Datenstrukturen müssen konsistent gehalten werden).
- **Option D – Pro Eintrag ein eigener verzögerter Task**
  (`ScheduledExecutorService.schedule(...)` bzw. bei Virtual Threads ein eigener,
  schlafender Thread pro offenem Datensatz). Sehr einfach pro Einzelfall, aber ohne
  zentrale Übersicht über „alle aktuell offenen Datensätze" – erschwert Reporting,
  Abbruch aller Einträge einer Sandbox sowie eine mögliche Batch-Statusabfrage.

**Empfehlung:** Option B (Map-basierter Status-Pool mit periodischem, nicht-
blockierendem Sweep) passt am besten zu den präzisierten Anforderungen:
Er bildet „Status unverändert → weiter prüfen" ohne Sonderlogik ab, verträgt sich
mit dem batchweisen, weiterlaufenden Producer, und hält die Tür für spätere
Sammel-Statusabfragen sowie für den Austausch gegen eine verteilte Lösung
(Anforderung „austauschbare Middleware-Komponenten") offen – die Map müsste dazu
nur hinter einem schmalen Interface (z. B. „StatusPollingPool") gekapselt werden,
damit sie später z. B. durch Redis (verteilte Map) oder eine Message Queue ersetzt
werden kann. Option A bleibt als einfachere Alternative dokumentiert, sollte aber
nur gewählt werden, wenn die Anzahl gleichzeitig offener Datensätze pro Sandbox
klein und die serielle Abarbeitung akzeptabel ist.

---

## 8. Producer (Erzeuger-Thread pro Benutzer)

**Aufgabe:** Inbox zyklisch scannen, Dateien nach JSON konvertieren, nicht-blockierend
an Cloud-API 1 übermitteln, TaskId in die Queue einreihen.

Bausteine/Optionen:

- **Datei-Scan:** Java NIO.2 (`Files.walk()`/`Files.newDirectoryStream()`), wie in den
  Anforderungen explizit gefordert – Alternative (klassisches `File.listFiles()`) ist
  laut Anforderung ausdrücklich **nicht** vorgesehen.
- **JSON-Konvertierung:** in Spring Boot integriertes Jackson (`ObjectMapper`,
  `spring-boot-starter-web`/`webflux` bringt es bereits mit) – keine Alternative
  erforderlich, da explizit vorgegeben.
- **Ausführung als Thread/Task:**
  - Option A: `Runnable`/`Callable`, submitted an einen `ThreadPoolTaskExecutor`
    (klassische Thread-Pools, konfigurierbare Poolgröße).
  - Option B: Java 21 Virtual Threads (`Executors.newVirtualThreadPerTaskExecutor()`),
    da `java.version` in der `pom.xml` bereits auf 21 gesetzt ist.
  - **Empfehlung:** Virtual Threads (Option B) für Producer/Consumer, da sie sich
    ideal für die vielen kurzlebigen, blockierungsarmen I/O-wartenden Aufgaben eignen
    und die Anforderung „Thread-Management … bei Nutzung von Java 21" direkt referenziert.
- **Vorabprüfung Abbruchsignal:** Producer prüft vor jedem Dateizyklus
  `taskContext.isCancelled()`, bevor eine neue Datei gelesen/übermittelt wird.

---

## 9. Consumer (Verbraucher-/Überwachungsthread pro Benutzer)

**Aufgabe:** Queue abfragen, Cloud-API 2 (Status) aufrufen, Datei je nach Ergebnis
verschieben, Fehlerzähler/Timeout im TaskContext pflegen.

Optionen:

- **Ausführung:** analog Producer – Virtual-Thread- oder ThreadPoolTaskExecutor-
  basiert; beide Threads (Producer/Consumer) sollten aus demselben, pro Sandbox
  konfigurierten Executor stammen, um die geforderte Isolation zwischen Benutzern zu
  gewährleisten (kein geteilter Pool über Benutzergrenzen hinweg oder – falls ein
  gemeinsamer Pool aus Ressourcengründen genutzt wird – mit sauber isoliertem
  TaskContext pro Aufgabe).
- **Ergebnis-Routing:**
  - SUCCESS → Datei nach `donebox` (NIO.2 `Files.move()`, atomar wo möglich)
  - ERROR/Timeout → Datei nach `errorbox`, `errorCount` im TaskContext erhöhen,
    Schwellenwert prüfen → ggf. Abbruchsignal setzen
  - PENDING → Element mit verlängerter Verzögerung erneut in die Queue einfügen
- **Schwellenwert-/Abbruchlogik:** zentral im TaskContext gekapselt (siehe Abschnitt 5),
  damit Producer und Consumer dieselbe Abbruchentscheidung konsistent auslesen können.

---

## 10. Lokale Dateiverwaltung (inbox/donebox/errorbox)

**Aufgabe:** Pro Benutzer isolierte Ordnerstruktur auf dem Server.

Optionen:

- **Option A – Konventionsbasierte Pfadstruktur**
  z. B. `<basisverzeichnis>/<userId>/{inbox,errorbox,donebox}`, beim ersten Zugriff
  eines Benutzers automatisch angelegt (`Files.createDirectories`).
- **Option B – Konfigurierbares Mapping** (z. B. über `application.yml` oder eine
  kleine Zuordnungstabelle), falls Benutzerordner nicht zwingend nach Konvention liegen
  müssen.
- **Empfehlung:** Option A für die aktuelle Phase (einfach, deckt „Lokale
  Dateiverwaltung"-Anforderung direkt ab); Abstraktion hinter einem
  „UserFolderResolver"-Interface, damit die Anforderung „Vorbereitung für horizontale
  Verteilung" (Ablösung durch DB-Statusfelder statt Ordnerstruktur) später ohne
  Änderung der aufrufenden Logik möglich ist.

---

## 11. Thread- und Ressourcenmanagement (übergreifend)

Optionen zur Konfiguration:

- **Option A – Ein globaler `ThreadPoolTaskExecutor`, geteilt über alle Sandboxen**
  Einfacher zu konfigurieren, aber Gefahr der gegenseitigen Beeinflussung bei
  Ressourcenknappheit (widerspricht teilweise „Aufgaben … dürfen sich gegenseitig
  nicht beeinflussen").
- **Option B – Pro Sandbox eigener, kleiner Executor (2 Threads: Producer+Consumer)**
  Stärkere Isolation, aber potenziell viele Threads bei vielen gleichzeitigen
  Benutzern (klassische Thread-Pools).
- **Option C – Virtual Threads (Java 21) pro Task, kein klassischer Pool nötig**
  Da Virtual Threads sehr günstig sind, kann pro Sandbox unbeschränkt (oder mit
  einfachem Semaphore-Limit) je ein Virtual Thread für Producer und Consumer erzeugt
  werden – löst das Isolations-/Skalierungsproblem von Option B ohne Pool-Tuning.
- **Empfehlung:** Option C, konsistent mit der pom.xml (Java 21) und der
  Anforderung „virtuelle Threads … für Konsumenten- und Produzentenprozesse".

---

## 12. Vertiefung: Eignung von Spring Batch für dieses Szenario

Die Anforderungen nennen Spring Batch (`ItemReader`/`ItemProcessor`/`ItemWriter`)
ausdrücklich als möglichen späteren Entwicklungspfad. Die zweite Rückmeldung
beschreibt jedoch genau die Stelle, an der dieses Modell nicht ohne Weiteres passt:

> Nachdem der Producer eine Anfrage (mehrere Datensätze) gesendet hat, sendet er die
> nächste Anfrage, obwohl sich noch Aufgaben aus der vorigen Anfrage im Status-Pool
> befinden.

**Warum das mit dem klassischen Spring-Batch-Modell in Spannung steht:**

- Spring Batch ist grundsätzlich **chunk-orientiert**: Ein Step liest einen Chunk
  (`ItemReader`), verarbeitet ihn (`ItemProcessor`) und schreibt ihn (`ItemWriter`) –
  danach gilt dieser Chunk als abgeschlossen, bevor der nächste beginnt. Das Modell
  ist auf **endliche, überschaubare Verarbeitungszeiten pro Chunk** ausgelegt.
- In unserem Fall liegt zwischen „Datensatz an Cloud übermitteln" (schnell) und
  „Cloud hat Datensatz fertig generiert" (dauert unbestimmt lange, ggf. Minuten,
  mit Wiederholungen) eine **große, unvorhersehbare Zeitspanne**. Ein `ItemWriter`,
  der auf dieses Ergebnis wartet, würde den Step faktisch blockieren oder den
  Producer künstlich ausbremsen – wogegen genau die Anforderung „Producer nicht
  blockieren, direkt mit dem nächsten Batch weitermachen" spricht.
- Spring Batch bietet zwar `AsyncItemProcessor`/`AsyncItemWriter`
  (aus `spring-batch-integration`), die einzelne Verarbeitungsschritte als
  `Future` zurückgeben. Der `AsyncItemWriter` wartet aber am Ende des jeweiligen
  Chunks auf alle offenen Futures dieses Chunks (`future.get()`), bevor der Chunk als
  abgeschlossen gilt – das entkoppelt also nur innerhalb eines Chunks, nicht über
  Chunk-Grenzen hinweg. Ein „Chunk N+1 startet, während Chunk N noch auf Cloud-Status
  wartet" ist damit nicht das vorgesehene Verhalten, sondern erfordert zusätzliche,
  nicht triviale Konstrukte (z. B. Partitioning mit vielen parallel laufenden Steps
  oder ein komplett custom `TaskletStep`, der intern wieder unsere eigene
  Producer/Consumer-Logik kapselt – womit der Nutzen von Spring Batch schrumpft).
- Spring Batch ist zudem primär für **abgegrenzte Jobausführungen** gedacht (ein Job
  läuft, bis der Reader erschöpft ist, und terminiert). Unsere Aufgaben sind dagegen
  pro Benutzer **lang laufende, kontinuierlich überwachte Prozesse** mit externem
  Abbruch und offenem Ende (Timeout/Fehlerschwelle) – das passt eher zum bereits
  gewählten Producer-Consumer-Modell als zu einem klassischen Batch-Job-Lebenszyklus.

**Einschätzung / Empfehlung:**

- Ein **vollständiger Umstieg** auf Spring Batch für die gesamte Pipeline (inkl.
  asynchroner Status-Überwachung) wird **nicht empfohlen** – die Kernanforderung
  „Producer läuft unabhängig vom Fortschritt der Status-Prüfung weiter" lässt sich
  im chunk-orientierten Modell nur mit erheblichem Zusatzaufwand und Verlust an
  Klarheit abbilden.
- Falls perspektivisch dennoch Spring-Batch-Konzepte genutzt werden sollen, kommt
  vor allem der **lesende Teil** in Frage: Das Scannen der `inbox` und das
  Zusammenstellen von Batches ließe sich als `ItemReader` modellieren, während die
  Cloud-Übermittlung weiterhin außerhalb eines starren Chunk-Zyklus erfolgt und die
  Status-Überwachung wie in Abschnitt 7 (Status-Pool) beschrieben **bewusst separat**
  vom Batch-Job-Lebenszyklus bleibt.
- Die in den Anforderungen genannte „Framework-Evolution" sollte daher nicht als
  „vollständiger Ersatz der Architektur durch Spring Batch", sondern als
  **optionale, punktuelle Wiederverwendung einzelner Spring-Batch-Bausteine**
  interpretiert werden, sofern sich das im weiteren Verlauf überhaupt als sinnvoll
  erweist. Die entsprechende Zeile in Abschnitt 13 (Anforderungs-Mapping) ist unten
  entsprechend präzisiert.

---

## 13. Abbildung auf nicht-funktionale Anforderungen

| Anforderung | Vorgeschlagene Lösung |
|---|---|
| Nicht-blockierende Netzwerkkommunikation | `WebClient` (Abschnitt 6) |
| Hochperformante I/O | Java NIO.2 für Scan/Move (Abschnitte 8, 10) |
| Effiziente JSON-Verarbeitung | Jackson (Spring-Boot-integriert) |
| Thread-Management | Virtual Threads je Sandbox (Abschnitt 11) |
| Entkoppelte Architektur | Producer/Consumer getrennt über Queue (Abschnitte 7–9) |
| Zentrales Task-Management | Registry + Factory (Abschnitte 3–4) |
| Framework-Evolution zu Spring Batch | Nur punktuell sinnvoll (z. B. `ItemReader` für Inbox-Scan); vollständiger Umstieg **nicht empfohlen**, siehe Abschnitt 12 |
| Dynamische Ressourcenisolierung | Pro-Task TaskContext/Queue/Executor, keine Singleton-Zustände (Abschnitt 5) |
| Vorbereitung horizontale Verteilung | `UserFolderResolver`-Abstraktion austauschbar gegen DB-Tabelle mit Statusfeldern |
| Austauschbare Middleware | Queue hinter Interface (Abschnitt 7) → später RabbitMQ/RocketMQ; Registry hinter Cache-Abstraktion (Abschnitt 4) → später Redis |
| Leichtgewichtiger Single-Server-Betrieb | Keine externen Abhängigkeiten in Phase 1 (nur JDK/Spring-Boot-Bordmittel) |
| Cloud-Native Vorbereitung | Klare Schichtentrennung ermöglicht spätere Containerisierung ohne Neuentwurf |

---

## 14. Offene Entscheidungen für die nächste Abstimmung

1. Executor-Strategie: Virtual Threads durchgängig oder hybrides Modell
   (ThreadPoolTaskExecutor als Fallback/Begrenzung)?
2. Status-Pool-Implementierung: Map + periodischer Sweep (Option B, empfohlen),
   reine `DelayQueue` (Option A) oder Hybrid mit Weckmechanismus (Option C)?
   Hängt u. a. davon ab, mit wie vielen gleichzeitig offenen Datensätzen pro
   Sandbox realistisch zu rechnen ist.
3. Unterstützt Cloud-API 2 eine **Sammel-Statusabfrage** (mehrere TaskIds pro
   Aufruf)? Falls ja, verstärkt das den Vorteil von Option B (Abschnitt 7) und
   sollte beim Sweep-Intervall/Batching-Design berücksichtigt werden.
4. Persistenzstrategie für Statusinformationen: rein in-memory (Phase 1) oder bereits
   jetzt optionale DB-Tabelle für Monitoring/Restart-Fähigkeit vorsehen?
5. Wie granular soll der REST-API-Vertrag sein (nur Start/Abbruch/Status oder auch
   Fortschritts-Streaming per SSE)?
6. Soll Spring Batch überhaupt (auch nur punktuell, z. B. für den Inbox-Scan)
   einbezogen werden, oder bleibt die Pipeline vollständig auf dem eigenen
   Producer/Consumer/Status-Pool-Modell aufgebaut (siehe Abschnitt 12)?

Nach Klärung dieser Punkte kann die konkrete Klassen-/Paketstruktur sowie die
Implementierung (Controller, Services, TaskContext, Producer/Consumer-Klassen)
ausgearbeitet werden.
