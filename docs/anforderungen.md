### Funktionale Anforderungen (Funktionale Anforderungen)

- Nutzung von Cloud-Diensten: Das System muss Cloud-Dienstschnittstellen aufrufen, um die Testdaten zu generieren.

- Lokale Dateiverwaltung: Jeder Benutzer muss über einen spezifisch zugewiesenen, lokalen Ordner auf dem Server verfügen. Dieser Ordner muss zwingend die drei Unterverzeichnisse inbox, errorbox und donebox enthalten, um die Testdatendateien zu speichern.

- Stapelverarbeitung (Batch-Processing): Die Benutzer stellen die zu generierenden Testdaten im inbox-Verzeichnis bereit. Die Anwendung muss diese Dateien stapelweise aus der inbox lesen, in das JSON-Format konvertieren und über den Cloud-Dienst verarbeiten, bis alle Dateien vollständig abgearbeitet sind.

- Parallele Statusüberwachung und Dateiverschiebung: Es ist erforderlich, dass ein separater Prozess (oder Thread) innerhalb der Aufgabe gleichzeitig den Generierungsstatus der Cloud-Dienste überwacht. Erfolgreich generierte Dateien müssen in die donebox verschoben werden. Dateien, bei denen während der Generierung Fehler aufgetreten sind, müssen in die errorbox verschoben werden.

- Unabhängige Benutzeraufgaben: Jeder Benutzer muss in der Lage sein, seine eigenen Verarbeitungsaufgaben zu starten. Diese Aufgaben müssen vollständig voneinander unabhängig ablaufen und dürfen sich gegenseitig in keiner Weise beeinflussen.

- Externer Abbruch: Wenn bei der Generierung der Testdaten zu viele Fehler auftreten oder es zu einer Zeitüberschreitung (Timeout) kommt, muss das System dem Benutzer die Möglichkeit bieten, seine Aufgabe von außen manuell zu beenden.


### Nicht-funktionale Anforderungen (Nicht-funktionale Anforderungen)

### 1. Leistungseffizienz (Performance Efficiency)

- Nicht-blockierende Netzwerkkommunikation: Für die Kommunikation mit der Cloud-API muss zwingend ein nicht-blockierender HTTP-Client (wie Spring WebFlux WebClient oder der native Java HttpClient ab Version 11) eingesetzt werden. Dies verhindert, dass der aufrufende Produzenten-Thread blockiert wird, wodurch er sofort weitere Dateien aus dem Ordner lesen kann.

- Hochperformante I/O-Operationen: Für Dateioperationen (wie das stapelweise Scannen der Ordner und das Verschieben der Dateien) muss Java NIO.2 (z. B. Files.walk(), Files.move()) verwendet werden, da dies eine wesentlich höhere Leistung als traditionelle IO-Methoden bietet.

- Effiziente JSON-Verarbeitung: Zur schnellen und sicheren Serialisierung der eingelesenen Testdatendateien in das JSON-Format ist die in Spring Boot integrierte Jackson-Bibliothek zu verwenden.

- Thread-Management: Um die Ressourcen des Servers effizient zu nutzen und die Nebenläufigkeit zu maximieren, sollten Thread-Pools (ThreadPoolTaskExecutor) oder – bei Nutzung von Java 21 – "Virtual Threads" (virtuelle Threads) für die Konsumenten- und Produzentenprozesse konfiguriert werden.


### 2. Erweiterbarkeit (Extensibility)

- Entkoppelte Architektur: Das System muss streng nach dem Produzenten-Konsumenten-Muster (Producer-Consumer Pattern) entworfen werden. Dies entkoppelt das Auslesen und Senden der Daten (Produzent) vollständig von der Statusüberwachung und dem Dateiverschieben (Konsument), wodurch beide Schichten unabhängig voneinander erweitert werden können.

- Zentrales Task-Management: Das Design verlangt ein zentrales Aufgabenregister ("TaskRegistry", z. B. als ConcurrentHashMap), welches gekapselte Aufgabenkontexte (TaskContext) verwaltet. Dies ermöglicht die einfache Erweiterung um neue externe Steuerungs-Schnittstellen (APIs) oder neue Abbruchbedingungen (z.B. neue Timeout-Regeln).

- Framework-Evolution: Die Architektur ist so zu gestalten, dass bei wachsender Komplexität ein reibungsloser Übergang auf das Spring Batch Framework (mit Komponenten wie ItemReader, ItemProcessor, ItemWriter) möglich ist, ohne die eigentliche Kernlogik drastisch anpassen zu müssen.


### 3. Skalierbarkeit (Scalability)

- Dynamische Ressourcenisolierung (Vertikale Skalierung): Auf dem Einzelserver muss das System über ein "Sandbox-Muster" skalieren. Jedem neuen Benutzer-Task wird zur Laufzeit dynamisch ein vollständig isolierter TaskContext (mit AtomicBoolean für Abbruchsignale und AtomicInteger für Fehlerzähler), eine eigene TestdatenAnlegenStatusPool im Speicher sowie dedizierte Threads zugewiesen.

- Vorbereitung für horizontale Verteilung: Für eine zukünftige Verteilung über mehrere Instanzen (Container/Pods) muss das Design vorsehen, dass die lokalen Dateisystem-Ordner (inbox, donebox, errorbox) durch relationale Datenbanktabellen mit entsprechenden Statusfeldern (INBOX, DONE, ERROR) abgelöst werden können.

- Austauschbare Middleware-Komponenten: Die interne Speicher-Warteschlange (TestdatenAnlegenStatusPool) ist so zu abstrahieren, dass sie künftig durch eine verteilte Message Queue (MQ wie RabbitMQ oder RocketMQ) ersetzt werden kann. Ebenso muss das lokale In-Memory-Register für Aufgaben bei einer Cluster-Skalierung in ein verteiltes Cache-System (wie Redis) überführt werden können, damit Abbruchsignale über alle Container hinweg wirksam werden.


### 4. Bereitstellung (Deployment)

- Leichtgewichtiger Single-Server-Betrieb: In der initialen Phase muss die Anwendung als eigenständige, leichtgewichtige Spring Boot-Applikation auf einem einzelnen Server bereitgestellt werden.

- Verzicht auf externe Middleware: Die Bereitstellung erfolgt zunächst gänzlich ohne externe Middleware; es werden stattdessen ausschließlich JVM-Standardbibliotheken (wie ConcurrentHashMap und TestdatenAnlegenStatusPool) genutzt, um die Aufgabensteuerung und Isolierung lokal zu gewährleisten.

- Lokale Dateiverwaltung: Im aktuellen Deployment-Szenario verwaltet und mappt die Anwendung physisch die exklusiven Ordner (inbox, donebox, errorbox) der einzelnen Benutzer direkt auf dem Server.

- Cloud-Native Vorbereitung (Containerisierung): Obwohl das System aktuell als Monolith läuft, garantiert die Entkopplung der Architektur (Trennung von Status, Produzent und Konsument) eine Container-Ready-Struktur, die später ohne komplette Neuentwicklung in Kubernetes- oder Docker Swarm-Umgebungen bereitgestellt werden kann.