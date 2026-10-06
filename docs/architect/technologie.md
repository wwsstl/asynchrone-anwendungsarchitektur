# Technologie-Stack

Welche Technologien die Testdaten-Pipeline einsetzt und warum. Grundlage sind die Dokumente in diesem Ordner: die Anforderungen ([`anforderungen.md`](anforderungen.md)), der Ablauf ([`funktionsweise_sequenz.md`](funktionsweise_sequenz.md)) und die Diagramme ([`anwendungsarchitektur.mmd`](anwendungsarchitektur.mmd), [`anwendungsarchitektur_user.mmd`](anwendungsarchitektur_user.mmd), [`funktionsweise_sequenz.mmd`](funktionsweise_sequenz.mmd), [`sandbox_sequenz.mmd`](sandbox_sequenz.mmd)). Versionen laut `pom.xml`.

## Überblick

| Bereich | Technologie | Einsatz |
|---|---|---|
| Laufzeit | Java 21 (LTS) | gesamte Anwendung |
| Anwendungsrahmen | Spring Boot 4.1 | eigenständige Anwendung, Konfiguration, Abhängigkeiten |
| REST-API | Spring WebFlux auf Netty | Start, Abbruch und Status einer Aufgabe |
| Cloud-Anbindung | Spring `WebClient` | Aufrufe von Cloud-API 1, 2 und 3 |
| Nebenläufigkeit | Virtual Threads | Producer und Consumer je Sandbox, Aufrufe des TaskManagers aus der REST-API |
| Zustand im Speicher | `ConcurrentHashMap`, `AtomicBoolean`, `AtomicInteger` | TaskRegistry, TaskHistory, Status-Pool, Aufgabenkontext |
| JSON | Jackson 3 | Batch-Anfragen an Cloud-API 1, Antworten der Cloud-APIs, REST-Antworten |
| Dateizugriff | Java NIO.2 | Ordner `inbox`, `pendingbox`, `donebox`, `errorbox` je Benutzer:in |
| Dokumentation | Mermaid (`.mmd`) | Architektur- und Sequenzdiagramme |

## Begründung je Technologie

### Java 21

Java 21 ist die aktuelle LTS-Version und bringt Virtual Threads als reguläres Sprachmittel mit (JEP 444). Die Anforderungen nennen Virtual Threads für die Produzenten- und Konsumentenprozesse ausdrücklich als Option „bei Nutzung von Java 21“ (Leistungseffizienz, Thread-Management). Erst damit wird das Sandbox-Modell mit eigenen Threads je Aufgabe ohne Ressourcensorgen möglich (siehe Virtual Threads).

### Spring Boot 4

Die Anwendung soll als „eigenständige, leichtgewichtige Spring Boot-Applikation auf einem einzelnen Server“ laufen (Bereitstellung). Spring Boot liefert dafür:

- ein ausführbares Artefakt mit eingebettetem Server, ohne separaten Anwendungsserver;
- Dependency Injection für die Bausteine (TaskManager, TaskRegistry, TaskHistory, Cloud-Client);
- typsichere Konfiguration aus `application.yml`, z. B. Batchgröße, Rückstau-Schwelle, Zeitlimits und Aufbewahrungsfrist;
- die integrierte Jackson-Bibliothek, die die Anforderungen ausdrücklich verlangen (Leistungseffizienz, JSON-Verarbeitung).

### Spring WebFlux für die REST-API

Die REST-API nimmt die Befehle der Benutzer:innen entgegen: Aufgabe starten, abbrechen, Status abfragen (`funktionsweise_sequenz.md`, Abschnitte 1, 4 und 5). Sie antwortet sofort, z. B. mit `202 Accepted`, während die Aufgabe asynchron weiterläuft.

WebFlux wird gewählt, weil der Cloud-Client (`WebClient`) ohnehin auf WebFlux beruht. Damit gibt es nur **einen** Web-Stack statt Spring MVC und WebFlux nebeneinander.

Der TaskManager arbeitet blockierend (Dateisystem, Sperren, Warten auf auslaufende Threads). Die REST-API reicht jeden Aufruf deshalb auf einem Virtual Thread weiter, damit der Netty-Event-Loop nie blockiert (`funktionsweise_sequenz.md`, Abschnitt 1).

### `WebClient` für die Cloud-Dienste

Die Anforderungen verlangen „zwingend einen nicht-blockierenden HTTP-Client (wie Spring WebFlux WebClient oder der native Java HttpClient ab Version 11)“, damit der Produzenten-Thread nicht blockiert (Leistungseffizienz). Gewählt ist `WebClient`:

- **Ein Stack:** Er gehört zu demselben Stack wie die REST-API, und Spring Boot konfiguriert ihn mit den Jackson-Codecs vor.
- **Timeouts und Wiederholungen je Schnittstelle:** Das lässt sich direkt ausdrücken. Wichtig ist das, weil Cloud-API 1 nicht idempotent ist (`funktionsweise_sequenz.md`, Abschnitt 2). Übermittlungen an Cloud-API 1 werden deshalb nie automatisch wiederholt (`submit-retries: 0`); Statusabfragen an Cloud-API 2 dagegen schon.
- **Gebündelte Aufrufe:** Er deckt alle drei Schnittstellen ab. Cloud-API 1 nimmt je Batch eine JSON-Anfrage entgegen, und für die ganze Aufgabe entsteht nur ein batchgenAuftrag. Cloud-API 2 bekommt eine Bulk-Statusabfrage je Durchlauf, Cloud-API 3 setzt den Auftragsstatus am Ende.

### Virtual Threads

Jede Aufgabe erhält eine eigene Sandbox mit einem Producer- und einem Consumer-Thread (`sandbox_sequenz.mmd`). Das verlangen die Anforderungen:

- **Getrennter Überwachungsthread:** Ein separater Thread überwacht den Status parallel (Funktionale Anforderungen).
- **Isolierte Aufgaben:** Jede Aufgabe läuft unabhängig (Funktionale Anforderungen) und erhält dedizierte Threads (Skalierbarkeit, Sandbox-Muster).

Beide Threads warten fast nur – auf Cloud-Antworten, auf den Rückstau oder auf das nächste Abfrageintervall. Ein wartender Virtual Thread belegt keinen Betriebssystem-Thread; zwei Threads je Aufgabe kosten daher praktisch nichts, auch bei vielen Benutzer:innen. Ein `ThreadPoolTaskExecutor` (die Alternative laut Anforderungen) müsste dagegen passend dimensioniert werden, und wartende Aufgaben würden Pool-Threads belegen.

### `ConcurrentHashMap` und Atomics im Speicher

Phase 1 kommt ohne externe Middleware aus und nutzt „ausschließlich JVM-Standardbibliotheken (wie ConcurrentHashMap …)“ (Bereitstellung). Eingesetzt werden sie für:

- **TaskRegistry:** laufende Aufgaben. Die Anforderungen verlangen ein zentrales Aufgabenregister, z. B. als `ConcurrentHashMap` (Erweiterbarkeit).
- **TaskHistory:** Endzustände beendeter Aufgaben, abfragbar für `task-retention` (`funktionsweise_sequenz.md`, Abschnitt 4).
- **Status-Pool:** je Sandbox ein eigener Pool übermittelter TaskIds (Skalierbarkeit, Sandbox-Muster).
- **Aufgabenkontext:** Abbruchsignal als `AtomicBoolean`, Fehlerzähler als `AtomicInteger` – so steht es wörtlich in den Anforderungen (Skalierbarkeit).

Diese Strukturen sind threadsicher ohne eigene Sperren; Producer, Consumer und REST-API greifen gleichzeitig darauf zu. Registry, Historie und Pool liegen hinter eigenen Schnittstellen. Später können an ihre Stelle Redis, eine Message Queue oder Datenbanktabellen treten, ohne die Aufrufer zu ändern (Skalierbarkeit, Austauschbare Middleware-Komponenten).

### Jackson 3

Die Anforderungen verlangen „die in Spring Boot integrierte Jackson-Bibliothek“ für die schnelle und sichere JSON-Verarbeitung (Leistungseffizienz). Jackson serialisiert:

- die Batch-Anfragen an Cloud-API 1 (die JSON-Daten mehrerer Dateien in einer Anfrage);
- die Antworten der Cloud-APIs, z. B. welche Dateien mit welcher TaskId umgewandelt wurden;
- die Aufgabenstatus der REST-API.

Spring Boot 4 bringt Jackson 3 mit (Packages `tools.jackson.*`).

### Java NIO.2

Die Anforderungen verlangen NIO.2 (z. B. `Files.move()`) für das stapelweise Lesen und Verschieben der Dateien (Leistungseffizienz). Jede Datei durchläuft die Ordner der Benutzer:in: `inbox` → `pendingbox` → `donebox` oder `errorbox`.

`Files.move` verschiebt innerhalb eines Dateisystems durch Umbenennen, ohne die Daten zu kopieren. Eine Datei liegt so immer in genau einem Ordner, und der Ordner zeigt ihren Zustand. Das trägt die Wiederaufnahme nach einem Abbruch: Dateien in der `pendingbox` mit TaskId-Marker werden weiter abgefragt, statt erneut übermittelt (`funktionsweise_sequenz.md`, Abschnitt 2).

### Mermaid für die Diagramme

Die Diagramme liegen als Text (`.mmd`) im Repository. Sie werden mit dem Code versioniert, sind in Pull Requests als Diff lesbar und werden auf GitHub direkt dargestellt.

## Bewusst nicht eingesetzt

| Technologie | Grund |
|---|---|
| Spring MVC / Tomcat | Ein Web-Stack genügt; WebFlux ist für den `WebClient` ohnehin nötig. |
| Java `HttpClient` | Laut Anforderungen gleichwertig zulässig; `WebClient` fügt sich besser in Spring Boot ein (Codecs, Konfiguration, Wiederholungen). |
| `ThreadPoolTaskExecutor` | Virtual Threads kosten je Aufgabe praktisch nichts und müssen nicht dimensioniert werden. |
| Spring Batch | Nicht eingesetzt; die Kernlogik (Lesen, Übermitteln, Verschieben) ist so geschnitten, dass ein späterer Übergang möglich bleibt (Erweiterbarkeit, Framework-Evolution). |
| Message Queue (RabbitMQ, RocketMQ), Redis, Datenbank | Phase 1 läuft ohne externe Middleware auf einem Server (Bereitstellung). Die Schnittstellen von Status-Pool, Registry und Ordnerverwaltung halten den Wechsel offen; für Phase 2 empfohlen ist eine relationale Datenbank statt Message Queue und Redis (siehe Ausblick). |

## Ausblick: Phase 2

Für die Verteilung auf mehrere Instanzen (Container/Pods, Kubernetes oder Docker Swarm) sehen die Anforderungen Ersatz für die Speicherstrukturen vor (Skalierbarkeit, Bereitstellung). Empfohlen wird, alles in **einer relationalen Datenbank** zu führen:

| Heute | Laut Anforderungen | Empfehlung |
|---|---|---|
| Benutzerordner `inbox`, `pendingbox`, `donebox`, `errorbox` | relationale Tabellen mit Statusfeldern (INBOX, DONE, ERROR) | ebenso; zusätzlich Statuswerte für „beansprucht“ und „übermittelt“ |
| Status-Pool im Speicher | verteilte Message Queue | Tabelle `datei` in derselben Datenbank |
| TaskRegistry im Speicher | verteilter Cache (Redis) | Tabelle `batchauftrag` in derselben Datenbank; die laufende Sandbox bleibt im Speicher des zuständigen Pods |
| TaskHistory im Speicher | – | Zeilen der Tabelle `batchauftrag` mit Endzustand |

### Status-Pool → Tabelle `datei`

```sql
datei (
  task_id, dateiname,   -- Primärschlüssel
  status,               -- INBOX | BEANSPRUCHT | UEBERMITTELT | DONE | ERROR
  cloud_task_id,        -- ersetzt den TaskId-Marker in der pendingbox
  naechste_pruefung,    -- ersetzt PollEntry.nextPollAt
  versuche
)
```

| Aufgabe des Status-Pools | Umsetzung in der Tabelle |
|---|---|
| TaskId eintragen | Zeile auf `UEBERMITTELT` setzen, `cloud_task_id` schreiben |
| fällige Einträge holen | `WHERE naechste_pruefung <= now()`; bei mehreren Pods mit `FOR UPDATE SKIP LOCKED`, sodass jede Zeile genau ein Pod bearbeitet |
| `PENDING`: später erneut prüfen | `naechste_pruefung` fortschreiben |
| Rückstau (Poolgröße) | `COUNT(*)` je Aufgabe und Status |

Eine Message Queue passt hier schlechter:

- **Erneut prüfen:** „Status unverändert, später erneut prüfen“ geht nur über verzögerte Neuzustellung; jede `PENDING`-Antwort wird zu einem weiteren Nachrichtenumlauf.
- **Rückstau:** Nachrichten lassen sich nicht je Aufgabe zählen.
- **Bulk-Abfrage:** Nachrichten lassen sich nicht gesammelt als „alle fälligen“ abholen, wie es die Bulk-Abfrage an Cloud-API 2 braucht.

Aus denselben Gründen wurde schon `DelayQueue` als Status-Pool verworfen.

### TaskRegistry → Tabelle `batchauftrag` plus lokaler Speicher

Die TaskRegistry hält heute lebende Sandboxen mit Threads. Diese lassen sich weder in einer Datenbank noch in Redis ablegen. Die Registry wird deshalb geteilt:

- **In der Datenbank:** Zustand der Aufgabe, Benutzer:in, Start- und Endzeit, Abbruchwunsch (`cancel_requested`) und zuständiger Pod (Lease).
- **Im Speicher des zuständigen Pods:** die laufende Sandbox, wie heute.

Daraus folgt:

- **Abbruch über alle Pods:** Die REST-API setzt `cancel_requested`. Der zuständige Pod liest die Spalte in jedem Durchlauf (`sweep-interval`). Für schnellere Reaktion eignet sich z. B. `LISTEN/NOTIFY` (PostgreSQL).
- **Eine Aufgabe je Benutzer:in:** ein partieller Unique-Index, z. B. auf `user_id` für laufende Aufgaben, statt einer Sperre im Speicher.
- **TaskHistory:** entfällt als eigene Struktur. Beendete Aufgaben sind Zeilen mit Endzustand; `task-retention` wird zu einer Löschregel und übersteht Neustarts.

### Vorteile gegenüber Message Queue und Redis

- **Weniger Komponenten:** eine Datenbank statt Message Queue und Redis.
- **Zustand und TaskId in einer Transaktion:** Die Lücken der Marker-Dateien (verwaiste oder halb geschriebene Marker) entfallen.
- **Direkt abfragbar:** Der Aufgabenstatus lässt sich auch ohne die Pipeline abfragen.

### Zu beachten

- **Abfragelast:** Jede laufende Aufgabe fragt je `sweep-interval` fällige Zeilen ab. Nötig sind ein Index auf `(status, naechste_pruefung)` und angemessene Intervalle.
- **Abbruch nicht sofort:** Er wirkt erst im nächsten Durchlauf, also nach Sekunden; Redis Pub/Sub wäre schneller. Für den manuellen Abbruch genügt das in der Regel.
- **Keine gemeinsame Transaktion mit dem Dateisystem:** Solange die Dateien im Dateisystem liegen, braucht es eine feste Reihenfolge und wiederholbare Schritte ([`batchauftrag_verwaltung_durch_cloud_api.md`](../batchauftrag_verwaltung_durch_cloud_api.md), Abschnitt 2.8).
- **Anforderungen anpassen:** Phase 1 bleibt ohne Middleware. Für Phase 2 muss der Wortlaut „Message Queue und Redis“ in [`anforderungen.md`](anforderungen.md) geändert werden; einen Vorschlag enthält [`architekturalternative_abgleichsschleife.md`](../architekturalternative_abgleichsschleife.md), Abschnitt 6.
- **Cloud-Dienst mit eigenem Dateistatus:** Führt der Cloud-Dienst Auftrag und Dateistatus selbst (Entwurf in [`batchauftrag_verwaltung_durch_cloud_api.md`](../batchauftrag_verwaltung_durch_cloud_api.md)), wird die Tabelle `datei` schlanker.

Die heutige Trennung in REST-API, TaskManager und Sandbox mit Producer und Consumer bleibt dabei erhalten; ausgetauscht werden nur die Implementierungen hinter den Schnittstellen.
