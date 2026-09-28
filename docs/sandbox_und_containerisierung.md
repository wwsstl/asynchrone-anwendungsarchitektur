# Sandbox-Modell und künftige Containerisierung

> **Fragestellung 1:** In der aktuellen Architektur wird die Aufgabe jedes Benutzers innerhalb einer Sandbox ausgeführt; eignet sich dieses Vorgehen für eine künftige Containerisierung, oder sollten bereits jetzt Anpassungen vorgenommen werden?
>
> **Fragestellung 2 (Architekturentwurf mit Datenbank):** Künftig wird das System durch eine Datenbank gestützt. Jede Benutzeraufgabe wird in einer Tabelle als `batchauftrag` abgebildet, die Daten der zu verarbeitenden Dateien samt Status in separaten Tabellen, verknüpft über `batchauftrag_id`. Benutzer fragen den Status direkt aus der Datenbank ab.
>
> **Ergänzung:** Den `batchauftrag` und die Unterdatensätze der Dateien legt der **Cloud-API-1-Dienst** an und schreibt deren Status fort. Der `InboxProducer` sendet die Anfragen, der `StatusConsumer` in der Sandbox **liest die Dateistatus nur** und verändert sie nicht.

Die ausführliche Herleitung zur Ergänzung steht in `batchauftrag_verwaltung_durch_cloud_api.md`. Der externe Abbruch ist in `abbruch_im_containerbetrieb.md` beschrieben (dort noch mit Redis; im Entwurf hier übernimmt der Cloud-Dienst diese Rolle).

## Kurzantwort

**Ja, die Sandbox-Architektur unterstützt die Containerisierung, und der Entwurf ist tragfähig.**

Das Hauptproblem der heutigen Architektur ist, dass der gesamte Zustand einer Sandbox nur im Speicher der JVM liegt und bei jedem Neustart eines Pods verloren geht. Der Entwurf löst das: Der **Cloud-API-1-Dienst ist der einzige Schreiber** für `batchauftrag` und die Dateistatus. Die Sandbox wird zum **nahezu zustandslosen Ausführer**. Sie übermittelt Dateien, liest Status und verschiebt Dateien im Dateisystem. Alles, was sie nach einem Neustart braucht, steht in der Datenbank oder im Dateisystem.

Die Rollen:

| Rolle | Zuständig | Ort |
|---|---|---|
| Auftrag und Dateistatus (Quelle der Wahrheit) | Cloud-API-1-Dienst | Datenbank des Dienstes |
| Übermitteln, Status lesen, Dateien verschieben | Sandbox (Producer, Consumer) | genau ein Pod |
| Dateiinhalte, Ergebnis des Verschiebens | Sandbox | gemeinsames Volume |

Damit der Entwurf trägt, braucht er vier Ergänzungen:

1. **Rückmeldung des Auftragsstatus.** Abschluss, Abbruch, Zeitüberschreitung und Fehlerschwelle entscheidet die Sandbox. Sie muss sie dem Dienst melden (Abschluss-Endpunkt), sonst steht der Auftrag in der Datenbank dauerhaft auf „läuft“. Das berührt die Regel „der Consumer ändert keine Dateistatus“ nicht.
2. **Owner und Lease.** Der Dienst muss wissen, welcher Pod einen Auftrag bearbeitet, damit nach einem Absturz genau ein anderer Pod übernimmt.
3. **Synchrone Anlage des `batchauftrag` in `TaskManager.start`** mit einer von der Pipeline erzeugten ID, nicht erst im Producer. Sonst fehlen bei `202` die ID und die 409-Prüfung.
4. **Statuslesen über Cloud-API 2** statt per SQL auf fremde Tabellen.

## Was an der heutigen Sandbox bereits passt

| Merkmal | Fundstelle | Bedeutung für den Entwurf |
|---|---|---|
| Vollständige Isolation je Task | `context/Sandbox.java`, `TaskManager.launch` | Ein `batchauftrag` entspricht genau einer Sandbox auf genau einem Pod. |
| TaskId als UUID | `TaskContext.taskId()` | Wird als `batchauftrag.id` an den Dienst übergeben und macht die Anlage idempotent. |
| Abstraktion der Cloud-Anbindung | `cloud/CloudClient.java` | Der Entwurf verändert vor allem diese Schnittstelle; Producer und Consumer bleiben in ihrer Rolle. |
| Abstraktion des Status-Pools | `pool/StatusPool.java` | Der Pool wird zum schlanken lokalen Zwischenspeicher für den Rückstau, nach Neustart aus Cloud-API 2 befüllbar. |
| Zyklischer Consumer | `StatusConsumer.loop()` | Eine Abfrage pro Durchlauf liefert Dateistatus, Abbruchwunsch und verlängert nebenbei die Lease. |
| Abbruch über ein Signal | `TaskContext.cancel()` / `awaitCancel(...)` | Meldet der Dienst `abbruchAngefordert`, genügt lokal `cancel(USER_REQUEST)`. |
| Virtuelle Threads | `application.yml` | Zwei Threads pro Auftrag kosten fast nichts. Die Pipeline braucht keine eigene Datenbankverbindung, also auch keinen Verbindungspool. |
| Konfiguration | `config/PipelineProperties.java` (`@ConfigurationProperties("pipeline")`) | Per Umgebungsvariable überschreibbar (`PIPELINE_BASE_DIRECTORY`, `PIPELINE_CLOUD_BASE_URL` …). |

## Was heute an den Prozess gebunden ist und wohin es wandert

| Heutiger Zustand | Heute | Im Entwurf | Schreiber |
|---|---|---|---|
| Register aller Tasks | `InMemoryTaskRegistry` | `batchauftrag`; lokal bleibt nur `SandboxRegistry` für die Threads | Cloud-Dienst |
| „Ein Task pro Benutzer“ | `TaskManager.latestByUser` | partieller Unique-Index beim Dienst, Anlage liefert **409** | Cloud-Dienst |
| Bereits übermittelte Dateien | `InboxProducer.seen` | Unterdatensätze + Ordner `processing/` | Cloud-Dienst / Sandbox |
| Offene Cloud-TaskIds | `InMemoryStatusPool` | Unterdatensätze, über Cloud-API 2 abrufbar | Cloud-Dienst |
| Verarbeitungsstatus je Datei | Antwort von Cloud-API 2 | Unterdatensätze | Cloud-Dienst |
| Zähler (`succeeded`, `failed` …) | `TaskContext` | aus den Unterdatensätzen berechnet | – |
| Endzustand, `cancelReason`, „Producer fertig“ | `TaskContext` | `batchauftrag`, per Abschluss-Endpunkt gemeldet | Sandbox meldet, Dienst schreibt |
| Abbruchwunsch des Benutzers | `TaskManager.cancel` | `batchauftrag.abbruch_angefordert`, gelesen über Cloud-API 2 | Dienst |
| Besitzer-Pod, Lease | implizit: die JVM | `owner`, `last_seen_at` beim Auftrag, per Polling verlängert | Dienst |
| Datei nach `donebox`/`errorbox` verschoben | Dateisystem | Dateisystem (unverändert) | Sandbox |

## Bewertung des Entwurfs

### Stärken

- **Ein Schreiber pro Tabelle.** Keine Wettläufe zwischen Pod und Cloud-Dienst um dieselben Zeilen, keine verteilten Transaktionen.
- **Status unabhängig vom Pod.** Jeder Pod kann `GET …/tasks/{taskId}` beantworten, auch nach Ende oder Absturz der ausführenden Sandbox.
- **Wiederaufnahme ohne eigenen Speicher.** Speichert der Dienst den Unterdatensatz vor seiner Antwort und nimmt Übermittlungen idempotent je `(batchauftrag_id, dateiname)` an, schließt sich das Absturzfenster „übermittelt, aber TaskId nicht gespeichert“. Eine neue Sandbox gleicht einfach `processing/` mit den Unterdatensätzen ab.
- **Weniger Infrastruktur für die Pipeline.** Kein Redis, keine eigene Datenbankverbindung.
- **Einfachere Statusabfrage.** Eine Abfrage pro Durchlauf nach Auftrag statt Bulk-Chunks je TaskId. `poll-interval` je Eintrag und `status-bulk-size` können entfallen.

### Risiken und wie man sie löst

| # | Risiko | Lösung |
|---|---|---|
| 1 | **Auftragsstatus in der Datenbank weicht vom tatsächlichen Stand ab**, weil nur die Sandbox Abschluss, Abbruch, Timeout und Fehlerschwelle kennt | Abschluss-Endpunkt `POST /batchauftraege/{id}/abschluss {state, cancelReason}`, aufgerufen im `finally` des Consumers, mit Wiederholung. Alternativ eine eigene Tabelle `pipeline_lauf`. |
| 2 | **Doppelte Übermittlung nach Absturz** | Unterdatensatz transaktional vor der Antwort speichern; Idempotenz je `(batchauftrag_id, dateiname)`. Muss mit dem Dienst geklärt werden. |
| 3 | **Zwei Pods bearbeiten denselben Auftrag** | Owner-Token bei jedem Statusaufruf, Übernahme nur per Compare-and-Set (`POST /batchauftraege/{id}/uebernahme`). |
| 4 | **ID und 409 fehlen bei `202`**, wenn erst der Producer den Auftrag anlegt | Anlage synchron in `TaskManager.start` mit clientseitiger UUID; erst danach Sandbox starten. |
| 5 | **Kopplung an fremdes Schema** | Consumer liest über Cloud-API 2, nicht per SQL. Auch Benutzer lesen über die REST-API, nicht mit eigenen Datenbank-Zugangsdaten. |
| 6 | **Verschiebe-Ergebnis nicht in der Datenbank**: Die Datenbank zeigt `SUCCESS`, auch wenn das Verschieben nach `donebox` scheiterte | Falls fachlich wichtig: Ergebnis in der Abschlussmeldung oder einem eigenen Rückmelde-Endpunkt mitgeben, ohne den Verarbeitungsstatus zu ändern. |
| 7 | **Zeitüberschreitung legt Dateien nach `errorbox`**, der Dienst führt sie noch als „in Bearbeitung“ (`StatusConsumer.failPendingAfterTimeout`) | Abschlussmeldung mit `TIMEOUT`; der Dienst schließt offene Unterdatensätze. |
| 8 | **Zentrale Abhängigkeit vom Cloud-Dienst**, auch für die Statusanzeige | `POST …/tasks` antwortet klar mit `503`, wenn der Dienst nicht erreichbar ist. Die Lease-Dauer muss länger sein als übliche Ausfälle, damit nicht unnötig übernommen wird. |

### Gesamturteil

| Kriterium | Bewertung |
|---|---|
| Passt zum Sandbox-Modell | **Ja.** Producer und Consumer bleiben; vor allem `CloudClient` ändert sich. |
| Mehrere Pods | **Ja**, Auftrag, Dateistatus, Sperre, Abbruch und Lease liegen zentral. |
| Robustheit bei Neustarts | **Sehr gut**, wenn der Dienst Übermittlungen idempotent annimmt (Risiko 2). |
| Konsistenz der Statusanzeige | **Nur mit Abschlussmeldung** (Risiko 1). |
| Aufwand in der Pipeline | **Gering**, weniger lokaler Zustand als heute. |
| Aufwand beim Cloud-Dienst | **Mittel**: idempotente Anlage und Übermittlung, Abfrage nach Auftrag, Abschluss und Übernahme. |

## Konkrete Schritte

### Phase A: jetzt, ohne Datenbank und ohne Änderung am Cloud-Dienst

Diese Schritte verbessern schon den Einzelserverbetrieb und passen unverändert zum Entwurf.

1. **Dateien beim Übermitteln beanspruchen.**
   Ordner `processing/` einführen (`UserFolders.processing()`, `moveToProcessing(...)`, angelegt in `NioUserFolderResolver.resolve`). Der Producer verschiebt jede Datei vor dem Aufruf von Cloud-API 1 dorthin. `InboxProducer.seen` entfällt.

2. **Datei-Schritte wiederholbar machen.**
   Reihenfolge `inbox → processing/ → donebox/errorbox`. Liegt eine Datei schon im Zielordner, gilt der Schritt als erledigt (`UserFolders.moveInto`). Beim Abbruch nicht entschiedene Dateien bewusst aus `processing/` zurück in die `inbox` legen (heute `abandonRemaining()`).

3. **Mit Dateinamen statt absoluten Pfaden arbeiten.**
   `PollEntry` speichert den Dateinamen relativ zum Benutzerordner. Das passt zum `dateiname` der Unterdatensätze, über den nach einem Neustart abgeglichen wird.

4. **Schnittstellen schneiden, mit In-Memory-Implementierung.**
   - `TaskRegistry` aufteilen in `SandboxRegistry` (lokal, Threads) und `BatchauftragService` (Anlage mit 409, Status lesen, Abschluss melden, Abbruchwunsch).
   - `CloudClient` um die künftigen Operationen erweitern (siehe Schritt 9) und die Fake-Cloud (`fakecloud/FakeCloudBackendApplication.java`, Test-`FakeCloud`) mitziehen.

5. **Anlage synchron in `TaskManager.start`.**
   Erst Auftrag anlegen (heute: im `BatchauftragService` im Speicher, später beim Dienst), dann `launch`. `latestByUser` entfällt, die 409-Regel steckt in der Anlage.

6. **Sauber herunterfahren.**
   `TaskManager.shutdown()` setzt das Abbruchsignal und wartet danach mit Zeitlimit per `Thread.join(...)` auf alle Threads, damit die Abschlussmeldung noch rausgeht. Dazu `server.shutdown: graceful` und `spring.lifecycle.timeout-per-shutdown-phase` größer als `status-timeout` + 5 s (`RESULT_GRACE`).

7. **Logs mit Auftragskontext.**
   `userId` und `taskId` per MDC in `InboxProducer.run` und `StatusConsumer.run`, strukturierte Logs auf stdout (`logging.structured.format.console: ecs`).

8. **Tests für Absturz und Wiederaufnahme.**
   Analog zu `TaskPipelineIntegrationTest`: Auftrag starten, Kontext mitten im Lauf schließen, neu starten. Erwartung: keine Datei wird zweimal an die Fake-Cloud übermittelt, offene Dateien werden weiter verfolgt.

### Phase B: Einführung des Cloud-Dienstes mit Datenbank

9. **`CloudClient` auf die neuen Endpunkte umstellen:**
   - `createBatchauftrag(taskId, userId)` → `201` / `200` (idempotent) / `409`
   - `submit(batchauftragId, items)`, idempotent je Dateiname
   - `queryStatus(batchauftragId, geaendertSeit, owner)` → Dateistatus + `abbruchAngefordert`, verlängert die Lease
   - `finish(batchauftragId, state, cancelReason)`
   - `takeOver(batchauftragId, owner, previousOwner)`

10. **Consumer umbauen.**
    Eine Abfrage pro Durchlauf nach Auftrag statt Bulk-Chunks; `abbruchAngefordert` → `context.cancel(USER_REQUEST)`; im `finally` `finish(...)` mit Wiederholung. Dateistatus werden nur gelesen.

11. **Status und Abbruch über den Dienst.**
    `TaskManager.status` liest den Auftrag über den Dienst, die Zähler berechnet der Dienst aus den Unterdatensätzen. `TaskManager.cancel` setzt dort den Abbruchwunsch; die Sandbox reagiert im nächsten Durchlauf. Abbrechen heißt dann **markieren, nicht löschen**. Das weicht von der bisherigen Vorgabe „cancel löscht die taskId aus der Registry“ ab.

12. **Wiederaufnahme verwaister Aufträge.**
    Ein zeitgesteuerter Job auf jedem Pod sucht Aufträge mit abgelaufener Lease und übernimmt sie per `takeOver` (Compare-and-Set). Die neue Sandbox gleicht `processing/` mit den Unterdatensätzen ab: vorhanden → weiter abfragen; nicht vorhanden → erneut übermitteln (idempotent).

13. **Vertragstests gegen den Dienst** (z. B. mit WireMock oder einer Testinstanz): Idempotenz, 409, Abschluss, Übernahme.

### Phase C: Betrieb und Deployment

14. **Gemeinsamer Speicher für die Benutzerordner:** Volume mit `ReadWriteMany` (z. B. NFS, Azure Files, EFS), fester Mount-Pfad, `PIPELINE_BASE_DIRECTORY=/data/users`. `inbox`, `processing`, `donebox` und `errorbox` müssen auf demselben Volume liegen, sonst ist `Files.move` nicht atomar.

15. **Health-Checks:** `spring-boot-starter-actuator`, Liveness- und Readiness-Probes (`management.endpoint.health.probes.enabled: true`). Readiness berücksichtigt die Erreichbarkeit des Cloud-Dienstes und geht beim Herunterfahren auf `DOWN`.

16. **Container-Image:** `mvnw spring-boot:build-image` oder Dockerfile mit geschichtetem JAR, als Nicht-Root-Benutzer. JVM-Speicher über `-XX:MaxRAMPercentage`; `InboxProducer` liest Dateien mit `Files.readString` komplett ein, das Limit muss zu Dateigröße × `batch-size` × Anzahl Aufträge passen.

17. **Kubernetes-Einstellungen:** `terminationGracePeriodSeconds` größer als der Shutdown-Timeout aus Schritt 6, ein `PodDisruptionBudget`, Pod-Identität per Downward API (`HOSTNAME`) als Owner-Token, Zugangsdaten zum Cloud-Dienst als `Secret`, übrige Konfiguration als `ConfigMap`.

## Priorität

| Priorität | Schritte | Begründung |
|---|---|---|
| **Hoch, sofort** | 1, 2, 6 | Verhindern Doppelübermittlung und verlorene Dateien bei Neustarts, auch heute schon. |
| **Mittel, bald** | 3, 4, 5, 7, 8 | Machen die Umstellung auf den Cloud-Dienst zu einem Austausch von Implementierungen. |
| **Mit dem Cloud-Dienst** | 9–13 | Brauchen die neuen Endpunkte. |
| **Mit der Containerisierung** | 14–17 | Brauchen gemeinsames Volume und Kubernetes. |
| **Vorab klären** | siehe unten | Bestimmen Wiederaufnahme und Semantik von `cancel`. |

## Offene Fragen an den Cloud-Dienst

1. Wird der Unterdatensatz transaktional **vor** der Antwort gespeichert?
2. Sind Anlage (`id`) und Übermittlung (`batchauftrag_id`, `dateiname`) **idempotent**?
3. Gibt es Endpunkte für **Abschluss/Abbruch** und **Übernahme**, oder führt die Pipeline eine eigene Tabelle `pipeline_lauf`?
4. Werden beim Abbruch laufende Verarbeitungen storniert oder nur nicht mehr abgeholt?
5. Soll das Ergebnis des Verschiebens (`donebox`/`errorbox`) in der Datenbank sichtbar sein?
6. Bisher löscht `cancel` die taskId aus der Registry. Soll der `batchauftrag` künftig stattdessen auf `CANCELLED` gesetzt und behalten werden?

## Fazit

Die Sandbox je Auftrag bleibt die richtige Ausführungseinheit, auch im Container. Mit dem Entwurf hält der Cloud-Dienst, **was** der Stand ist, und die Sandbox sorgt dafür, **dass** gearbeitet wird. Die Sandbox liest Dateistatus nur und meldet lediglich den Auftragsstatus zurück. Tragfähig wird das mit vier Ergänzungen: Abschlussmeldung, Owner-Lease, synchrone und idempotente Anlage sowie Statuslesen über Cloud-API 2. Die Vorbereitung in Phase A lohnt sich schon jetzt.
