# Ablauf einer Aufgabe (erste Ebene)

Sequenzdiagramm: [`../funktionsweise_sequenz_old.mmd`](../funktionsweise_sequenz_old.mmd) – erste Verfeinerung des Überblicks in [`anwendungsarchitektur.mmd`](anwendungsarchitektur.mmd). Die Nummern der Abschnitte entsprechen den Phasen im Diagramm. Chinesische Fassung: [`../funktionsweise_sequenz_zh.md`](../funktionsweise_sequenz_zh.md).

## 1. Start

Die Benutzer:in startet über die REST-API eine Aufgabe. Die API ist mit Spring WebFlux umgesetzt und reicht den Aufruf auf einem Virtual Thread an den TaskManager weiter, damit der Event-Loop nicht blockiert.

Der Start läuft in vier Schritten. Prüfen und Belegen geschehen jeweils in einem einzigen atomaren Schritt im Laufregister (Abschnitt 7). So können sich zwei gleichzeitige Starts nicht gegenseitig übersehen, auch wenn sie auf verschiedenen Instanzen eintreffen.

1. **Benutzer:in belegen:** Der TaskManager legt im Laufregister einen Lauf mit dem Status `STARTING` an. Hat die Benutzer:in bereits einen Lauf in `STARTING` oder `RUNNING`, schlägt das fehl, und der Start wird mit `409 Conflict` und dem Ursachencode `USER_TASK_RUNNING` abgewiesen.
2. **BatchgenAuftrag bestimmen:** Der TaskManager prüft über Cloud-API 3, ob für die Benutzer:in bereits ein BatchgenAuftrag im Status „RUNNING“ existiert (Fortsetzen nach einem Abbruch, siehe Abschnitt 5):

   | Ergebnis | Reaktion |
   |---|---|
   | BatchgenAuftrag vorhanden | Der TaskManager verwendet dessen Nummer wieder (Fortsetzen). |
   | kein BatchgenAuftrag, aber Dateien in der `pendingbox` | Die Dateien gehören zu einem abgebrochenen BatchgenAuftrag. Der Lauf aus Schritt 1 wird wieder entfernt, und der Start wird mit `409 Conflict` und dem Ursachencode `PENDINGBOX_NOT_EMPTY` abgewiesen. Die Benutzer:in wird aufgefordert, zuerst fortzusetzen (a) oder neu zu beginnen (b), siehe Abschnitt 5. |
   | kein BatchgenAuftrag, `pendingbox` leer | Der TaskManager lässt über Cloud-API 1 einen neuen BatchgenAuftrag anlegen. |

3. **Aufgabennummer belegen:** Die Nummer des BatchgenAuftrags ist zugleich die Aufgabennummer. Der TaskManager trägt sie mit der nächsten Laufnummer in den Lauf ein und setzt ihn auf `RUNNING`, zusammen mit der Instanz, die ihn ausführt. Läuft unter dieser Aufgabennummer bereits ein Lauf, schlägt das fehl: Der Lauf aus Schritt 1 wird entfernt, und der Start wird mit `409 Conflict` und dem Ursachencode `TASK_ALREADY_RUNNING` abgewiesen.
4. **Sandbox starten:** Unter der Aufgabennummer baut der TaskManager eine eigene Sandbox auf (Kontext, Status-Pool, Producer- und Consumer-Thread), trägt sie in die lokale TaskRegistry seiner Instanz ein und startet Producer und Consumer. Die API antwortet sofort mit `202 Accepted`, dem Aufgabenstatus `RUNNING` und der Adresse für spätere Statusabfragen.

Die Benutzer:in wird belegt, **bevor** Cloud-API 1 aufgerufen wird. Sonst könnten zwei fast gleichzeitige Starts derselben Benutzer:in je einen neuen BatchgenAuftrag anlegen, von dem einer ungenutzt in der Cloud zurückbliebe.

Die Ursachencodes stehen in der Antwort der REST-API, damit die Oberfläche je Ursache einen passenden Hinweis anzeigen kann.

Jeder Start unter derselben Aufgabennummer ist ein neuer Lauf mit fortlaufender Laufnummer (1, 2, …). Gezählt werden nur Läufe, die noch im Laufregister stehen; ältere Läufe werden nach Ablauf von `task-retention` (8 Stunden) ignoriert.

## 2. Producer – übermitteln

Zuerst nimmt der Producer Dateien wieder auf, die ein früherer Task bereits übermittelt hat. Sie liegen mit TaskId-Marker in der `pendingbox`; ihre TaskIds wandern direkt zurück in den Status-Pool, ohne erneute Übermittlung.

Danach arbeitet er die `inbox` batchweise ab:

1. Bis zu `batch-size` Dateien in die `pendingbox` verschieben.
2. Die JSON-Daten dieser Dateien zusammen mit der BatchgenAuftrag-Nummer in einer einzigen Anfrage an Cloud-API 1 übermitteln. Der BatchgenAuftrag besteht seit dem Start (Abschnitt 1); alle Batches der Aufgabe gehören zu ihm. Cloud-API 1 antwortet mit JSON-Daten.
3. Die JSON-Antwort von Cloud-API 1 gibt an, welche Dateien erfolgreich in Datenerzeugungsaufträge umgewandelt wurden – jeweils mit TaskId – und welche nicht.
4. Für die erfolgreich umgewandelten Dateien wird die TaskId als Marker gesichert und in den Status-Pool eingetragen.
5. Die nicht umgewandelten Dateien werden direkt in die `errorbox` verschoben.

Weil Cloud-API 1 nicht idempotent ist, wird jede Datei höchstens einmal übermittelt. Nach jedem Batch wartet der Producer, bis der Status-Pool auf `pool-resume-threshold` oder darunter gesunken ist (Rückstau), und liest erst dann die nächste Charge.

## 3. Consumer – Status verfolgen

Parallel zum Producer holt der Consumer in jedem `sweep-interval` die fälligen TaskIds aus dem Status-Pool, fasst sie zu einer einzigen Anfrage zusammen und fragt ihren Status gebündelt bei Cloud-API 2 ab:

| Status | Folge |
|---|---|
| `SUCCESS` | Datei wandert von der `pendingbox` in die `donebox`; der Marker wird gelöscht. |
| `ERROR` | Datei wandert in die `errorbox`; der Marker wird gelöscht, der Fehlerzähler steigt um eins. |
| `PENDING` | Der Eintrag bleibt im Status-Pool und wird nach `poll-interval` erneut geprüft. |

Zusätzlich prüft der Consumer in jedem Durchlauf über Cloud-API 3 den Status des BatchgenAuftrags. Steht er auf „CANCELLED“, beenden sich Producer und Consumer (siehe Abschnitt 5).

Außerdem schreibt der Consumer in jedem Durchlauf den aktuellen Fortschritt (übermittelte, erfolgreiche, fehlerhafte und offene Dateien) als Momentaufnahme in seinen Lauf im Laufregister und verlängert dessen Lease (Abschnitt 7).

## 4. Statusabfrage

Die Benutzer:in kann den Status jederzeit abfragen. Die Antwort stammt immer aus dem Laufregister, nie aus dem Speicher einer einzelnen Instanz; jede Instanz kann die Abfrage also beantworten:

- **Laufende Aufgabe:** die Momentaufnahme des Laufs, höchstens ein `sweep-interval` alt.
- **Aufgabe wird abgebrochen:** Steht der Lauf noch auf `RUNNING`, meldet Cloud-API 3 für den BatchgenAuftrag aber bereits „CANCELLED“, liefert die Abfrage `CANCELLING` („wird abgebrochen“). Dieser Status wird bei der Abfrage abgeleitet und nirgends gespeichert.
- **Beendete Aufgabe:** der Endzustand des letzten Laufs; er bleibt für `task-retention` erhalten (Standard 8 Stunden).

Das Laufregister legt jeden Lauf unter Aufgabennummer und Laufnummer ab. Wird eine abgebrochene Aufgabe fortgesetzt, bleibt der Endzustand des abgebrochenen Laufs daher erhalten und wird nicht überschrieben.

## 5. Abbruch von außen

Bricht die Benutzer:in eine Aufgabe ab, leitet die REST-API den Abbruch direkt an Cloud-API 3 weiter: Da die Aufgabennummer zugleich die BatchgenAuftrag-Nummer ist, markiert sie den BatchgenAuftrag als „CANCELLED“ und antwortet mit `CANCELLING`. Dabei liest und schreibt sie keinen Zustand der Instanz. Jede Instanz kann den Abbruch daher entgegennehmen, unabhängig davon, auf welcher Instanz die Aufgabe läuft.

Der Consumer prüft den Status des BatchgenAuftrags in jedem Durchlauf über die Cloud-API 3. Ist er als „CANCELLED“ markiert, werden sowohl der Producer als auch der Consumer beendet – spätestens nach einem `sweep-interval`. Bis dahin liefert die Statusabfrage `CANCELLING` (Abschnitt 4).

Danach legt der TaskManager der ausführenden Instanz den Endzustand `CANCELLED` im Laufregister ab und entfernt die Sandbox aus seiner TaskRegistry. So bleibt die Aufgabe jederzeit abfragbar, während Threads, Status-Pool und Kontext freigegeben werden.

Bereits übermittelte Dateien bleiben mit ihrem Marker in der `pendingbox`. Danach hat die Benutzer:in zwei Wege:

- **a) Fortsetzen:** Sie setzt den abgebrochenen BatchgenAuftrag manuell wieder auf „RUNNING“ und startet erneut eine Aufgabe. Der TaskManager findet beim Start den laufenden BatchgenAuftrag und verwendet dessen Nummer wieder (Abschnitt 1). Der Producer nimmt die markierten Dateien der `pendingbox` wieder auf, ohne sie erneut zu übermitteln (Abschnitt 2).
- **b) Neu beginnen:** Sie leert die `pendingbox` manuell und startet eine neue Aufgabe; dafür wird ein neuer BatchgenAuftrag angelegt.

## 6. Abschluss und Abmeldung

Die Aufgabe endet auf einem von zwei Wegen:

- **`COMPLETED`:** Der Producer ist fertig und der Status-Pool leer.
- **`CANCELLED`:** Die maximale Laufzeit (`task-timeout`) ist überschritten – noch offene Dateien gehen dann in die `errorbox` – oder die Fehlerschwelle (`error-threshold`) ist erreicht.

**Timeout oder Fehlerschwelle** lösen denselben Mechanismus aus wie der Abbruch von außen (Abschnitt 5):

1. Die Sandbox meldet das Ereignis dem TaskManager ihrer Instanz.
2. Der TaskManager markiert den BatchgenAuftrag über Cloud-API 3 als „CANCELLED“. Das geschieht auf der Instanz, auf der die Sandbox läuft; ein gemeinsamer Zustand ist dafür nicht nötig.
3. Im nächsten Durchlauf erkennt der Consumer den Status, und Producer und Consumer beenden sich.
4. Der TaskManager legt den Endzustand `CANCELLED` im Laufregister ab und entfernt die Sandbox aus seiner TaskRegistry.

Danach gelten dieselben zwei Wege wie in Abschnitt 5: fortsetzen oder neu beginnen.

**Normales Ende:** Ist der Producer fertig und der Status-Pool leer, meldet die Sandbox dies dem TaskManager. Der TaskManager markiert den BatchgenAuftrag über Cloud-API 3 als „COMPLETED“. Danach beendet er die Sandbox und gibt ihre Ressourcen frei: Er legt den Endzustand `COMPLETED` im Laufregister ab und entfernt die Sandbox aus seiner TaskRegistry.

In allen Fällen bleibt die Aufgabe jederzeit abfragbar, während Threads, Status-Pool und Kontext freigegeben werden.

## 7. Gemeinsamer Zustand und Betrieb mit mehreren Instanzen

Die Anwendung soll später auf mehreren Instanzen (Container/Pods) laufen können. Eine Anfrage kann dann auf einer anderen Instanz eintreffen als der, auf der die Aufgabe läuft. Deshalb hängt nichts, was eine Instanz einer anderen mitteilen muss, vom Speicher einer einzelnen Instanz ab. Der Zustand ist so aufgeteilt:

| Zustand | Ort | Grund |
|---|---|---|
| laufende Sandbox (Threads, Status-Pool, Kontext) | lokale TaskRegistry der ausführenden Instanz | lässt sich nicht teilen und muss es auch nicht |
| Läufe: Benutzer:in, Aufgabennummer, Laufnummer, Status, ausführende Instanz, Lease, Fortschritt, Endzustand | Laufregister (gemeinsam) | Startprüfungen, Statusabfrage und Historie müssen auf jeder Instanz gleich ausfallen |
| Abbruchwunsch | Status des BatchgenAuftrags in Cloud-API 3 | liegt ohnehin außerhalb aller Instanzen |
| Dateien | Benutzerordner auf einem gemeinsamen Volume | Startprüfung und Fortsetzen lesen die `pendingbox` |

Ein Lauf hat die Status `STARTING`, `RUNNING`, `COMPLETED` und `CANCELLED`. Das Laufregister bietet nur atomare Operationen an:

- Benutzer:in belegen;
- Aufgabennummer belegen;
- Lauf freigeben;
- Lease verlängern;
- Momentaufnahme schreiben und lesen;
- Endzustand ablegen.

Prüfen und Belegen sind dadurch nie getrennte Schritte.

- **Phase 1 (ein Server):** Das Laufregister ist eine In-Memory-Implementierung ohne Datenbank; die bisherige TaskHistory geht darin auf (siehe „Phase 1 ohne Datenbank“).
- **Phase 2 (mehrere Instanzen):** Das Laufregister wird eine Datenbanktabelle mit zwei partiellen Unique-Indizes:
  - auf der Benutzer:in für Läufe in `STARTING` oder `RUNNING` – eine Aufgabe je Benutzer:in;
  - auf der Aufgabennummer für Läufe in `RUNNING` – kein BatchgenAuftrag läuft doppelt.

  `task-retention` wird zu einer Löschregel. REST-API und TaskManager ändern sich dabei nicht; ausgetauscht wird nur die Implementierung hinter den Operationen.
- **Ausfall einer Instanz:** Die ausführende Instanz verlängert in jedem Durchlauf die Lease ihres Laufs. Fällt sie aus, läuft die Lease ab, und der Lauf zählt nicht mehr als laufend; die Benutzer:in kann neu starten. Der BatchgenAuftrag steht in der Cloud noch auf „RUNNING“, und die Dateien liegen mit Marker in der `pendingbox`. Der neue Start setzt die Aufgabe deshalb fort (Abschnitt 1).

### Phase 1 ohne Datenbank

In Phase 1 läuft genau ein Prozess auf einem Server. Für das Laufregister genügt deshalb eine In-Memory-Implementierung mit den Mitteln der JVM:

| Operation | Umsetzung |
|---|---|
| Benutzer:in belegen | `putIfAbsent` auf einer `ConcurrentHashMap` je Benutzer:in |
| Aufgabennummer belegen | `putIfAbsent` auf einer `ConcurrentHashMap` je Aufgabennummer |
| Lauf freigeben | `remove(key, value)` in beiden Maps |
| Momentaufnahme | direkt aus dem Kontext der Sandbox gelesen |
| Endzustände | `ConcurrentHashMap` mit dem Schlüssel Aufgabennummer und Laufnummer; Einträge werden nach `task-retention` gelöscht |
| Lease | entfällt: Es gibt nur einen Prozess. Endet er, verschwindet der gesamte Zustand im Speicher, als wären alle Leases abgelaufen. |

**Neustart der Anwendung:** Der Speicher ist danach leer.

- **Laufende Aufgaben** enden mit dem Prozess. Der BatchgenAuftrag steht in der Cloud weiter auf „RUNNING“, und die Dateien liegen mit Marker in der `pendingbox`. Startet die Benutzer:in erneut, findet der Start den BatchgenAuftrag und setzt die Aufgabe fort (Abschnitt 1). Das ist derselbe Weg wie nach dem Ausfall einer Instanz in Phase 2.
- **Endzustände beendeter Aufgaben** gehen verloren; die Statusabfrage liefert dann `404`, und die Laufnummern beginnen wieder bei 1. Das entspricht der Regel, dass Läufe nach `task-retention` ignoriert werden.

**Grenzen:**

- **Nur ein Prozess:** Auf denselben Benutzerordnern darf nur ein Prozess laufen. Ein versehentlich gestarteter zweiter Prozess sähe die Belegungen im Speicher nicht. Absichern lässt sich das mit einer Dateisperre (`FileChannel.tryLock()`) oder als Betriebsregel.
- **Statusabfrage:** Jede Statusabfrage einer laufenden Aufgabe fragt Cloud-API 3 ab, um `CANCELLING` abzuleiten. Bei häufigen Abfragen kann das Ergebnis kurz zwischengespeichert werden, höchstens für ein `sweep-interval`.
