# Ablauf einer Aufgabe (erste Ebene)

Sequenzdiagramm: [`../funktionsweise_sequenz_old.mmd`](../funktionsweise_sequenz_old.mmd) – erste Verfeinerung des Überblicks in [`anwendungsarchitektur.mmd`](anwendungsarchitektur.mmd). Die Nummern der Abschnitte entsprechen den Phasen im Diagramm. Chinesische Fassung: [`../funktionsweise_sequenz_zh.md`](../funktionsweise_sequenz_zh.md).

## 1. Start

Die Benutzer:in startet über die REST-API eine Aufgabe. Die API ist mit Spring WebFlux umgesetzt und reicht den Aufruf auf einem Virtual Thread an den TaskManager weiter, damit der Event-Loop nicht blockiert.

Der TaskManager baut für die Aufgabe eine eigene Sandbox auf (Kontext, Status-Pool, Producer- und Consumer-Thread), trägt sie in die TaskRegistry ein und startet Producer und Consumer. Die API antwortet sofort mit `202 Accepted`, dem Aufgabenstatus `RUNNING` und der Adresse für spätere Statusabfragen. Je Benutzer:in läuft höchstens eine Aufgabe gleichzeitig.

## 2. Producer – übermitteln

Zuerst nimmt der Producer Dateien wieder auf, die ein früherer Task bereits übermittelt hat. Sie liegen mit TaskId-Marker in der `pendingbox`; ihre TaskIds wandern direkt zurück in den Status-Pool, ohne erneute Übermittlung.

Danach arbeitet er die `inbox` batchweise ab:

1. Bis zu `batch-size` Dateien in die `pendingbox` verschieben.
2. Die JSON-Daten dieser Dateien in einer einzigen Anfrage an Cloud-API 1 übermitteln. Cloud-API 1 legt dazu einen batchgenAuftrag an und antwortet mit JSON-Daten.
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

## 4. Statusabfrage

Die Benutzer:in kann den Status jederzeit abfragen:

- **Laufende Aufgabe:** Der TaskManager liefert den aktuellen Stand aus der Sandbox in der TaskRegistry.
- **Beendete Aufgabe:** Der TaskManager liefert den Endzustand aus der TaskHistory; er bleibt für `task-retention` erhalten (Standard 8 Stunden).

## 5. Abbruch von außen

Bricht die Benutzer:in eine Aufgabe ab, setzt der TaskManager das Abbruchsignal, und Producer und Consumer beenden sich. Der TaskManager sendet eine Anfrage an Cloud-API 3, die den batchgenAuftrag als „CANCELLED“ markiert.

Danach legt der TaskManager zuerst den Endzustand in der TaskHistory ab und entfernt dann die Sandbox aus der TaskRegistry. So bleibt die Aufgabe jederzeit abfragbar, während Threads, Status-Pool und Kontext freigegeben werden.

Bereits übermittelte Dateien bleiben mit ihrem Marker in der `pendingbox`. Der nächste Task der Benutzer:in fragt ihren Status weiter ab, statt sie erneut zu übermitteln.

## 6. Abschluss und Abmeldung

Die Aufgabe endet auf einem von zwei Wegen:

- **`COMPLETED`:** Der Producer ist fertig und der Status-Pool leer.
- **`CANCELLED`:** Die maximale Laufzeit (`task-timeout`) ist überschritten – noch offene Dateien gehen dann in die `errorbox` – oder die Fehlerschwelle (`error-threshold`) ist erreicht.

Sind beide Threads beendet, meldet der zuletzt endende Thread die Aufgabe beim TaskManager ab. Der TaskManager sendet zuerst eine Anfrage an Cloud-API 3, die den batchgenAuftrag je nach Endzustand als „COMPLETED“ oder „CANCELLED“ markiert. Danach legt er den Endzustand in der TaskHistory ab und entfernt anschließend die Sandbox aus der TaskRegistry. So bleibt die Aufgabe jederzeit abfragbar, während Threads, Status-Pool und Kontext freigegeben werden.
