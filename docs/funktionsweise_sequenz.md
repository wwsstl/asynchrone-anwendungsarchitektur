# Ablauf einer Aufgabe (erste Ebene)

Sequenzdiagramm: [`funktionsweise_sequenz.mmd`](funktionsweise_sequenz.mmd) – erste Ebene zum Überblick in [`funktionsweise.mmd`](funktionsweise.mmd). Die Nummern der Abschnitte entsprechen den Phasen im Diagramm.

## 1. Start

Die Benutzer:in startet über die REST-API eine Aufgabe. Die API ist mit Spring WebFlux umgesetzt und reicht den Aufruf auf einem Virtual Thread an den TaskManager weiter, damit der Event-Loop nicht blockiert.

Der TaskManager baut für die Aufgabe eine eigene Sandbox auf (Kontext, Status-Pool, Producer- und Consumer-Thread), trägt sie in die TaskRegistry ein und startet Producer und Consumer. Die API antwortet sofort mit `202 Accepted`, dem Aufgabenstatus `RUNNING` und der Adresse für spätere Statusabfragen. Je Benutzer:in läuft höchstens eine Aufgabe gleichzeitig.

## 2. Producer – übermitteln

Zuerst nimmt der Producer Dateien wieder auf, die ein früherer Task bereits übermittelt hat. Sie liegen mit TaskId-Marker in der `pendingbox`; ihre TaskIds wandern direkt in den Status-Pool, ohne erneute Übermittlung.

Danach arbeitet er die `inbox` batchweise ab:

1. Bis zu `batch-size` Dateien in die `pendingbox` verschieben.
2. Die Dateien in einem Aufruf an Cloud-API 1 übermitteln.
3. Die zurückgelieferte TaskId je Datei als Marker sichern und in den Status-Pool eintragen.

Weil Cloud-API 1 nicht idempotent ist, wird jede Datei höchstens einmal übermittelt. Nach jedem Batch wartet der Producer, bis der Pool auf `pool-resume-threshold` abgebaut ist (Rückstau), und liest erst dann die nächste Charge.

## 3. Consumer – Status verfolgen

Parallel zum Producer holt der Consumer in jedem `sweep-interval` die fälligen TaskIds aus dem Status-Pool und fragt ihren Status gebündelt bei Cloud-API 2 ab:

| Status | Folge |
|---|---|
| `SUCCESS` | Datei wandert von der `pendingbox` in die `donebox`; der Marker wird gelöscht. |
| `ERROR` | Datei wandert in die `errorbox`; der Marker wird gelöscht, der Fehlerzähler steigt. |
| `PENDING` | Der Eintrag bleibt im Pool und wird nach `poll-interval` erneut geprüft. |

## 4. Statusabfrage

Die Benutzer:in kann den Status jederzeit abfragen:

- **Laufende Aufgabe:** Der TaskManager liefert den aktuellen Stand aus der Sandbox in der TaskRegistry.
- **Beendete Aufgabe:** Der TaskManager liefert den Endzustand aus der TaskHistory, bis `task-retention` (Standard 24 Stunden) abgelaufen ist.

## 5. Abbruch von außen (optional)

Bricht die Benutzer:in ab, setzt der TaskManager das Abbruchsignal: Producer und Consumer beenden sich. Die Aufgabe wird sofort aus der TaskRegistry gelöscht; sie ist danach nicht mehr abfragbar und wird auch nicht in die Historie übernommen.

Bereits übermittelte Dateien bleiben mit ihrem Marker in der `pendingbox`. Der nächste Task der Benutzer:in fragt ihren Status weiter ab, statt sie erneut zu übermitteln.

## 6. Abschluss und Abmeldung

Die Aufgabe endet auf einem von zwei Wegen:

- **`COMPLETED`:** Der Producer ist fertig und der Status-Pool leer.
- **`CANCELLED`:** Die maximale Laufzeit (`task-timeout`) ist überschritten – offene Dateien gehen dann in die `errorbox` – oder die Fehlerschwelle (`error-threshold`) ist erreicht.

Sind beide Threads beendet, meldet der zuletzt endende Thread die Aufgabe beim TaskManager ab. Dieser legt zuerst den Endzustand in der TaskHistory ab und entfernt dann die Sandbox aus der TaskRegistry. So bleibt die Aufgabe lückenlos abfragbar, während Threads, Status-Pool und Kontext freigegeben werden.
