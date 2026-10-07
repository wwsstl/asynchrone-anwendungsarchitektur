# Ablauf einer Aufgabe

## Grundsätze

Die Anwendung veranlasst nur, dass die Cloud Daten erzeugt, und verarbeitet die lokalen Dateien anhand des Erzeugungsstatus, den die Cloud zurückliefert:

- Der Cloud-Dienst erzeugt die Daten in der Untertabelle des BatchgenAuftrags. Jede lokale Datei entspricht einem Datensatz dieser Untertabelle.
- Den Status der Datensätze aktualisiert die Cloud selbst; den Endzustand des BatchgenAuftrags schreibt die Anwendung über Cloud-API 3 (siehe Abschnitte 5 und 6).
- Kein Fortsetzen: Eine neue Aufgabe übernimmt keine Dateien, die eine frühere Aufgabe in der `pendingbox` hinterlassen hat (siehe Abschnitt 7).

| Begriff | Bedeutung |
|---|---|
| Aufgabennummer | Nummer des BatchgenAuftrags. Je Aufgabe wird genau ein BatchgenAuftrag erzeugt; alle Batches der Aufgabe gehören zu ihm. |
| TaskId | Nummer eines Datensatzes der Untertabelle. Sie gehört zu einer lokalen Datei und wird als Marker zusammen mit der Datei in der `pendingbox` gespeichert. |
| `inbox` | Dateien, die auf die Übermittlung warten |
| `pendingbox` | Dateien, die an Cloud-API 1 übergeben wurden und noch kein Ergebnis haben |
| `donebox` | erfolgreich erzeugte Dateien |
| `errorbox` | Dateien, die nicht umgewandelt werden konnten oder bei deren Erzeugung ein Fehler auftrat |

## 1. Start

Die Benutzer:in startet über die REST-API eine Aufgabe. Die REST-API ist mit Spring WebFlux umgesetzt und reicht den Aufruf auf einem Virtual Thread an den TaskManager weiter, damit der Event-Loop nicht blockiert. Je Benutzer:in läuft höchstens eine Aufgabe gleichzeitig.

Der TaskManager startet die Aufgabe in folgenden Schritten:

1. **`pendingbox` prüfen:** Ist sie nicht leer, liegen dort noch Dateien einer früheren Aufgabe, die nicht abgeschlossen wurden (siehe Abschnitt 7). Dann legt der TaskManager keinen BatchgenAuftrag an; der Start wird mit `409 Conflict` und dem Ursachencode `PENDINGBOX_NOT_EMPTY` abgewiesen, und die Benutzer:in wird aufgefordert, zuerst diese Dateien zu bearbeiten.
2. **BatchgenAuftrag anlegen:** Über Cloud-API 1 wird in der Cloud-Datenbank ein neuer BatchgenAuftrag mit dem Anfangsstatus RUNNING angelegt; seine Nummer ist die Aufgabennummer. In Phase 1 begrenzt die Cloud die Zahl der BatchgenAufträge je Benutzer:in nicht; ein neuer kann auch dann angelegt werden, wenn ein früherer noch auf RUNNING steht.
   Ist die Cloud nicht erreichbar (Cloud-API 1 nicht erreichbar oder Aufruf fehlgeschlagen), wird der Start mit `502 Bad Gateway` abgewiesen, und es wird keine Sandbox angelegt. Läuft der Aufruf in ein Timeout, obwohl die Cloud den BatchgenAuftrag tatsächlich angelegt hat, bleibt dieser auf RUNNING und muss manuell korrigiert werden (siehe Abschnitt 7).
3. **Sandbox starten:** Der TaskManager baut für die Aufgabe eine eigene Sandbox auf (Kontext, Status-Pool, Producer- und Consumer-Thread), trägt sie in die TaskRegistry ein und startet Producer und Consumer. Die REST-API antwortet sofort mit `202 Accepted`, dem Aufgabenstatus `RUNNING` und der Adresse für spätere Statusabfragen.

## 2. Producer – übermitteln

Der Producer arbeitet die `inbox` batchweise ab. Für jeden Batch:

1. Bis zu `batch-size` Dateien in die `pendingbox` verschieben. Cloud-API 1 ist nicht idempotent, und jede Datei wird höchstens einmal übermittelt; deshalb verlassen die Dateien die `inbox` schon vor dem Aufruf.
2. Die JSON-Daten dieser Dateien zusammen mit der Aufgabennummer in einer einzigen Anfrage an Cloud-API 1 übermitteln.
3. Die JSON-Antwort von Cloud-API 1 gibt an, welche Dateien erfolgreich in Datenerzeugungsaufträge umgewandelt wurden und welche nicht. Jede erfolgreich umgewandelte Datei erhält die Nummer des in der Untertabelle erzeugten Datensatzes, also ihre TaskId.
4. Erfolgreich umgewandelte Dateien: Die TaskId wird als Marker gesichert und in den Status-Pool eingetragen.
5. Nicht umgewandelte Dateien: Sie werden direkt in die `errorbox` verschoben, und der Fehlerzähler steigt um eins; sie zählen wie Dateien mit dem Status `ERROR` aus Abschnitt 3 zur Fehlerschwelle.
6. Warten, bis der Status-Pool auf `pool-resume-threshold` oder darunter gesunken ist (Rückstau), und erst dann den nächsten Batch lesen.

Während der Producer auf die Antwort von Cloud-API 1 wartet, ist er blockiert, aber nur kurz: Die Antwort gibt nur an, welche Dateien abgefragt werden können und welche direkt in die `errorbox` gehen. Der Producer läuft auf einem Virtual Thread; das Warten belegt keinen Plattform-Thread.

Schlägt der Aufruf von Cloud-API 1 fehl, haben die Dateien des Batches noch keinen TaskId-Marker. Sie werden nach Art des Fehlers behandelt:

| Fehlerart | Beispiele | Behandlung |
|---|---|---|
| Eindeutig nicht verarbeitet | Verbindungsfehler, Anfrage abgelehnt | Die Dateien wandern zurück in die `inbox`. Nach einer Wartezeit (z. B. ein `sweep-interval`, bei wiederholten Fehlern zunehmend länger) liest der Producer sie erneut und übermittelt sie noch einmal; ist Cloud-API 1 nur vorübergehend nicht erreichbar, erholt sich dieselbe Aufgabe also selbst. Bleibt Cloud-API 1 dauerhaft nicht verfügbar, endet die Aufgabe schließlich mit `TIMEOUT` (siehe Abschnitt 6); die Dateien wurden nie erfolgreich übermittelt, bleiben deshalb in der `inbox` und werden beim nächsten Start normal übermittelt. |
| Ergebnis unklar | Timeout, keine Antwort | Cloud-API 1 hat den Batch möglicherweise bereits verarbeitet. Damit nichts doppelt übermittelt wird, bleiben die Dateien in der `pendingbox` und werden von der Benutzer:in bearbeitet (siehe Abschnitt 7). |

## 3. Consumer – Status verfolgen

Parallel zum Producer holt der Consumer in jedem `sweep-interval` die fälligen TaskIds aus dem Status-Pool, fasst sie zu einer einzigen Anfrage zusammen und fragt ihren Status gebündelt bei Cloud-API 2 ab:

| Status | Folge |
|---|---|
| `SUCCESS` | Datei wandert von der `pendingbox` in die `donebox`; der Marker wird gelöscht. |
| `ERROR` | Datei wandert in die `errorbox`; der Marker wird gelöscht, der Fehlerzähler steigt um eins. |
| `PENDING` | Der Eintrag bleibt im Status-Pool und wird nach `poll-interval` erneut geprüft. |

## 4. Statusabfrage

Die Benutzer:in kann den Status jederzeit abfragen; eine TaskHistory ist nicht nötig:

- **Cloud (jederzeit abfragbar):** Status des BatchgenAuftrags und Status seiner Datensätze (wie viele `SUCCESS`, `ERROR` und `PENDING`). Zum Status des BatchgenAuftrags gehören auch die von der Anwendung geschriebenen Endzustände `COMPLETED`, `TIMEOUT`, `ERROR` und `TERMINATED` (siehe Abschnitte 5 und 6); auch nach dem Ende einer Aufgabe ist also abfragbar, wie sie geendet hat.
- **Lokal (nur während die Aufgabe läuft, also solange die Sandbox in der TaskRegistry steht):** der Stand im Speicher, einschließlich der Anzahl je Status. Er enthält nur Ergebnisse, die der Consumer bereits verarbeitet hat, und kann den Zahlen der Cloud etwas hinterherhinken. Nach dem Ende der Aufgabe ist nur noch die Abfrage in der Cloud möglich.

## 5. Abbruch von außen

Bricht die Benutzer:in eine Aufgabe ab:

1. Der TaskManager setzt das Abbruchsignal, und Producer und Consumer beenden sich. Bereits übermittelte Dateien bleiben mit ihrem Marker in der `pendingbox`.
2. Der TaskManager setzt den Status des BatchgenAuftrags über Cloud-API 3 auf `TERMINATED`.
3. Der TaskManager entfernt die Sandbox aus der TaskRegistry; Threads, Status-Pool und Kontext werden freigegeben.

Die Dateien, die in der `pendingbox` geblieben sind, bearbeitet die Benutzer:in vor dem nächsten Start (siehe Abschnitt 7).

## 6. Abschluss und Abmeldung

Die Aufgabe endet von selbst auf einem von drei Wegen. In jedem Fall schreibt der TaskManager den passenden Endzustand über Cloud-API 3 in den BatchgenAuftrag:

| Ende | Bedingung | Status des BatchgenAuftrags | `pendingbox` |
|---|---|---|---|
| Normales Ende | Der Producer ist fertig (alle Dateien der `inbox` sind übermittelt), und der Status-Pool ist leer | `COMPLETED` | leer; einzige Ausnahme sind Dateien mit unklarem Übermittlungsergebnis (Abschnitt 2) |
| Zeitüberschreitung | Die maximale Laufzeit (`task-timeout`) ist überschritten | `TIMEOUT` | Dateien ohne Ergebnis bleiben |
| Fehlerschwelle | Der Fehlerzähler erreicht `error-threshold` | `ERROR` | Dateien ohne Ergebnis bleiben |

Dateien ohne Ergebnis haben in der Cloud noch kein Ergebnis und sind nicht unbedingt fehlerhaft. Sie werden deshalb nicht in die `errorbox` verschoben, sondern bleiben mit ihrem TaskId-Marker in der `pendingbox` (siehe Abschnitt 7).

Sind beide Threads beendet, meldet der zuletzt endende Thread die Aufgabe beim TaskManager ab: Der TaskManager entfernt die Sandbox aus der TaskRegistry; Threads, Status-Pool und Kontext werden freigegeben.

## 7. Restdateien und manuelle Bearbeitung

In folgenden Fällen bleiben Dateien in der `pendingbox`:

| Ursache | Verbleibende Dateien |
|---|---|
| Zeitüberschreitung, Fehlerschwelle (Abschnitt 6) | übermittelte Dateien ohne Ergebnis, mit TaskId-Marker |
| Abbruch von außen (Abschnitt 5) | übermittelte Dateien ohne Ergebnis, mit TaskId-Marker |
| Absturz der Anwendung | übermittelte Dateien ohne Ergebnis, mit TaskId-Marker; der Batch, der beim Absturz gerade übermittelt wurde, ohne Marker |
| Unklares Ergebnis eines Aufrufs von Cloud-API 1 (Abschnitt 2) | die Dateien dieses Batches, ohne TaskId-Marker |

Diese Dateien bearbeitet die Benutzer:in vor dem nächsten Start selbst. Die Prüfung beim Start (Abschnitt 1) stellt das sicher: Ist die `pendingbox` nicht leer, wird der Start abgewiesen (`PENDINGBOX_NOT_EMPTY`).

In folgenden Fällen bleibt der BatchgenAuftrag auf RUNNING und muss manuell korrigiert werden:

- Die Anwendung stürzt ab und kann keinen Endzustand mehr schreiben.
- Das Schreiben des Endzustands über Cloud-API 3 schlägt fehl (z. B. weil die Cloud vorübergehend nicht erreichbar ist).
- Beim Start läuft der Aufruf von Cloud-API 1 in ein Timeout, obwohl die Cloud den BatchgenAuftrag tatsächlich angelegt hat (Abschnitt 1).
