# Cloud-API 2: Bulk-Statusabfrage

Mit dieser Anfrage fragt der Consumer den Erzeugungsstatus mehrerer Datensätze in einem Aufruf ab
(funktionsweise_sequenz.md, Abschnitt 3). Den Status der Datensätze aktualisiert die Cloud selbst; die Anfrage liest
nur und verändert nichts. Umgesetzt in `WebClientCloudClient.queryStatus`, aufgerufen von
`StatusConsumer.queryAndRoute`.

## Wann die Anfrage gestellt wird

- Der Consumer holt in jedem `pipeline.sweep-interval` (Standard 1 s) die **fälligen** TaskIds aus dem Status-Pool
  seiner Sandbox. Fällig ist eine TaskId sofort nach dem Übermitteln und danach jeweils `pipeline.poll-interval`
  (Standard 5 s) nach ihrer letzten Abfrage.
- Alle fälligen TaskIds gehen in **eine** Anfrage, höchstens `pipeline.status-bulk-size` (Standard 200) je Anfrage; sind
  es mehr, folgen weitere Anfragen.
- Alle TaskIds einer Anfrage gehören zur selben Aufgabe (demselben BatchgenAuftrag). Die Anfrage enthält die
  Aufgabennummer trotzdem nicht: Eine TaskId ist allein eindeutig.
- Sind keine TaskIds fällig, wird keine Anfrage gestellt.

## Anfrage

```
POST {pipeline.cloud.base-url}{pipeline.cloud.status-path}
Content-Type: application/json
```

Standard: `POST http://localhost:8081/tasks/status`. Der Rumpf ist UTF-8-kodiertes JSON.

### Felder

| Feld | Typ | Pflicht | Bedeutung |
|---|---|---|---|
| `taskIds` | Array von Strings | ja | die abzufragenden TaskIds, wie Cloud-API 1 sie beim Übermitteln geliefert hat (`tasks[].taskId`, siehe [`cloud_api1_submit.md`](cloud_api1_submit.md)); mindestens 1, höchstens `status-bulk-size` Einträge, keine doppelten |

### Beispiel

Die TaskIds aus der [Beispielantwort von Cloud-API 1](cloud_api1_submit.md#antwort); die Datei
[`cloud_api2_status_request.json`](cloud_api2_status_request.json) enthält sie ohne Kommentar:

```json
{
  "taskIds": [
    "3f2b9c1e-6a4d-4f0e-9b7a-1c2d3e4f5a6b",
    "8d7c6b5a-4e3f-4a2b-9c1d-0e9f8a7b6c5d"
  ]
}
```

## Antwort

```
200 OK
Content-Type: application/json
```

Der Rumpf enthält je abgefragter TaskId ihren aktuellen Status.

### Felder

| Feld | Typ | Pflicht | Bedeutung |
|---|---|---|---|
| `results` | Array | nein | ein Eintrag je bekannter TaskId; fehlt das Feld, wird jede abgefragte TaskId wie `PENDING` behandelt |
| `results[].taskId` | String | ja | TaskId aus der Anfrage |
| `results[].status` | String | ja | genau einer der Werte `SUCCESS`, `ERROR`, `PENDING` (siehe unten) |

| `status` | Bedeutung |
|---|---|
| `SUCCESS` | Die Cloud hat die Testdaten erfolgreich erzeugt |
| `ERROR` | Bei der Erzeugung ist ein Fehler aufgetreten |
| `PENDING` | Die Erzeugung läuft noch |

### Beispiel

Antwort auf das [Beispiel der Anfrage](#beispiel); die Datei
[`cloud_api2_status_response.json`](cloud_api2_status_response.json) enthält sie ohne Kommentar:

```json
{
  "results": [
    {
      "taskId": "3f2b9c1e-6a4d-4f0e-9b7a-1c2d3e4f5a6b",
      "status": "SUCCESS"
    },
    {
      "taskId": "8d7c6b5a-4e3f-4a2b-9c1d-0e9f8a7b6c5d",
      "status": "PENDING"
    }
  ]
}
```

So verarbeitet die Anwendung diese Antwort:

| TaskId | Datei | `status` | Folge |
|---|---|---|---|
| `3f2b9c1e-…` | `test_1.json` | `SUCCESS` | Datei von der `pendingbox` in die `donebox`, Marker löschen, aus dem Status-Pool entfernen |
| `8d7c6b5a-…` | `test_2.json` | `PENDING` | bleibt im Status-Pool und wird nach `poll-interval` erneut abgefragt |

Eine spätere Antwort für `test_2.json` könnte so aussehen:

```json
{ "results": [ { "taskId": "8d7c6b5a-4e3f-4a2b-9c1d-0e9f8a7b6c5d", "status": "ERROR" } ] }
```

Dann kommt die Datei in die `errorbox`, ihr Marker wird gelöscht, und der Fehlerzähler der Aufgabe steigt um eins
(Ende mit `ERROR`, sobald er `pipeline.error-threshold` erreicht).

### Regeln für die Auswertung

- Die Anwendung ordnet die Einträge über `taskId` zu; die Reihenfolge spielt keine Rolle.
- Eine abgefragte TaskId ohne Eintrag in `results` wird wie `PENDING` behandelt: Die Cloud hat kein Ergebnis
  gemeldet, das ist weder Erfolg noch Fehler.
- Einträge ohne `taskId` oder ohne `status` werden ignoriert; die betroffene TaskId gilt damit als `PENDING`.
- Einträge zu TaskIds, die nicht abgefragt wurden, werden ignoriert.
- `status` darf nur `SUCCESS`, `ERROR` oder `PENDING` sein. Ein anderer Wert (z. B. `RUNNING`) macht die **ganze**
  Antwort unlesbar; sie wird dann wie eine fehlgeschlagene Abfrage behandelt (siehe unten).
- Eine leere Antwort `{"results":[]}` bedeutet: noch kein Ergebnis, alle TaskIds werden nach `poll-interval` erneut
  abgefragt.

## Fehler und ihre Behandlung

Die Anfrage liest nur und ist **idempotent**; eine Wiederholung ist daher unbedenklich. Jeder Versuch hat höchstens
`pipeline.cloud.status-timeout`.

| Antwort / Fehler | automatisch wiederholt? | Folge in der Anwendung |
|---|---|---|
| `5xx` | ja, bis zu `pipeline.cloud.retries` Mal (Wartezeit ab 200 ms, wachsend) | erst wenn alle Versuche scheitern: wie unten |
| keine Antwort innerhalb von `status-timeout` | ja, wie `5xx` | wie unten |
| Netzwerkfehler (Verbindung abgelehnt, abgebrochen) | ja, wie `5xx` | wie unten |
| `4xx` | nein | wie unten |
| Antwort nicht lesbar (z. B. unbekannter `status`) | nein | wie unten |

Schlägt die Abfrage endgültig fehl:

1. Alle TaskIds dieser Anfrage werden wie `PENDING` behandelt und nach `poll-interval` erneut abgefragt.
2. Die Aufgabe läuft weiter; Dateien werden nicht verschoben.
3. Die Ursache wird gemerkt. Endet die Aufgabe später mit `TIMEOUT` (Laufzeit `task-timeout` erreicht) und war die
   letzte Abfrage fehlgeschlagen, nennt der Grund sie, z. B. `maximale Laufzeit (task-timeout) überschritten; letzte
   Statusabfrage (Cloud-API 2) fehlgeschlagen: 503 Service Unavailable from POST http://localhost:8081/tasks/status`.
   Eine spätere erfolgreiche Abfrage löscht die gemerkte Ursache.

Der Consumer wartet auf eine Anfrage höchstens `status-timeout` × (`retries` + 1) + 5 s. In dieser Zeit bemerkt er
weder einen Abbruch noch das Erreichen von `task-timeout`.

## Ausprobieren mit dem Fake-Backend

Das Fake-Backend ändert den Status eines Datensatzes nie von selbst. Jeder Datensatz beginnt mit `PENDING`; den Status
setzt man über einen Admin-Endpunkt.

```bash
# 1. TaskIds der übermittelten Dateien nachsehen
curl http://localhost:8081/tasks

# 2. Status abfragen (TaskIds in der Datei durch die echten ersetzen)
curl -X POST http://localhost:8081/tasks/status -H "Content-Type: application/json" -d "@docs/architect/cloud_api2_status_request.json"

# 3. Status eines Datensatzes setzen und erneut abfragen
curl -X POST "http://localhost:8081/tasks/<taskId>/status?status=SUCCESS"
```

Unbekannte TaskIds lässt das Fake-Backend in `results` weg. Unter Windows PowerShell `curl.exe` statt `curl` verwenden.
