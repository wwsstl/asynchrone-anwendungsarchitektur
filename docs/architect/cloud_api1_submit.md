# Cloud-API 1: Batch übermitteln

Mit dieser Anfrage übermittelt der Producer einen Batch von Dateien aus der `inbox` an die Cloud
(funktionsweise_sequenz.md, Abschnitt 2). Für jede Datei, die die Cloud in einen Datensatz der Untertabelle umwandelt,
liefert sie eine TaskId. Umgesetzt in `WebClientCloudClient.submit`, aufgerufen von `InboxProducer.submit`.

## Anfrage

```
POST {pipeline.cloud.base-url}{pipeline.cloud.submit-path}
Content-Type: application/json
```

Standard: `POST http://localhost:8081/tasks`. Der Rumpf ist UTF-8-kodiertes JSON.

### Felder

| Feld | Typ | Pflicht | Bedeutung |
|---|---|---|---|
| `jobId` | String | ja | Nummer des BatchgenAuftrags (= Aufgabennummer), die Cloud-API 1 beim Start geliefert hat, z. B. `BJ-1` |
| `items` | Array | ja | die Dateien des Batches; mindestens 1, höchstens `pipeline.batch-size` Einträge (Standard 4) |
| `items[].fileName` | String | ja | Dateiname in der `inbox` der Benutzer:in; innerhalb eines Batches eindeutig. Über ihn ordnet die Anwendung die Antwort den Dateien zu |
| `items[].content` | beliebiger JSON-Wert | ja | Inhalt der Datei, in JSON umgewandelt. Er wird **als JSON-Wert** übermittelt (meist ein Objekt, möglich sind auch Array, Zahl, Zeichenkette, `true`/`false`, `null`), **nicht** als Zeichenkette mit maskiertem JSON |

Dateien, die sich nicht lesen lassen oder kein gültiges JSON enthalten (auch leere Dateien), werden nicht übermittelt:
Die Anwendung verschiebt sie vorher in die `errorbox`. Ein Batch enthält deshalb nur gültiges JSON.

### Beispiel

Die Datei [`cloud_api1_submit_request.json`](cloud_api1_submit_request.json) enthält dieses Beispiel ohne Kommentar:

```json
{
  "jobId": "BJ-1",
  "items": [
    {
      "fileName": "test_1.json",
      "content": {
        "test_1": "test_1"
      }
    },
    {
      "fileName": "test_2.json",
      "content": {
        "test_2": "test_2"
      }
    }
  ]
}
```

Die beiden Einträge entsprechen den Dateien `test_1.json` und `test_2.json` in `data/users/0000/inbox`. `content` kann
jeder gültige JSON-Inhalt sein, auch verschachtelt, z. B.
`{"name":"Beispiel GmbH","adresse":{"plz":"10115","ort":"Berlin"},"tags":["neu","test"],"aktiv":true}`. Die
Anwendung übermittelt das JSON kompakt in einer Zeile; die Einrückung hier dient nur der Lesbarkeit.

**Falsch** wäre der Inhalt als Zeichenkette:

```json
{ "fileName": "test_1.json", "content": "{\n  \"test_1\": \"test_1\"\n}" }
```

## Antwort

```
200 OK
Content-Type: application/json
```

Der Rumpf enthält je umgewandelter Datei ihre TaskId.

### Felder

| Feld | Typ | Pflicht | Bedeutung |
|---|---|---|---|
| `tasks` | Array | nein | die umgewandelten Dateien; fehlt das Feld oder ist es leer, wurde keine Datei umgewandelt |
| `tasks[].fileName` | String | ja | Dateiname aus der Anfrage (`items[].fileName`) |
| `tasks[].taskId` | String, nicht leer | ja | Nummer des angelegten Datensatzes in der Untertabelle. Die Anwendung sichert sie als Marker in `pendingbox/.taskids/<fileName>` und fragt mit ihr den Status ab (Cloud-API 2). Das Format bestimmt die Cloud; das Fake-Backend vergibt UUIDs |

### Beispiel

Antwort auf das [Beispiel der Anfrage](#beispiel); die Datei
[`cloud_api1_submit_response.json`](cloud_api1_submit_response.json) enthält sie ohne Kommentar:

```json
{
  "tasks": [
    {
      "fileName": "test_1.json",
      "taskId": "3f2b9c1e-6a4d-4f0e-9b7a-1c2d3e4f5a6b"
    },
    {
      "fileName": "test_2.json",
      "taskId": "8d7c6b5a-4e3f-4a2b-9c1d-0e9f8a7b6c5d"
    }
  ]
}
```

So verarbeitet die Anwendung diese Antwort:

| Datei | in `tasks` | Folge |
|---|---|---|
| `test_1.json` | ja | TaskId als Marker sichern, Datei in den Status-Pool eintragen; der Consumer fragt ihren Status ab |
| `test_2.json` | ja | wie `test_1.json` |

Hätte die Cloud `test_2.json` **nicht umgewandelt**, fehlte die Datei in `tasks`:

```json
{ "tasks": [ { "fileName": "test_1.json", "taskId": "3f2b9c1e-6a4d-4f0e-9b7a-1c2d3e4f5a6b" } ] }
```

Dann käme `test_2.json` in die `errorbox`, und der Fehlerzähler der Aufgabe stiege um eins.

### Regeln für die Auswertung

- Die Anwendung ordnet die Einträge über `fileName` den Dateien des Batches zu; die Reihenfolge spielt keine Rolle.
- Jede Datei des Batches, für die kein Eintrag mit TaskId kommt, gilt als nicht umgewandelt.
- Einträge ohne `fileName`, ohne `taskId` oder mit leerer `taskId` werden ignoriert; die betroffene Datei gilt damit
  ebenfalls als nicht umgewandelt.
- Einträge zu Dateinamen, die nicht im Batch waren, werden ignoriert.
- Eine leere Antwort `{"tasks":[]}` bedeutet: keine Datei umgewandelt, alle Dateien des Batches kommen in die `errorbox`.

## Fehler und ihre Behandlung

Die Anfrage ist **nicht idempotent** und wird nie automatisch wiederholt. Die Anwendung wartet höchstens
`pipeline.cloud.submit-timeout` auf die Antwort und ordnet Fehler so ein:

| Antwort / Fehler | Einordnung | Folge in der Anwendung |
|---|---|---|
| `4xx` | eindeutig nicht verarbeitet | Dateien zurück in die `inbox`, erneuter Versuch nach einer Wartezeit (beginnend bei `sweep-interval`, verdoppelt bis `submit-retry-max-wait`) |
| `503 Service Unavailable` | eindeutig nicht verarbeitet | wie `4xx` |
| Verbindung abgelehnt, Host unbekannt, keine Route | eindeutig nicht verarbeitet | wie `4xx` |
| keine Antwort innerhalb von `submit-timeout` | Ergebnis unklar | Dateien bleiben ohne Marker in der `pendingbox`; die Aufgabe endet sofort mit `TIMEOUT` (Grund z. B. `Cloud-API 1 (Batch übermitteln): Ergebnis unklar, keine Antwort innerhalb von submit-timeout`) |
| sonstige `5xx` (z. B. `500`) | Ergebnis unklar | wie Timeout |
| Verbindung während der Anfrage abgebrochen | Ergebnis unklar | wie Timeout |
| Antwort nicht lesbar | Ergebnis unklar | wie Timeout |

Bei einem unklaren Ergebnis kann die Cloud den Batch bereits verarbeitet haben; die Dateien müssen von Hand geprüft
werden (funktionsweise_sequenz.md, Abschnitt 7).

## Ausprobieren mit dem Fake-Backend

```bash
# 1. BatchgenAuftrag anlegen; die Antwort enthält die jobId, z. B. {"jobId":"BJ-1"}
curl -X POST http://localhost:8081/jobs -H "Content-Type: application/json" -d '{"userId":"0000"}'

# 2. Batch übermitteln (jobId in der Datei gegebenenfalls anpassen)
curl -X POST http://localhost:8081/tasks -H "Content-Type: application/json" -d "@docs/architect/cloud_api1_submit_request.json"

# 3. Datensätze ansehen
curl http://localhost:8081/tasks
```

Unter Windows PowerShell `curl.exe` statt `curl` verwenden; das JSON in Schritt 1 dann mit `'{\"userId\":\"0000\"}'`
übergeben. Das Fake-Backend wertet `content` nicht aus; es wandelt jede Datei um, außer sie wurde vorher mit
`POST /tasks/reject?fileName=…` markiert.
