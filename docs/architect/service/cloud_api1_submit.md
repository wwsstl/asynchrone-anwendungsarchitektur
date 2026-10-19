# Cloud-API 1: Batch übermitteln

Mit dieser Anfrage übermittelt der Producer einen Batch von Dateien aus der `inbox` an die Cloud
(funktionsweise_sequenz.md, Abschnitt 2). Für jede Datei, die die Cloud in einen Datensatz der Untertabelle umwandelt,
liefert sie eine TaskId; für jede Datei, die sie nicht umwandelt, eine Fehlermeldung. Zugeordnet werden die Einträge
der Antwort über den `identifikator` (derzeit der Dateiname). Umgesetzt in `WebClientCloudClient.submit`, aufgerufen von
`InboxProducer.submit`.

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
| `items[].identifikator` | String | ja | Identifikator der Datei, derzeit ihr Dateiname (z. B. `test_1.json`). Innerhalb einer Anfrage eindeutig, weil alle Dateien eines Batches aus demselben Ordner stammen; in einer späteren Anfrage kann derselbe Wert wieder vorkommen. Über ihn ordnet die Anwendung die Antwort den Dateien zu |
| `items[].content` | beliebiger JSON-Wert | ja | Inhalt der Datei, in JSON umgewandelt. Er wird **als JSON-Wert** übermittelt (meist ein Objekt, möglich sind auch Array, Zahl, Zeichenkette, `true`/`false`, `null`), **nicht** als Zeichenkette mit maskiertem JSON |

Dateien, die sich nicht lesen lassen oder kein gültiges JSON enthalten (auch leere Dateien), werden nicht übermittelt:
Die Anwendung verschiebt sie vorher in die `errorbox` und legt den Grund daneben ab (siehe
[Gründe in der errorbox](#gründe-in-der-errorbox)). Ein Batch enthält deshalb nur gültiges JSON.

### Beispiel

Die Datei [`cloud_api1_submit_request.json`](cloud_api1_submit_request.json) enthält dieses Beispiel ohne Kommentar:

```json
{
  "jobId": "BJ-1",
  "items": [
    {
      "identifikator": "test_1.json",
      "content": {
        "test_1": "test_1"
      }
    },
    {
      "identifikator": "test_2.json",
      "content": {
        "test_2": "test_2"
      }
    },
    {
      "identifikator": "test_3.json",
      "content": {
        "test_3": "test_3"
      }
    }
  ]
}
```

Die Einträge entsprechen den Dateien `test_1.json` bis `test_3.json` in `../../../data/users/0000/KUNDENDATEN/inbox`. `content`
kann jeder gültige JSON-Inhalt sein, auch verschachtelt, z. B.
`{"name":"Beispiel GmbH","adresse":{"plz":"10115","ort":"Berlin"},"tags":["neu","test"],"aktiv":true}`. Die
Anwendung übermittelt das JSON kompakt in einer Zeile; die Einrückung hier dient nur der Lesbarkeit.

**Falsch** wäre der Inhalt als Zeichenkette:

```json
{ "identifikator": "test_1.json", "content": "{\n  \"test_1\": \"test_1\"\n}" }
```

## Antwort

```
200 OK
Content-Type: application/json
```

Der Rumpf enthält je Datei des Batches einen Eintrag: bei einer umgewandelten Datei mit TaskId, bei einer nicht
umgewandelten mit leerer TaskId und Fehlermeldung.

### Felder

| Feld | Typ | Pflicht | Bedeutung |
|---|---|---|---|
| `tasks` | Array | nein | je Datei des Batches ein Eintrag; fehlt das Feld oder ist es leer, wurde keine Datei umgewandelt |
| `tasks[].identifikator` | String | ja | unverändert aus der Anfrage (`items[].identifikator`) |
| `tasks[].taskId` | String | ja | umgewandelt: Nummer des angelegten Datensatzes in der Untertabelle. Die Anwendung sichert sie als Marker in `pendingbox/.taskids/<Dateiname>` und fragt mit ihr den Status ab (Cloud-API 2). Das Format bestimmt die Cloud; das Fake-Backend vergibt UUIDs. Nicht umgewandelt: leer (`""`) |
| `tasks[].fehlermeldung` | String | nein | nicht umgewandelt: warum die Cloud die Datei nicht umgewandelt hat; die Anwendung legt sie als Grund in der `errorbox` ab. Umgewandelt: leer (`""`) |

### Beispiel

Antwort auf das [Beispiel der Anfrage](#beispiel); die Datei
[`cloud_api1_submit_response.json`](cloud_api1_submit_response.json) enthält sie ohne Kommentar:

```json
{
  "tasks": [
    {
      "identifikator": "test_1.json",
      "taskId": "3f2b9c1e-6a4d-4f0e-9b7a-1c2d3e4f5a6b",
      "fehlermeldung": ""
    },
    {
      "identifikator": "test_2.json",
      "taskId": "8d7c6b5a-4e3f-4a2b-9c1d-0e9f8a7b6c5d",
      "fehlermeldung": ""
    },
    {
      "identifikator": "test_3.json",
      "taskId": "",
      "fehlermeldung": "Pflichtfeld 'name' fehlt"
    }
  ]
}
```

So verarbeitet die Anwendung diese Antwort:

| Datei | `taskId` | Folge |
|---|---|---|
| `test_1.json` | vorhanden | TaskId als Marker sichern, Datei in den Status-Pool eintragen; der Consumer fragt ihren Status ab |
| `test_2.json` | vorhanden | wie `test_1.json` |
| `test_3.json` | leer | **nicht umgewandelt**: Datei in die `errorbox`, Grund mit der Fehlermeldung daneben ablegen, Fehlerzähler + 1 |

### Regeln für die Auswertung

- Die Anwendung ordnet die Einträge über `identifikator` den Dateien des Batches zu; die Reihenfolge spielt keine Rolle.
  Die Cloud muss ihn unverändert zurückgeben (verglichen wird die genaue Zeichenkette).
- Ein Eintrag mit TaskId bedeutet: umgewandelt. Ein Eintrag mit leerer oder fehlender `taskId` bedeutet: nicht
  umgewandelt, Grund in `fehlermeldung`.
- Fehlt eine Datei des Batches ganz in `tasks`, gilt sie ebenfalls als nicht umgewandelt; als Grund steht dann
  „keine Fehlermeldung der Cloud“. Dasselbe gilt für eine leere oder fehlende `fehlermeldung`.
- Einträge ohne `identifikator` und Einträge mit einem Identifikator, der nicht im Batch war, werden ignoriert.
- Eine leere Antwort `{"tasks":[]}` bedeutet: keine Datei umgewandelt, alle Dateien des Batches kommen in die `errorbox`.

## Gründe in der errorbox

Für jede Datei, die in die `errorbox` kommt, legt die Anwendung den Grund in
`errorbox/.fehler/<Dateiname>.txt` ab und schreibt ihn zusätzlich ins Log. Für `test_3.json` aus dem Beispiel:

```
Aufgabe: BJ-1
Grund: Cloud-API 1 (Batch übermitteln) hat die Datei nicht umgewandelt: Pflichtfeld 'name' fehlt
```

Weitere Gründe, die beim Übermitteln entstehen können:

| Fall | `Grund:` |
|---|---|
| Datei fehlt in der Antwort oder `fehlermeldung` ist leer | `Cloud-API 1 (Batch übermitteln) hat die Datei nicht umgewandelt: keine Fehlermeldung der Cloud` |
| Datei ist kein gültiges JSON | `Datei nicht lesbar oder kein gültiges JSON: <Meldung des JSON-Parsers>` |
| Datei ist leer | `Datei nicht lesbar oder kein gültiges JSON: Datei ist leer` |

Der Unterordner `.fehler` zählt nicht als Inhalt der `errorbox`. Bekommt eine Datei beim Verschieben einen Zusatz,
weil es den Namen in der `errorbox` schon gibt (z. B. `test_3-1f3c9a2b.json`), heißt die Grund-Datei entsprechend
(`test_3-1f3c9a2b.json.txt`).

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
curl -X POST http://localhost:8081/jobs -H "Content-Type: application/json" \
     -d '{"userId":"0000","jobType":"KUNDENDATEN","jobName":"Testlauf"}'

# 2. Optional: test_3.json bei der nächsten Übermittlung mit Fehlermeldung ablehnen lassen
curl -X POST "http://localhost:8081/tasks/reject?fileName=test_3.json&fehlermeldung=Pflichtfeld%20'name'%20fehlt"

# 3. Batch übermitteln (jobId in der Datei gegebenenfalls anpassen)
curl -X POST http://localhost:8081/tasks -H "Content-Type: application/json" -d "@docs/architect/service/cloud_api1_submit_request.json"

# 4. Datensätze ansehen
curl http://localhost:8081/tasks
```

Unter Windows PowerShell `curl.exe` statt `curl` verwenden; das JSON in Schritt 1 dann mit
`'{\"userId\":\"0000\",\"jobType\":\"KUNDENDATEN\",\"jobName\":\"Testlauf\"}'` übergeben. Ohne `jobType`
antwortet das Fake-Backend mit `400` (`userId oder jobType fehlt`).

Das Fake-Backend wertet `content` nicht aus; es wandelt jede Datei um, außer sie wurde vorher mit
`POST /tasks/reject?fileName=…` markiert. Ohne `fehlermeldung` antwortet es dann mit
`nicht umgewandelt (vorgegeben mit /tasks/reject)`.
