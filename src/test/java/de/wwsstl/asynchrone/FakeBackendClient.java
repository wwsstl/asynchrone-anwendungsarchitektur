package de.wwsstl.asynchrone;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import tools.jackson.databind.JsonNode;

/**
 * HTTP-Zugriff auf das eigenständig laufende {@code FakeCloudBackendApplication}. Der Black-Box-Test kennt nur seine
 * Adresse; er übernimmt die Rolle der Cloud und entscheidet über den Admin-Endpunkt, wie jeder Datensatz ausgeht.
 */
final class FakeBackendClient {

    private final RestClient client;

    FakeBackendClient(String baseUrl) {
        // HTTP/1.1: Den Upgrade-Versuch auf HTTP/2 beantwortet der JDK-HttpServer des Fake-Backends nicht.
        HttpClient http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(http);
        requestFactory.setReadTimeout(Duration.ofSeconds(10));
        this.client = RestClient.builder().baseUrl(baseUrl).requestFactory(requestFactory).build();
    }

    /** Der BatchgenAuftrag, wie ihn Cloud-API 3 liefert. */
    JsonNode job(String jobId) {
        return client.get().uri("/jobs/{jobId}", jobId).retrieve().body(JsonNode.class);
    }

    /** TaskId je Dateiname für alle bisher übermittelten Dateien des BatchgenAuftrags. */
    Map<String, String> taskIds(String jobId) {
        Map<String, String> taskIds = new TreeMap<>();
        for (JsonNode record : client.get().uri("/tasks").retrieve().body(JsonNode.class).path("tasks")) {
            if (jobId.equals(record.path("jobId").asString())) {
                taskIds.put(record.path("fileName").asString(), record.path("taskId").asString());
            }
        }
        return taskIds;
    }

    /** Setzt den Status eines Datensatzes über den Admin-Endpunkt. */
    void decide(String taskId, String status) {
        client.post().uri("/tasks/{taskId}/status?status={status}", taskId, status).retrieve().toBodilessEntity();
    }

    /** Die nächste Übermittlung dieser Datei wird nicht umgewandelt. */
    void rejectNext(String fileName) {
        client.post().uri("/tasks/reject?fileName={fileName}", fileName).retrieve().toBodilessEntity();
    }

    /** Die nächste Übermittlung antwortet mit diesem Status, ohne den Batch zu verarbeiten. */
    void nextResponse(int status) {
        client.post().uri("/tasks/next-response?status={status}", status).retrieve().toBodilessEntity();
    }
}
