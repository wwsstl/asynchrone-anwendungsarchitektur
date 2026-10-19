package de.wwsstl.asynchrone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import de.wwsstl.asynchrone.task.TaskSnapshot;
import de.wwsstl.asynchrone.task.TaskStatus;

/** Hilfsklasse der Integrationstests: REST-Aufrufe gegen den laufenden Server und Zugriff auf die Benutzerordner. */
final class PipelineApi {

    /** Statuscode, Location-Header und Body einer Antwort; bei Fehlern ein ProblemDetail. */
    record Response(int status, String location, Map<String, Object> body) {

        /** Der Ursachencode eines Fehlers. */
        Object code() {
            return body == null ? null : body.get("code");
        }
    }

    private final RestClient client;
    private final Path base;

    PipelineApi(int port, Path base) {
        // Nur HTTP/1.1: sonst versucht der JDK-Client auf jeder neuen Verbindung ein Upgrade auf h2c, und eine
        // POST-Anfrage mit Rumpf bleibt dabei hängen, weil der Server kein HTTP/2 spricht.
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(
                HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build());
        requestFactory.setReadTimeout(Duration.ofSeconds(60));
        this.client = RestClient.builder().baseUrl("http://localhost:" + port).requestFactory(requestFactory).build();
        this.base = base;
    }

    // --- REST ------------------------------------------------------------------------------------------------

    /** Art, wenn ein Test nichts anderes angibt: beim Start und für die Ordner. */
    static final String DEFAULT_JOB_TYPE = "KUNDENDATEN";

    /** Rumpf der Startanfrage, wenn ein Test nichts anderes angibt. */
    static final Map<String, String> DEFAULT_START = Map.of("jobType", DEFAULT_JOB_TYPE);

    TaskSnapshot start(String user) {
        return client.post().uri("/api/users/{u}/tasks", user).contentType(MediaType.APPLICATION_JSON)
                .body(DEFAULT_START).retrieve().body(TaskSnapshot.class);
    }

    Response startResponse(String user) {
        return startResponse(user, DEFAULT_START);
    }

    /** Startanfrage mit diesem Rumpf: ein Objekt, das als JSON geschrieben wird, oder ein String als roher Rumpf. */
    Response startResponse(String user, Object body) {
        RestClient.RequestBodySpec request = client.post().uri("/api/users/{u}/tasks", user)
                .contentType(MediaType.APPLICATION_JSON);
        if (body != null) {
            request.body(body);
        }
        return request.exchange((req, response) -> new Response(response.getStatusCode().value(),
                response.getHeaders().getFirst("Location"), body(response)));
    }

    TaskStatus status(String user, String taskNumber) {
        return client.get().uri("/api/users/{u}/tasks/{t}", user, taskNumber).retrieve().body(TaskStatus.class);
    }

    Response statusResponse(String user, String taskNumber) {
        return client.get().uri("/api/users/{u}/tasks/{t}", user, taskNumber)
                .exchange((request, response) -> new Response(response.getStatusCode().value(), null, body(response)));
    }

    Response cancel(String user, String taskNumber) {
        return client.post().uri("/api/users/{u}/tasks/{t}/cancel", user, taskNumber)
                .exchange((request, response) -> new Response(response.getStatusCode().value(), null, body(response)));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> body(RestClient.RequestHeadersSpec.ConvertibleClientHttpResponse response)
            throws IOException {
        return response.bodyTo(Map.class);
    }

    // --- Benutzerordner (ohne Angabe der Art die von DEFAULT_JOB_TYPE) -------------------------------------

    Path box(String user, String box) {
        return box(user, DEFAULT_JOB_TYPE, box);
    }

    Path box(String user, String jobType, String box) {
        return base.resolve(user).resolve(jobType).resolve(box);
    }

    /**
     * Legt eine Datei mit dem JSON-Inhalt {@code {"data":"<marker>"}} in die inbox; der Marker steuert die
     * {@link FakeCloud} ({@code REJECT}, {@code FAIL}, {@code SLOW}).
     */
    void drop(String user, String fileName, String marker) {
        drop(user, DEFAULT_JOB_TYPE, fileName, marker);
    }

    void drop(String user, String jobType, String fileName, String marker) {
        dropRaw(user, jobType, fileName, "{\"data\":\"" + marker + "\"}");
    }

    /** Legt eine Datei mit genau diesem Inhalt in die inbox, z. B. ungültiges JSON. */
    void dropRaw(String user, String fileName, String content) {
        dropRaw(user, DEFAULT_JOB_TYPE, fileName, content);
    }

    void dropRaw(String user, String jobType, String fileName, String content) {
        Path inbox = box(user, jobType, "inbox");
        try {
            Files.createDirectories(inbox);
            Files.writeString(inbox.resolve(fileName), content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Legt {@code count} Dateien {@code <user>-<n>.json} mit dem angegebenen Marker in die inbox. */
    void dropFiles(String user, int count, String marker) {
        for (int i = 0; i < count; i++) {
            drop(user, user + "-" + i + ".json", marker);
        }
    }

    /** Inhalt der Datei mit dem Grund zu einer Datei der errorbox; {@code null}, wenn es keinen gibt. */
    String errorReason(String user, String fileName) {
        Path reason = box(user, "errorbox").resolve(".fehler").resolve(fileName + ".txt");
        try {
            return Files.exists(reason) ? Files.readString(reason) : null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Die regulären Dateien eines Ordners (ohne den Marker-Ordner der pendingbox). */
    List<String> names(String user, String box) {
        return list(box(user, box));
    }

    List<String> names(String user, String jobType, String box) {
        return list(box(user, jobType, box));
    }

    /** Die TaskId-Marker in der pendingbox. */
    List<String> markers(String user) {
        return list(box(user, "pendingbox").resolve(".taskids"));
    }

    void clearPendingbox(String user) {
        try (Stream<Path> files = Files.walk(box(user, "pendingbox"))) {
            files.filter(Files::isRegularFile).forEach(file -> {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<String> list(Path dir) {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(Files::isRegularFile).map(path -> path.getFileName().toString()).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
