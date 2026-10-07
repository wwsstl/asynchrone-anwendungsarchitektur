package de.wwsstl.asynchrone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory();
        requestFactory.setReadTimeout(Duration.ofSeconds(60));
        this.client = RestClient.builder().baseUrl("http://localhost:" + port).requestFactory(requestFactory).build();
        this.base = base;
    }

    // --- REST ------------------------------------------------------------------------------------------------

    TaskSnapshot start(String user) {
        return client.post().uri("/api/users/{u}/tasks", user).retrieve().body(TaskSnapshot.class);
    }

    Response startResponse(String user) {
        return client.post().uri("/api/users/{u}/tasks", user).exchange((request, response) -> new Response(
                response.getStatusCode().value(), response.getHeaders().getFirst("Location"), body(response)));
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

    // --- Benutzerordner --------------------------------------------------------------------------------------

    Path box(String user, String box) {
        return base.resolve(user).resolve(box);
    }

    void drop(String user, String fileName, String content) {
        try {
            Files.createDirectories(box(user, "inbox"));
            Files.writeString(box(user, "inbox").resolve(fileName), content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Legt {@code count} Dateien {@code <user>-<n>.json} mit dem angegebenen Inhalt in die inbox. */
    void dropFiles(String user, int count, String content) {
        for (int i = 0; i < count; i++) {
            drop(user, user + "-" + i + ".json", "{\"data\":\"" + content + "\"}");
        }
    }

    /** Die regulären Dateien eines Ordners (ohne den Marker-Ordner der pendingbox). */
    List<String> names(String user, String box) {
        return list(box(user, box));
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
