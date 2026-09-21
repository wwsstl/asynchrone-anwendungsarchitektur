package de.wwsstl.asynchrone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.context.TaskState;

/** Hilfsklasse der Integrationstests: REST-Aufrufe gegen den laufenden Server und Zugriff auf die Benutzerordner. */
final class PipelineApi {

    private final RestClient client;
    private final Path base;

    PipelineApi(int port, Path base) {
        // Großzügiges Read-Timeout: Der Default (10 s) reißt auf überlasteten Build-Rechnern schon bei der ersten
        // Anfrage an einen frisch gestarteten Tomcat.
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory();
        requestFactory.setReadTimeout(Duration.ofSeconds(60));
        this.client = RestClient.builder().baseUrl("http://localhost:" + port).requestFactory(requestFactory)
                .build();
        this.base = base;
    }

    // --- REST ------------------------------------------------------------------------------------------------

    TaskSnapshot start(String user) {
        return client.post().uri("/api/users/{u}/tasks", user).retrieve().body(TaskSnapshot.class);
    }

    int startStatus(String user) {
        return client.post().uri("/api/users/{u}/tasks", user).exchange((req, res) -> res.getStatusCode().value());
    }

    TaskSnapshot status(String user, UUID taskId) {
        return client.get().uri("/api/users/{u}/tasks/{t}", user, taskId).retrieve().body(TaskSnapshot.class);
    }

    int statusCode(String user, String taskId) {
        return client.get().uri("/api/users/{u}/tasks/{t}", user, taskId)
                .exchange((req, res) -> res.getStatusCode().value());
    }

    TaskSnapshot cancel(String user, UUID taskId) {
        return client.post().uri("/api/users/{u}/tasks/{t}/cancel", user, taskId).retrieve()
                .body(TaskSnapshot.class);
    }

    int cancelStatus(String user, UUID taskId) {
        return client.post().uri("/api/users/{u}/tasks/{t}/cancel", user, taskId)
                .exchange((req, res) -> res.getStatusCode().value());
    }

    TaskSnapshot awaitFinished(String user, UUID taskId) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50))
                .until(() -> status(user, taskId).state() != TaskState.RUNNING);
        return status(user, taskId);
    }

    // --- Ordner ----------------------------------------------------------------------------------------------

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

    /** Legt {@code count} Dateien {@code <user>-<n>.txt} mit dem angegebenen Inhalt ab. */
    void dropFiles(String user, int count, String content) {
        for (int i = 0; i < count; i++) {
            drop(user, user + "-" + i + ".txt", content);
        }
    }

    List<String> names(String user, String box) {
        try (Stream<Path> files = Files.list(box(user, box))) {
            return files.map(p -> p.getFileName().toString()).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
