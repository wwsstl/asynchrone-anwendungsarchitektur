package de.wwsstl.asynchrone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.context.TaskState;
import de.wwsstl.asynchrone.taskmanager.CancelResult;

/** Hilfsklasse der Integrationstests: REST-Aufrufe gegen den laufenden Server und Zugriff auf die Benutzerordner. */
final class PipelineApi {

    /** Statuscode und Body einer Antwort; bei Fehlern ein ProblemDetail. */
    record Response(int status, Map<String, Object> body) {

        /** Der Ursachencode eines abgewiesenen Aufrufs. */
        Object code() {
            return body == null ? null : body.get("code");
        }
    }

    private final RestClient client;
    private final Path base;

    PipelineApi(int port, Path base) {
        // Großzügiges Read-Timeout: Der Default (10 s) reißt auf überlasteten Build-Rechnern schon bei der ersten
        // Anfrage an einen frisch gestarteten Server.
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

    Response startResponse(String user) {
        return client.post().uri("/api/users/{u}/tasks", user).exchange((req, res) -> response(res));
    }

    int startStatus(String user) {
        return startResponse(user).status();
    }

    TaskSnapshot status(String user, String taskId) {
        return client.get().uri("/api/users/{u}/tasks/{t}", user, taskId).retrieve().body(TaskSnapshot.class);
    }

    int statusCode(String user, String taskId) {
        return client.get().uri("/api/users/{u}/tasks/{t}", user, taskId)
                .exchange((req, res) -> res.getStatusCode().value());
    }

    CancelResult cancel(String user, String taskId) {
        return client.post().uri("/api/users/{u}/tasks/{t}/cancel", user, taskId).retrieve()
                .body(CancelResult.class);
    }

    Response cancelResponse(String user, String taskId) {
        return client.post().uri("/api/users/{u}/tasks/{t}/cancel", user, taskId).exchange((req, res) -> response(res));
    }

    int cancelStatus(String user, String taskId) {
        return cancelResponse(user, taskId).status();
    }

    /** Wartet auf einen Endzustand ({@code COMPLETED} oder {@code CANCELLED}) des letzten Laufs. */
    TaskSnapshot awaitFinished(String user, String taskId) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50))
                .until(() -> isFinal(status(user, taskId).state()));
        return status(user, taskId);
    }

    private static boolean isFinal(TaskState state) {
        return state == TaskState.COMPLETED || state == TaskState.CANCELLED;
    }

    @SuppressWarnings("unchecked")
    private static Response response(RestClient.RequestHeadersSpec.ConvertibleClientHttpResponse res)
            throws IOException {
        int status = res.getStatusCode().value();
        return new Response(status, res.bodyTo(Map.class));
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

    /** Die regulären Dateien eines Ordners (ohne Unterordner wie {@code pendingbox/.taskids}). */
    List<String> names(String user, String box) {
        try (Stream<Path> files = Files.list(box(user, box))) {
            return files.filter(Files::isRegularFile).map(p -> p.getFileName().toString()).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Leert die {@code pendingbox} samt TaskId-Markern, wie es die Benutzer:in von Hand tut (Weg b). */
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
}
