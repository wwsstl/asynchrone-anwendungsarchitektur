package de.wwsstl.asynchrone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import de.wwsstl.asynchrone.cloud.JobStatus;
import de.wwsstl.asynchrone.task.TaskStatus;

/**
 * Basis der Integrationstests: die vollständige Anwendung auf einem zufälligen Port gegen eine {@link FakeCloud}, mit
 * kurzen Intervallen. Jeder Test arbeitet mit eigenen Benutzer:innen, damit sich die Tests nicht beeinflussen.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
abstract class PipelineTestSupport {

    /** Gemeinsam für alle Testklassen; der Prozess der Tests beendet den Server. */
    static final FakeCloud cloud = new FakeCloud();

    static final Path base = Path.of("target", "it-data", UUID.randomUUID().toString()).toAbsolutePath();

    @DynamicPropertySource
    static void pipeline(DynamicPropertyRegistry registry) {
        registry.add("pipeline.base-directory", base::toString);
        registry.add("pipeline.batch-size", () -> 4);
        registry.add("pipeline.pool-resume-threshold", () -> 2);
        registry.add("pipeline.sweep-interval", () -> "50ms");
        registry.add("pipeline.poll-interval", () -> "100ms");
        registry.add("pipeline.status-bulk-size", () -> 3);
        registry.add("pipeline.error-threshold", () -> 3);
        registry.add("pipeline.submit-retry-max-wait", () -> "200ms");
        registry.add("pipeline.cloud.base-url", cloud::baseUrl);
        registry.add("pipeline.cloud.submit-timeout", () -> "1s");
        registry.add("pipeline.cloud.status-timeout", () -> "1s");
        registry.add("pipeline.cloud.retries", () -> 1);
    }

    @LocalServerPort
    int port;

    PipelineApi api;

    @BeforeEach
    void connect() {
        api = new PipelineApi(port, base);
    }

    static ConditionFactory eventually() {
        return await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50));
    }

    /** Wartet, bis die Cloud den Endzustand kennt und die Sandbox abgemeldet ist; liefert den Status danach. */
    TaskStatus awaitEnd(String user, String taskNumber, JobStatus expected) {
        AtomicReference<TaskStatus> last = new AtomicReference<>();
        eventually().untilAsserted(() -> {
            TaskStatus status = api.status(user, taskNumber);
            last.set(status);
            assertThat(status.local()).as("lokaler Stand nach der Abmeldung").isNull();
            assertThat(status.cloud().status()).isEqualTo(expected);
        });
        return last.get();
    }

    /** Wartet, bis so viele Dateien der Aufgabe im Status-Pool auf ihr Ergebnis warten. */
    void awaitPending(String user, String taskNumber, int pending) {
        eventually().untilAsserted(() -> assertThat(api.status(user, taskNumber).local().pending()).isEqualTo(pending));
    }

    /** Die von Cloud-API 1 verarbeiteten Dateinamen der Benutzer:in. */
    static List<String> submittedBy(String user) {
        return cloud.submittedFileNames().stream().filter(name -> name.startsWith(user + "-")).toList();
    }

    static String userOf(String fileName) {
        return fileName.substring(0, fileName.indexOf('-'));
    }
}
