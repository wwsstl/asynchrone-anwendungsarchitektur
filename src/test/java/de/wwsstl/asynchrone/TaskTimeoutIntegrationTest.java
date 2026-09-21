package de.wwsstl.asynchrone;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.FileSystemUtils;

import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.context.TaskState;

/** Prüft die im Consumer-Zyklus integrierte Timeout-Erkennung (loesung_final.md 4.4 / 4.8). */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TaskTimeoutIntegrationTest {

    private static final FakeCloud cloud = new FakeCloud();
    private static final Path base = createBase();

    private static Path createBase() {
        try {
            return Files.createTempDirectory("pipeline-timeout-it");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("pipeline.cloud.base-url", cloud::baseUrl);
        registry.add("pipeline.base-directory", base::toString);
        registry.add("pipeline.sweep-interval", () -> "50ms");
        registry.add("pipeline.poll-interval", () -> "100ms");
        registry.add("pipeline.task-timeout", () -> "1s");
    }

    @AfterAll
    static void cleanUp() throws IOException {
        cloud.close();
        FileSystemUtils.deleteRecursively(base);
    }

    @Value("${local.server.port}")
    int port;

    @Test
    void zeitueberschreitungBrichtDenTaskAbUndBehandeltOffeneDateienAlsFehler() {
        PipelineApi api = new PipelineApi(port, base);
        api.dropFiles("slow", 3, "SLOW");

        TaskSnapshot slow = api.start("slow");

        TaskSnapshot done = api.awaitFinished("slow", slow.taskId());
        assertThat(done.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(done.cancelReason()).isEqualTo(CancelReason.TIMEOUT);
        assertThat(done.failed()).isEqualTo(3);
        assertThat(api.names("slow", "errorbox")).hasSize(3);
        assertThat(api.names("slow", "inbox")).isEmpty();
    }
}
