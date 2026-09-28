package de.wwsstl.asynchrone.taskmanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import de.wwsstl.asynchrone.MutableClock;
import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.CloudStatus;
import de.wwsstl.asynchrone.cloud.SubmittedTask;
import de.wwsstl.asynchrone.cloud.TaskStatusResult;
import de.wwsstl.asynchrone.cloud.TestdataItem;
import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.context.Sandbox;
import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.context.TaskState;
import de.wwsstl.asynchrone.files.NioUserFolderResolver;
import de.wwsstl.asynchrone.pool.TaskId;
import de.wwsstl.asynchrone.registry.InMemoryTaskHistory;
import de.wwsstl.asynchrone.registry.InMemoryTaskRegistry;
import de.wwsstl.asynchrone.registry.TaskHistory;
import de.wwsstl.asynchrone.registry.TaskRegistry;

/** Abmeldung beendeter Tasks und Aufbewahrungsfrist ihres Endzustands ({@code pipeline.task-retention}). */
class TaskManagerLifecycleTest {

    private static final Duration RETENTION = Duration.ofHours(1);

    /** Cloud, die jeden Auftrag annimmt und ihn erst nach {@link #release()} erfolgreich abschließt. */
    private static final class ReleasableCloud implements CloudClient {

        private final AtomicBoolean released = new AtomicBoolean();

        void release() {
            released.set(true);
        }

        @Override
        public CompletableFuture<List<SubmittedTask>> submit(List<TestdataItem> items) {
            return CompletableFuture.completedFuture(items.stream()
                    .map(item -> new SubmittedTask(item.fileName(), new TaskId(UUID.randomUUID().toString())))
                    .toList());
        }

        @Override
        public CompletableFuture<List<TaskStatusResult>> queryStatus(List<TaskId> taskIds) {
            CloudStatus status = released.get() ? CloudStatus.SUCCESS : CloudStatus.PENDING;
            return CompletableFuture.completedFuture(taskIds.stream()
                    .map(id -> new TaskStatusResult(id, status))
                    .toList());
        }
    }

    @TempDir
    Path base;

    private final MutableClock clock = new MutableClock();
    private final TaskRegistry registry = new InMemoryTaskRegistry();
    private final TaskHistory history = new InMemoryTaskHistory();
    private final ReleasableCloud cloud = new ReleasableCloud();
    private TaskManager taskManager;

    @BeforeEach
    void setUp() {
        // Task-Timeout weit jenseits der Frist, damit das Vorstellen der Uhr keinen laufenden Task beendet.
        PipelineProperties properties = new PipelineProperties(base, 4, 2, Duration.ofMillis(10),
                Duration.ofMillis(10), 200, 10, Duration.ofDays(30), RETENTION,
                new PipelineProperties.Cloud(URI.create("http://localhost:8081"), "/tasks", "/tasks/status",
                        Duration.ofSeconds(30), Duration.ofSeconds(30), 0, 0));
        taskManager = new TaskManager(registry, history, new NioUserFolderResolver(properties), cloud, properties,
                clock);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        taskManager.shutdown();
        for (Sandbox sandbox : registry.all()) {
            sandbox.producerThread().join(Duration.ofSeconds(5));
            sandbox.consumerThread().join(Duration.ofSeconds(5));
        }
    }

    @Test
    void beendeterTaskWirdSamtSandboxAbgemeldetBleibtAberAbfragbar() throws IOException {
        dropFile("alice", "a.txt");
        UUID taskId = taskManager.start("alice").taskId();
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> taskManager.status("alice", taskId).pending() == 1);
        Sandbox sandbox = registry.find(taskId).orElseThrow();

        cloud.release();
        clock.advance(Duration.ofSeconds(1)); // macht den Eintrag erneut fällig (poll-interval 10 ms)
        awaitDeregistered(taskId);

        // Threads beendet, Sandbox nicht mehr im Register ...
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !sandbox.isAlive());
        assertThat(registry.find(taskId)).isEmpty();
        // ... der Endzustand aber vollständig über die Status-Abfrage erhalten
        TaskSnapshot finished = taskManager.status("alice", taskId);
        assertThat(finished.state()).isEqualTo(TaskState.COMPLETED);
        assertThat(finished.finishedAt()).isNotNull();
        assertThat(finished.submitted()).isEqualTo(1);
        assertThat(finished.succeeded()).isEqualTo(1);
        assertThat(finished.pending()).isZero();
        // Auch die Startsperre ist freigegeben.
        assertThat(taskManager.start("alice").taskId()).isNotEqualTo(taskId);
    }

    @Test
    void endzustandBleibtBisZumAblaufDerFristAbfragbar() {
        UUID taskId = taskManager.start("bob").taskId();
        awaitDeregistered(taskId);

        clock.advance(RETENTION.minusSeconds(1));
        assertThat(taskManager.status("bob", taskId).state()).isEqualTo(TaskState.COMPLETED);

        clock.advance(Duration.ofSeconds(1));
        assertThatThrownBy(() -> taskManager.status("bob", taskId)).isInstanceOf(TaskNotFoundException.class);
        assertThatThrownBy(() -> taskManager.cancel("bob", taskId)).isInstanceOf(TaskNotFoundException.class);
        assertThat(history.find(taskId)).isEmpty();
    }

    @Test
    void startEntferntAbgelaufeneEintraegeAndererBenutzer() {
        UUID a = taskManager.start("a").taskId();
        UUID b = taskManager.start("b").taskId();
        awaitDeregistered(a);
        awaitDeregistered(b);
        clock.advance(RETENTION);

        taskManager.start("c");

        assertThat(history.find(a)).isEmpty();
        assertThat(history.find(b)).isEmpty();
    }

    @Test
    void abbruchLoeschtDenEndzustandAusDerHistorie() {
        UUID taskId = taskManager.start("carol").taskId();
        awaitDeregistered(taskId);

        assertThat(taskManager.cancel("carol", taskId).state()).isEqualTo(TaskState.COMPLETED);

        assertThat(history.find(taskId)).isEmpty();
        assertThatThrownBy(() -> taskManager.status("carol", taskId)).isInstanceOf(TaskNotFoundException.class);
    }

    @Test
    void abgebrochenerTaskWirdNachDemAuslaufenNichtArchiviert() throws IOException {
        dropFile("dave", "a.txt");
        UUID taskId = taskManager.start("dave").taskId();
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> taskManager.status("dave", taskId).pending() == 1);
        Sandbox sandbox = registry.find(taskId).orElseThrow();

        taskManager.cancel("dave", taskId);
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !sandbox.isAlive());

        assertThat(history.find(taskId)).isEmpty();
        assertThatThrownBy(() -> taskManager.status("dave", taskId)).isInstanceOf(TaskNotFoundException.class);
    }

    @Test
    void laufendeTasksWerdenNieEntfernt() throws IOException {
        dropFile("erin", "a.txt");
        UUID taskId = taskManager.start("erin").taskId();
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> taskManager.status("erin", taskId).pending() == 1);

        clock.advance(RETENTION.multipliedBy(10));
        taskManager.start("frank");

        assertThat(taskManager.status("erin", taskId).state()).isEqualTo(TaskState.RUNNING);
        assertThat(registry.find(taskId)).isPresent();
    }

    private void dropFile(String user, String fileName) throws IOException {
        Files.createDirectories(base.resolve(user).resolve("inbox"));
        Files.writeString(base.resolve(user).resolve("inbox").resolve(fileName), fileName);
    }

    /** Wartet, bis der Task abgemeldet ist: nicht mehr im Register, aber in der Historie. */
    private void awaitDeregistered(UUID taskId) {
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> registry.find(taskId).isEmpty() && history.find(taskId).isPresent());
    }
}
