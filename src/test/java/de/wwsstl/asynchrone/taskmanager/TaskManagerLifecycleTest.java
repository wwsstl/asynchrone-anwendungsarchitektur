package de.wwsstl.asynchrone.taskmanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import de.wwsstl.asynchrone.MutableClock;
import de.wwsstl.asynchrone.cloud.BatchJob;
import de.wwsstl.asynchrone.cloud.BatchJobId;
import de.wwsstl.asynchrone.cloud.BatchJobStatus;
import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.CloudStatus;
import de.wwsstl.asynchrone.cloud.SubmittedTask;
import de.wwsstl.asynchrone.cloud.TaskStatusResult;
import de.wwsstl.asynchrone.cloud.TestdataItem;
import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.Sandbox;
import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.context.TaskState;
import de.wwsstl.asynchrone.files.NioUserFolderResolver;
import de.wwsstl.asynchrone.pool.TaskId;
import de.wwsstl.asynchrone.registry.InMemoryRunRegistry;
import de.wwsstl.asynchrone.registry.InMemoryTaskRegistry;
import de.wwsstl.asynchrone.registry.RunRegistry;
import de.wwsstl.asynchrone.registry.TaskRegistry;

/**
 * Lebenszyklus der Läufe im {@link TaskManager}: Start in vier Schritten, Abbruch über Cloud-API 3, Abmeldung und
 * Aufbewahrungsfrist ({@code pipeline.task-retention}).
 */
class TaskManagerLifecycleTest {

    private static final Duration RETENTION = Duration.ofHours(1);
    private static final int ERROR_THRESHOLD = 2;

    /**
     * Steuerbare Cloud im Speicher. Aufträge bleiben {@code PENDING}, bis {@link #release} sie erfolgreich oder
     * {@link #fail} sie fehlerhaft enden lässt. Über Sperren lassen sich Cloud-API 1 und 2 anhalten.
     */
    private static final class InMemoryCloud implements CloudClient {

        private final Map<BatchJobId, BatchJob> jobs = new ConcurrentHashMap<>();
        private final Map<String, BatchJobId> runningOverride = new ConcurrentHashMap<>();
        private final AtomicInteger sequence = new AtomicInteger();
        private volatile CloudStatus outcome = CloudStatus.PENDING;
        private volatile boolean statusUpdatesFail;
        private volatile Gate createGate = Gate.opened();
        private volatile Gate statusGate = Gate.opened();

        /** Hält Aufrufe an, bis {@link #open} folgt; {@link #entered} zeigt, dass ein Aufruf wartet. */
        private record Gate(CountDownLatch entered, CountDownLatch released) {

            static Gate opened() {
                return new Gate(new CountDownLatch(1), new CountDownLatch(0));
            }

            static Gate closed() {
                return new Gate(new CountDownLatch(1), new CountDownLatch(1));
            }

            void pass() {
                entered.countDown();
                try {
                    released.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            void awaitEntered() throws InterruptedException {
                assertThat(entered.await(10, TimeUnit.SECONDS)).as("Aufruf erreicht die Sperre").isTrue();
            }

            void open() {
                released.countDown();
            }
        }

        void release() {
            outcome = CloudStatus.SUCCESS;
        }

        void fail() {
            outcome = CloudStatus.ERROR;
        }

        BatchJobStatus status(String jobId) {
            return jobs.get(new BatchJobId(jobId)).status();
        }

        BatchJobId createRunningJob(String userId) {
            BatchJobId id = new BatchJobId("BJ-" + sequence.incrementAndGet());
            jobs.put(id, new BatchJob(id, userId, BatchJobStatus.RUNNING));
            return id;
        }

        @Override
        public CompletableFuture<BatchJobId> createBatchJob(String userId) {
            createGate.pass();
            return CompletableFuture.completedFuture(createRunningJob(userId));
        }

        @Override
        public CompletableFuture<List<SubmittedTask>> submit(BatchJobId job, List<TestdataItem> items) {
            return CompletableFuture.completedFuture(items.stream()
                    .map(item -> new SubmittedTask(item.fileName(), new TaskId(UUID.randomUUID().toString())))
                    .toList());
        }

        @Override
        public CompletableFuture<List<TaskStatusResult>> queryStatus(List<TaskId> taskIds) {
            statusGate.pass();
            CloudStatus status = outcome;
            return CompletableFuture.completedFuture(taskIds.stream()
                    .map(id -> new TaskStatusResult(id, status))
                    .toList());
        }

        @Override
        public CompletableFuture<Optional<BatchJobId>> findRunningBatchJob(String userId) {
            BatchJobId override = runningOverride.get(userId);
            if (override != null) {
                return CompletableFuture.completedFuture(Optional.of(override));
            }
            return CompletableFuture.completedFuture(jobs.values().stream()
                    .filter(job -> job.userId().equals(userId) && job.status() == BatchJobStatus.RUNNING)
                    .map(BatchJob::id)
                    .findFirst());
        }

        @Override
        public CompletableFuture<Optional<BatchJob>> batchJob(BatchJobId job) {
            return CompletableFuture.completedFuture(Optional.ofNullable(jobs.get(job)));
        }

        @Override
        public CompletableFuture<Void> setBatchJobStatus(BatchJobId job, BatchJobStatus status) {
            if (statusUpdatesFail) {
                return CompletableFuture.failedFuture(new IOException("Cloud-API 3 nicht erreichbar"));
            }
            jobs.computeIfPresent(job, (id, existing) -> new BatchJob(id, existing.userId(), status));
            return CompletableFuture.completedFuture(null);
        }
    }

    @TempDir
    Path base;

    private final MutableClock clock = new MutableClock();
    private final TaskRegistry registry = new InMemoryTaskRegistry();
    private final RunRegistry runs = new InMemoryRunRegistry();
    private final InMemoryCloud cloud = new InMemoryCloud();
    private TaskManager taskManager;

    @BeforeEach
    void setUp() {
        // Task-Timeout weit jenseits der Frist, damit das Vorstellen der Uhr keinen laufenden Task beendet.
        PipelineProperties properties = new PipelineProperties(base, 4, 2, Duration.ofMillis(10),
                Duration.ofMillis(10), 200, ERROR_THRESHOLD, Duration.ofDays(30), RETENTION,
                new PipelineProperties.Cloud(URI.create("http://localhost:8081"), "/tasks", "/tasks/status", "/jobs",
                        Duration.ofSeconds(30), Duration.ofSeconds(30), 0, 0));
        taskManager = new TaskManager(registry, runs, new NioUserFolderResolver(properties), cloud, properties,
                clock);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        cloud.createGate.open();
        cloud.statusGate.open();
        taskManager.shutdown();
        for (Sandbox sandbox : registry.all()) {
            sandbox.producerThread().join(Duration.ofSeconds(5));
            sandbox.consumerThread().join(Duration.ofSeconds(5));
        }
    }

    @Test
    void beendeterLaufWirdSamtSandboxAbgemeldetBleibtAberAbfragbar() throws IOException {
        dropFile("alice", "a.txt");
        String taskId = taskManager.start("alice").taskId();
        awaitPending("alice", taskId, 1);
        Sandbox sandbox = registry.find(new BatchJobId(taskId)).orElseThrow();

        cloud.release();
        clock.advance(Duration.ofSeconds(1)); // macht den Eintrag erneut fällig (poll-interval 10 ms)
        awaitDeregistered(taskId);

        // Threads beendet, Sandbox nicht mehr im Register ...
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !sandbox.isAlive());
        assertThat(registry.find(new BatchJobId(taskId))).isEmpty();
        // ... der BatchgenAuftrag abgeschlossen und der Endzustand über die Status-Abfrage erhalten
        assertThat(cloud.status(taskId)).isEqualTo(BatchJobStatus.COMPLETED);
        TaskSnapshot finished = taskManager.status("alice", taskId);
        assertThat(finished.state()).isEqualTo(TaskState.COMPLETED);
        assertThat(finished.run()).isEqualTo(1);
        assertThat(finished.finishedAt()).isNotNull();
        assertThat(finished.submitted()).isEqualTo(1);
        assertThat(finished.succeeded()).isEqualTo(1);
        assertThat(finished.pending()).isZero();
        // Auch die Benutzer:in ist freigegeben; der nächste Start legt einen neuen BatchgenAuftrag an.
        assertThat(taskManager.start("alice").taskId()).isNotEqualTo(taskId);
    }

    @Test
    void endzustandBleibtBisZumAblaufDerFristAbfragbar() {
        String taskId = taskManager.start("bob").taskId();
        awaitDeregistered(taskId);

        clock.advance(RETENTION.minusSeconds(1));
        assertThat(taskManager.status("bob", taskId).state()).isEqualTo(TaskState.COMPLETED);

        clock.advance(Duration.ofSeconds(1));
        assertThatThrownBy(() -> taskManager.status("bob", taskId)).isInstanceOf(TaskNotFoundException.class);
    }

    @Test
    void startEntferntAbgelaufeneEndzustaendeAndererBenutzer() {
        String a = taskManager.start("a").taskId();
        String b = taskManager.start("b").taskId();
        awaitDeregistered(a);
        awaitDeregistered(b);
        clock.advance(RETENTION);

        taskManager.start("c");

        assertThat(runs.latestFinished(new BatchJobId(a))).isEmpty();
        assertThat(runs.latestFinished(new BatchJobId(b))).isEmpty();
    }

    @Test
    void statusMeldetCancellingBisDerConsumerDenAbbruchErkennt() throws Exception {
        dropFile("carol", "a.txt");
        String taskId = taskManager.start("carol").taskId();
        awaitPending("carol", taskId, 1);
        // Den Consumer in der nächsten Abfrage von Cloud-API 2 festhalten: Er kann Cloud-API 3 nicht prüfen.
        InMemoryCloud.Gate gate = InMemoryCloud.Gate.closed();
        cloud.statusGate = gate;
        clock.advance(Duration.ofSeconds(1));
        gate.awaitEntered();

        CancelResult result = taskManager.cancel("carol", taskId);

        assertThat(result).isEqualTo(new CancelResult(taskId, TaskState.CANCELLING));
        assertThat(cloud.status(taskId)).isEqualTo(BatchJobStatus.CANCELLED);
        assertThat(taskManager.status("carol", taskId).state()).isEqualTo(TaskState.CANCELLING);
        assertThat(taskManager.cancel("carol", taskId)).as("idempotent").isEqualTo(result);

        gate.open();
        awaitDeregistered(taskId);
        TaskSnapshot finished = taskManager.status("carol", taskId);
        assertThat(finished.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(finished.cancelReason()).isEqualTo(CancelReason.USER_REQUEST);
        assertThat(finished.abandoned()).isEqualTo(1);
    }

    @Test
    void abbruchPrueftDieBenutzerinUndDenStatusDesBatchgenAuftrags() {
        String taskId = taskManager.start("dave").taskId();
        awaitDeregistered(taskId);

        assertThatThrownBy(() -> taskManager.cancel("mallory", taskId)).isInstanceOf(TaskNotFoundException.class);
        assertThatThrownBy(() -> taskManager.cancel("dave", "BJ-999")).isInstanceOf(TaskNotFoundException.class);
        assertThatThrownBy(() -> taskManager.cancel("dave", taskId))
                .isInstanceOfSatisfying(TaskRejectedException.class,
                        e -> assertThat(e.reason()).isEqualTo(RejectReason.TASK_NOT_RUNNING));
        assertThat(cloud.status(taskId)).isEqualTo(BatchJobStatus.COMPLETED);
    }

    @Test
    void laufendeTasksWerdenNieEntfernt() throws IOException {
        dropFile("erin", "a.txt");
        String taskId = taskManager.start("erin").taskId();
        awaitPending("erin", taskId, 1);

        clock.advance(RETENTION.multipliedBy(10));
        taskManager.start("frank");

        assertThat(taskManager.status("erin", taskId).state()).isEqualTo(TaskState.RUNNING);
        assertThat(registry.find(new BatchJobId(taskId))).isPresent();
    }

    @Test
    void dieBenutzerinWirdVorDemAnlegenDesBatchgenAuftragsBelegt() throws Exception {
        InMemoryCloud.Gate gate = InMemoryCloud.Gate.closed();
        cloud.createGate = gate;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<TaskSnapshot> first = executor.submit(() -> taskManager.start("grace"));
            gate.awaitEntered();

            assertThatThrownBy(() -> taskManager.start("grace"))
                    .isInstanceOfSatisfying(TaskRejectedException.class,
                            e -> assertThat(e.reason()).isEqualTo(RejectReason.USER_TASK_RUNNING));

            gate.open();
            assertThat(first.get().state()).isEqualTo(TaskState.RUNNING);
        }
        assertThat(cloud.jobs.values()).as("genau ein BatchgenAuftrag").hasSize(1);
    }

    @Test
    void pendingboxOhneLaufendenBatchgenAuftragWirdAbgewiesenUndGibtDieBenutzerinFrei() throws IOException {
        Files.createDirectories(base.resolve("heidi").resolve("pendingbox"));
        Files.writeString(base.resolve("heidi").resolve("pendingbox").resolve("alt.txt"), "alt");

        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> taskManager.start("heidi"))
                    .isInstanceOfSatisfying(TaskRejectedException.class,
                            e -> assertThat(e.reason()).isEqualTo(RejectReason.PENDINGBOX_NOT_EMPTY));
        }
        assertThat(cloud.jobs).isEmpty();
    }

    @Test
    void belegteAufgabennummerWirdAbgewiesenUndGibtDieBenutzerinFrei() throws IOException {
        dropFile("ivan", "a.txt");
        String taskId = taskManager.start("ivan").taskId();
        // Die Cloud meldet denselben laufenden BatchgenAuftrag auch für eine andere Benutzer:in.
        cloud.runningOverride.put("judy", new BatchJobId(taskId));

        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> taskManager.start("judy"))
                    .isInstanceOfSatisfying(TaskRejectedException.class,
                            e -> assertThat(e.reason()).isEqualTo(RejectReason.TASK_ALREADY_RUNNING));
        }
        assertThat(taskManager.status("ivan", taskId).state()).isEqualTo(TaskState.RUNNING);
    }

    @Test
    void ohneCloudApi3EndetDerLaufBeiDerFehlerschwelleSofort() throws IOException {
        dropFile("kim", "a.txt");
        dropFile("kim", "b.txt");
        cloud.fail();
        cloud.statusUpdatesFail = true;

        String taskId = taskManager.start("kim").taskId();
        awaitDeregistered(taskId);

        TaskSnapshot finished = taskManager.status("kim", taskId);
        assertThat(finished.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(finished.cancelReason()).isEqualTo(CancelReason.ERROR_THRESHOLD);
        assertThat(finished.failed()).isEqualTo(ERROR_THRESHOLD);
        assertThat(cloud.status(taskId)).as("Markierung gescheitert").isEqualTo(BatchJobStatus.RUNNING);
    }

    @Test
    void fehlerschwelleMarkiertDenBatchgenAuftragUeberCloudApi3() throws IOException {
        dropFile("leo", "a.txt");
        dropFile("leo", "b.txt");
        cloud.fail();

        String taskId = taskManager.start("leo").taskId();
        awaitDeregistered(taskId);

        assertThat(taskManager.status("leo", taskId).cancelReason()).isEqualTo(CancelReason.ERROR_THRESHOLD);
        assertThat(cloud.status(taskId)).isEqualTo(BatchJobStatus.CANCELLED);
    }

    private void dropFile(String user, String fileName) throws IOException {
        Files.createDirectories(base.resolve(user).resolve("inbox"));
        Files.writeString(base.resolve(user).resolve("inbox").resolve(fileName), fileName);
    }

    private void awaitPending(String user, String taskId, int pending) {
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> taskManager.status(user, taskId).pending() == pending);
    }

    /** Wartet, bis der Lauf abgemeldet ist: nicht mehr in der Registry, aber mit Endzustand im Laufregister. */
    private void awaitDeregistered(String taskId) {
        BatchJobId jobId = new BatchJobId(taskId);
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> registry.find(jobId).isEmpty() && runs.latestFinished(jobId).isPresent());
    }
}
