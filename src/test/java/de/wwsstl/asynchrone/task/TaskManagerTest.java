package de.wwsstl.asynchrone.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import de.wwsstl.asynchrone.TestProperties;
import de.wwsstl.asynchrone.cloud.BatchJob;
import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.CloudTimeoutException;
import de.wwsstl.asynchrone.cloud.CloudUnavailableException;
import de.wwsstl.asynchrone.cloud.JobStatus;
import de.wwsstl.asynchrone.cloud.RecordStatus;
import de.wwsstl.asynchrone.cloud.RecordStatusResult;
import de.wwsstl.asynchrone.cloud.SubmitFailedException;
import de.wwsstl.asynchrone.cloud.SubmittedFile;
import de.wwsstl.asynchrone.cloud.TaskId;
import de.wwsstl.asynchrone.cloud.TestdataItem;
import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.files.UserFolderResolver;
import de.wwsstl.asynchrone.files.UserFolders;

/** Lebenszyklus der Sandboxen im TaskManager, ohne HTTP gegen eine steuerbare Cloud. */
class TaskManagerTest {

    @TempDir
    Path base;

    private final StubCloud cloud = new StubCloud();
    private final InMemoryTaskRegistry registry = new InMemoryTaskRegistry();
    private TaskManager manager;
    private UserFolders folders;

    @BeforeEach
    void setUp() {
        UserFolderResolver resolver = new UserFolderResolver(TestProperties.withBase(base));
        manager = new TaskManager(registry, resolver, cloud, TestProperties.withBase(base), Clock.systemUTC());
        folders = resolver.resolve("anna");
    }

    @AfterEach
    void tearDown() {
        manager.shutdown();
    }

    @Test
    void completedTaskWritesEndStateAndIsDeregistered() throws IOException {
        inboxFile("a.json");
        inboxFile("b.json");
        cloud.status = RecordStatus.SUCCESS;

        TaskSnapshot started = manager.start("anna");

        assertThat(started.taskNumber()).isEqualTo("BJ-1");
        assertThat(started.state()).isEqualTo(TaskState.RUNNING);
        awaitDeregistered();
        assertThat(cloud.writtenStates).containsExactly(JobStatus.COMPLETED);
        assertThat(folders.donebox().resolve("a.json")).exists();
        assertThat(folders.donebox().resolve("b.json")).exists();
    }

    @Test
    void filesInPendingboxBlockTheStart() throws IOException {
        Files.writeString(folders.pendingbox().resolve("left.json"), "{}");

        assertThatThrownBy(() -> manager.start("anna"))
                .isInstanceOfSatisfying(TaskRejectedException.class,
                        e -> assertThat(e.reason()).isEqualTo(RejectReason.PENDINGBOX_NOT_EMPTY));
        assertThat(cloud.createdJobs).hasValue(0);
    }

    @Test
    void secondStartIsRejectedWhileRunning() throws IOException {
        inboxFile("a.json");
        manager.start("anna");

        assertThatThrownBy(() -> manager.start("anna"))
                .isInstanceOfSatisfying(TaskRejectedException.class,
                        e -> assertThat(e.reason()).isEqualTo(RejectReason.USER_TASK_RUNNING));
        assertThat(cloud.createdJobs).hasValue(1);
    }

    @Test
    void startWaitsForTheDeregistrationOfAnEndingTask() {
        // Die Aufgabe endet sofort (leere inbox), das Schreiben des Endzustands dauert aber noch.
        cloud.endStateDelay = Duration.ofMillis(500);
        manager.start("anna");
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.find("BJ-1")
                .map(sandbox -> sandbox.context().state() == TaskState.COMPLETED).orElse(false));

        assertThat(manager.start("anna").taskNumber()).isEqualTo("BJ-2");
        assertThat(cloud.writtenStates).contains(JobStatus.COMPLETED);
    }

    @Test
    void invalidJobNumberRejectsStartAndFreesTheUser() {
        cloud.createJob = () -> CompletableFuture.completedFuture("keine gültige Nummer");

        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> manager.start("anna")).isInstanceOf(CloudTimeoutException.class)
                    .hasMessage("Cloud-API 1 (BatchgenAuftrag anlegen): Ergebnis unklar, ungültige Nummer des "
                            + "BatchgenAuftrags: 'keine gültige Nummer'");
        }
        assertThat(registry.size()).isZero();
    }

    @Test
    void unreachableCloudRejectsStart() {
        cloud.createJob = () -> CompletableFuture.failedFuture(new SubmitFailedException(
                SubmitFailedException.Kind.NOT_PROCESSED, new IOException("Verbindung abgelehnt")));

        assertThatThrownBy(() -> manager.start("anna")).isInstanceOf(CloudUnavailableException.class);
        assertThat(registry.size()).isZero();
    }

    @Test
    void createJobWithoutResponseRejectsStartWithTimeoutAndDoesNothingElse() throws IOException {
        inboxFile("a.json");
        cloud.createJob = () -> CompletableFuture.failedFuture(new SubmitFailedException(
                SubmitFailedException.Kind.UNKNOWN, new TimeoutException("Did not observe any item within 30000ms")));

        for (int attempt = 0; attempt < 2; attempt++) {
            assertThatThrownBy(() -> manager.start("anna")).isInstanceOf(CloudTimeoutException.class)
                    .hasMessage("Cloud-API 1 (BatchgenAuftrag anlegen): Ergebnis unklar, "
                            + "keine Antwort innerhalb von submit-timeout");
        }
        assertThat(registry.size()).isZero();
        assertThat(folders.inbox().resolve("a.json")).exists();
        assertThat(cloud.writtenStates).isEmpty();
    }

    @Test
    void cancelWritesTerminatedAfterThreadsEnded() throws IOException {
        inboxFile("a.json");
        manager.start("anna");
        awaitPending(1);

        TaskSnapshot cancelled = manager.cancel("anna", "BJ-1");

        assertThat(cancelled.state()).isEqualTo(TaskState.TERMINATED);
        assertThat(cancelled.reason()).isEqualTo("Abbruch über die REST-API");
        awaitDeregistered();
        assertThat(cloud.writtenStates).containsExactly(JobStatus.TERMINATED);
        assertThat(folders.pendingbox().resolve("a.json")).exists();
        assertThat(folders.markerOf(folders.pendingbox().resolve("a.json"))).hasContent("t-a.json");
        assertThatThrownBy(() -> manager.cancel("anna", "BJ-1")).isInstanceOf(TaskNotFoundException.class);
    }

    @Test
    void cancelOfForeignTaskIsNotFound() throws IOException {
        inboxFile("a.json");
        manager.start("anna");

        assertThatThrownBy(() -> manager.cancel("bert", "BJ-1")).isInstanceOf(TaskNotFoundException.class);
        assertThat(registry.find("BJ-1")).get().extracting(sandbox -> sandbox.context().state())
                .isEqualTo(TaskState.RUNNING);
    }

    @Test
    void unexpectedFailureAbortsWithoutWritingEndState() throws IOException {
        inboxFile("a.json");
        cloud.submit = items -> {
            throw new IllegalStateException("unerwarteter Fehler");
        };

        manager.start("anna");

        awaitDeregistered();
        assertThat(cloud.writtenStates).isEmpty();
        // Wie nach einem Absturz: die Datei liegt ohne Marker in der pendingbox und blockiert den nächsten Start.
        assertThat(folders.pendingbox().resolve("a.json")).exists();
        assertThat(folders.markerOf(folders.pendingbox().resolve("a.json"))).doesNotExist();
        assertThatThrownBy(() -> manager.start("anna")).isInstanceOf(TaskRejectedException.class);
    }

    @Test
    void shutdownStopsRunningTasksWithoutWritingEndState() throws IOException {
        inboxFile("a.json");
        manager.start("anna");
        awaitPending(1);
        Sandbox sandbox = registry.find("BJ-1").orElseThrow();

        manager.shutdown();

        assertThat(sandbox.context().state()).isEqualTo(TaskState.ABORTED);
        assertThat(sandbox.isAlive()).isFalse();
        assertThat(registry.size()).isZero();
        assertThat(cloud.writtenStates).isEmpty();
    }

    @Test
    void statusFallsBackToLocalStateWhileCloudIsUnavailable() throws IOException {
        inboxFile("a.json");
        manager.start("anna");
        awaitPending(1);
        cloud.jobAvailable = false;

        TaskStatus status = manager.status("anna", "BJ-1");

        assertThat(status.cloud()).isNull();
        assertThat(status.local().pending()).isEqualTo(1);

        manager.cancel("anna", "BJ-1");
        awaitDeregistered();
        assertThatThrownBy(() -> manager.status("anna", "BJ-1")).isInstanceOf(CloudUnavailableException.class);
    }

    @Test
    void submitWithoutResponseEndsTaskWithTimeoutAndNamesTheCall() throws IOException {
        inboxFile("a.json");
        cloud.submit = items -> CompletableFuture.failedFuture(new SubmitFailedException(
                SubmitFailedException.Kind.UNKNOWN, new TimeoutException("Did not observe any item within 30000ms")));

        TaskSnapshot ended = awaitEndedWhileRegistered(TaskState.TIMEOUT);

        assertThat(ended.reason()).isEqualTo(
                "Cloud-API 1 (Batch übermitteln): Ergebnis unklar, keine Antwort innerhalb von submit-timeout");
        assertThat(ended.unclear()).isEqualTo(1);
        awaitDeregistered();
        assertThat(cloud.writtenStates).containsExactly(JobStatus.TIMEOUT);
    }

    @Test
    void unclearSubmitResultEndsTaskWithTimeoutAndNamesTheCall() throws IOException {
        inboxFile("a.json");
        cloud.submit = items -> CompletableFuture.failedFuture(new SubmitFailedException(
                SubmitFailedException.Kind.UNKNOWN,
                new IllegalStateException("500 Internal Server Error from POST http://cloud/tasks")));

        TaskSnapshot ended = awaitEndedWhileRegistered(TaskState.TIMEOUT);

        assertThat(ended.reason()).isEqualTo("Cloud-API 1 (Batch übermitteln): Ergebnis unklar, "
                + "500 Internal Server Error from POST http://cloud/tasks");
    }

    @Test
    void timeoutNamesTheFailingStatusQuery() throws IOException {
        withTaskTimeout(Duration.ofMillis(300));
        inboxFile("a.json");
        cloud.failingQueries.set(-1);

        TaskSnapshot ended = awaitEndedWhileRegistered(TaskState.TIMEOUT);

        assertThat(ended.reason()).isEqualTo("maximale Laufzeit (task-timeout) überschritten; letzte Statusabfrage "
                + "(Cloud-API 2) fehlgeschlagen: 503 Service Unavailable from POST http://cloud/tasks/status");
    }

    @Test
    void timeoutDoesNotNameAStatusQueryThatRecovered() throws IOException {
        withTaskTimeout(Duration.ofMillis(300));
        inboxFile("a.json");
        cloud.failingQueries.set(1);

        TaskSnapshot ended = awaitEndedWhileRegistered(TaskState.TIMEOUT);

        assertThat(ended.reason()).isEqualTo("maximale Laufzeit (task-timeout) überschritten");
    }

    @Test
    void statusOfUnknownTaskIsNotFound() {
        assertThatThrownBy(() -> manager.status("anna", "BJ-404")).isInstanceOf(TaskNotFoundException.class);
        assertThatThrownBy(() -> manager.status("anna", "BJ 404")).isInstanceOf(InvalidTaskNumberException.class);
    }

    private void awaitPending(int pending) {
        await().atMost(Duration.ofSeconds(5)).until(() -> registry.find("BJ-1")
                .map(sandbox -> sandbox.snapshot().pending() == pending).orElse(false));
    }

    /** Ersetzt den TaskManager durch einen mit kurzer maximaler Laufzeit. */
    private void withTaskTimeout(Duration taskTimeout) {
        PipelineProperties defaults = TestProperties.withBase(base);
        PipelineProperties properties = new PipelineProperties(defaults.baseDirectory(), defaults.batchSize(),
                defaults.poolResumeThreshold(), defaults.sweepInterval(), defaults.pollInterval(),
                defaults.statusBulkSize(), defaults.errorThreshold(), taskTimeout, defaults.submitRetryMaxWait(),
                defaults.cloud());
        manager.shutdown();
        manager = new TaskManager(registry, new UserFolderResolver(properties), cloud, properties, Clock.systemUTC());
    }

    /**
     * Startet eine Aufgabe und liefert ihren lokalen Stand mit Endzustand. Das Schreiben des Endzustands wird
     * verzögert, damit die Sandbox lange genug in der TaskRegistry steht.
     */
    private TaskSnapshot awaitEndedWhileRegistered(TaskState expected) {
        cloud.endStateDelay = Duration.ofMillis(500);
        manager.start("anna");
        TaskSnapshot ended = await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(10))
                .until(() -> registry.find("BJ-1").map(Sandbox::snapshot).orElse(null),
                        local -> local != null && local.state() != TaskState.RUNNING);
        assertThat(ended.state()).isEqualTo(expected);
        return ended;
    }

    private void awaitDeregistered() {
        await().atMost(Duration.ofSeconds(10)).until(() -> registry.size() == 0);
    }

    private void inboxFile(String name) throws IOException {
        Files.writeString(folders.inbox().resolve(name), "{}");
    }

    /** Steuerbare Cloud: vergibt TaskIds {@code t-<Dateiname>} und meldet für alle Datensätze {@link #status}. */
    private static final class StubCloud implements CloudClient {

        final AtomicInteger createdJobs = new AtomicInteger();
        final List<JobStatus> writtenStates = new CopyOnWriteArrayList<>();

        volatile Supplier<CompletableFuture<String>> createJob =
                () -> CompletableFuture.completedFuture("BJ-" + createdJobs.incrementAndGet());
        volatile Function<List<TestdataItem>, CompletableFuture<List<SubmittedFile>>> submit =
                items -> CompletableFuture.completedFuture(items.stream()
                        .map(item -> new SubmittedFile(item.fileName(), new TaskId("t-" + item.fileName())))
                        .toList());
        volatile RecordStatus status = RecordStatus.PENDING;
        /** So viele Aufrufe von Cloud-API 2 schlagen noch mit 503 fehl; negativ: alle. */
        final AtomicInteger failingQueries = new AtomicInteger();
        volatile Duration endStateDelay = Duration.ZERO;
        volatile boolean jobAvailable = true;

        @Override
        public CompletableFuture<String> createBatchJob(String userId) {
            return createJob.get();
        }

        @Override
        public CompletableFuture<List<SubmittedFile>> submit(String jobId, List<TestdataItem> items) {
            return submit.apply(items);
        }

        @Override
        public CompletableFuture<List<RecordStatusResult>> queryStatus(List<TaskId> taskIds) {
            if (failingQueries.getAndUpdate(left -> left > 0 ? left - 1 : left) != 0) {
                return CompletableFuture.failedFuture(
                        new IOException("503 Service Unavailable from POST http://cloud/tasks/status"));
            }
            return CompletableFuture.completedFuture(
                    taskIds.stream().map(taskId -> new RecordStatusResult(taskId, status)).toList());
        }

        @Override
        public CompletableFuture<Void> setJobStatus(String jobId, JobStatus jobStatus) {
            return CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(endStateDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                writtenStates.add(jobStatus);
            });
        }

        @Override
        public CompletableFuture<Optional<BatchJob>> batchJob(String jobId) {
            if (!jobAvailable) {
                return CompletableFuture.failedFuture(new IOException("Cloud nicht erreichbar"));
            }
            return CompletableFuture.completedFuture(Optional.empty());
        }
    }
}
