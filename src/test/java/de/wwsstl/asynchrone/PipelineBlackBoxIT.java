package de.wwsstl.asynchrone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import de.wwsstl.asynchrone.cloud.JobStatus;
import de.wwsstl.asynchrone.fakecloud.FakeCloudBackendApplication;
import de.wwsstl.asynchrone.task.TaskSnapshot;
import de.wwsstl.asynchrone.task.TaskState;
import de.wwsstl.asynchrone.task.TaskStatus;

/**
 * Black-Box-Integrationstest: Die ausgelieferte Anwendung (Spring-Boot-JAR) und das {@link FakeCloudBackendApplication}
 * laufen als eigene Prozesse. Der Test spricht beide nur über HTTP an und prüft die Benutzerordner. Weil das
 * Fake-Backend keine Statuslogik hat, übernimmt der Test die Rolle der Cloud und entscheidet jeden Datensatz über den
 * Admin-Endpunkt.
 *
 * <p>Läuft mit {@code mvn verify} (Failsafe), nachdem das JAR gebaut ist; die Ausgaben beider Prozesse stehen in
 * {@code target/it-logs}.
 */
class PipelineBlackBoxIT {

    private static final Path base = Path.of("target", "it-blackbox", UUID.randomUUID().toString()).toAbsolutePath();

    private static ExternalProcess fakeBackend;
    private static ExternalProcess app;
    private static FakeBackendClient cloud;
    private static PipelineApi api;

    @BeforeAll
    static void startProcesses() throws Exception {
        String appJar = System.getProperty("pipeline.it.app-jar");
        assertThat(appJar).as("System-Property pipeline.it.app-jar (gesetzt von mvn verify)").isNotBlank();
        assertThat(Path.of(appJar)).as("JAR der Anwendung").isRegularFile();
        Files.createDirectories(base);

        int fakePort = ExternalProcess.freePort();
        String fakeUrl = "http://localhost:" + fakePort;
        fakeBackend = ExternalProcess.start("fake-backend", List.of(ExternalProcess.java(),
                "@" + ExternalProcess.classpathArgFile(), FakeCloudBackendApplication.class.getName(),
                String.valueOf(fakePort)));
        fakeBackend.awaitHttp(fakeUrl + "/tasks", Duration.ofSeconds(30));

        int appPort = ExternalProcess.freePort();
        List<String> command = new ArrayList<>(List.of(ExternalProcess.java(), "-jar", appJar));
        command.addAll(List.of(
                "--server.port=" + appPort,
                "--pipeline.base-directory=" + base,
                "--pipeline.cloud.base-url=" + fakeUrl,
                "--pipeline.batch-size=4",
                "--pipeline.pool-resume-threshold=2",
                "--pipeline.sweep-interval=50ms",
                "--pipeline.poll-interval=200ms",
                "--pipeline.error-threshold=3",
                "--pipeline.cloud.status-timeout=2s"));
        app = ExternalProcess.start("pipeline-app", command);
        // Eine unbekannte Aufgabe beantwortet die Anwendung mit 404, sobald sie läuft.
        app.awaitHttp("http://localhost:" + appPort + "/api/users/probe/tasks/BJ-0", Duration.ofSeconds(60));

        cloud = new FakeBackendClient(fakeUrl);
        api = new PipelineApi(appPort, base);
    }

    @AfterAll
    static void stopProcesses() throws InterruptedException {
        if (app != null) {
            app.close();
        }
        if (fakeBackend != null) {
            fakeBackend.close();
        }
    }

    @Test
    void processesFilesAndWritesCompleted() {
        api.drop("bbdone", "bbdone-a.json", "{}");
        api.drop("bbdone", "bbdone-b.json", "{}");
        api.drop("bbdone", "bbdone-c.json", "{}");
        String taskNumber = api.start("bbdone").taskNumber();
        Map<String, String> taskIds = awaitSubmitted(taskNumber, 3);

        // Solange die Cloud nicht entschieden hat, wartet alles auf PENDING.
        eventually().untilAsserted(() -> assertThat(api.status("bbdone", taskNumber).local().pending()).isEqualTo(3));
        TaskStatus running = api.status("bbdone", taskNumber);
        assertThat(running.cloud()).isEqualTo(new TaskStatus.Cloud(JobStatus.RUNNING, 0, 0, 3));
        TaskSnapshot local = running.local();
        assertThat(local.state()).isEqualTo(TaskState.RUNNING);
        assertThat(local.submitted()).isEqualTo(3);

        cloud.decide(taskIds.get("bbdone-a.json"), "SUCCESS");
        cloud.decide(taskIds.get("bbdone-b.json"), "SUCCESS");
        cloud.decide(taskIds.get("bbdone-c.json"), "ERROR");

        assertThat(awaitEnd("bbdone", taskNumber, JobStatus.COMPLETED).cloud())
                .isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 2, 1, 0));
        assertThat(api.names("bbdone", "donebox")).containsExactly("bbdone-a.json", "bbdone-b.json");
        assertThat(api.names("bbdone", "errorbox")).containsExactly("bbdone-c.json");
        assertThat(api.names("bbdone", "pendingbox")).isEmpty();
        assertThat(api.markers("bbdone")).isEmpty();
    }

    @Test
    void fileThatWasNotConvertedGoesToErrorbox() {
        api.dropFiles("bbreject", 2, "{}");
        cloud.rejectNext("bbreject-1.json");

        String taskNumber = api.start("bbreject").taskNumber();
        cloud.decide(awaitSubmitted(taskNumber, 1).get("bbreject-0.json"), "SUCCESS");

        assertThat(awaitEnd("bbreject", taskNumber, JobStatus.COMPLETED).cloud())
                .isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 1, 0, 0));
        assertThat(api.names("bbreject", "donebox")).containsExactly("bbreject-0.json");
        assertThat(api.names("bbreject", "errorbox")).containsExactly("bbreject-1.json");
    }

    @Test
    void batchThatWasNotProcessedIsSubmittedAgain() {
        api.dropFiles("bbretry", 2, "{}");
        cloud.nextResponse(503);

        String taskNumber = api.start("bbretry").taskNumber();
        // Erst der zweite Versuch kommt an; jede Datei gibt es genau einmal.
        awaitSubmitted(taskNumber, 2).values().forEach(taskId -> cloud.decide(taskId, "SUCCESS"));

        assertThat(awaitEnd("bbretry", taskNumber, JobStatus.COMPLETED).cloud())
                .isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 2, 0, 0));
        assertThat(api.names("bbretry", "donebox")).containsExactly("bbretry-0.json", "bbretry-1.json");
    }

    @Test
    void batchWithUnclearResultStaysInPendingboxWithoutMarkerAndEndsTaskWithTimeout() {
        api.dropFiles("bbunclear", 2, "{}");
        cloud.nextResponse(500);

        String taskNumber = api.start("bbunclear").taskNumber();

        assertThat(awaitEnd("bbunclear", taskNumber, JobStatus.TIMEOUT).cloud())
                .isEqualTo(new TaskStatus.Cloud(JobStatus.TIMEOUT, 0, 0, 0));
        assertThat(api.names("bbunclear", "pendingbox")).containsExactly("bbunclear-0.json", "bbunclear-1.json");
        assertThat(api.markers("bbunclear")).isEmpty();
        assertRejected(api.startResponse("bbunclear"), 409, "PENDINGBOX_NOT_EMPTY");
    }

    @Test
    void producerWaitsUntilStatusPoolShrinks() {
        api.dropFiles("bbpress", 6, "{}");
        String taskNumber = api.start("bbpress").taskNumber();

        // Erster Batch: 4 Dateien im Status-Pool, mehr als pool-resume-threshold (2); der Rest bleibt in der inbox.
        Map<String, String> firstBatch = awaitSubmitted(taskNumber, 4);
        assertThat(api.names("bbpress", "inbox")).hasSize(2);
        assertThat(api.names("bbpress", "pendingbox")).hasSize(4);

        firstBatch.values().stream().limit(2).forEach(taskId -> cloud.decide(taskId, "SUCCESS"));
        Map<String, String> all = awaitSubmitted(taskNumber, 6);
        assertThat(api.names("bbpress", "inbox")).isEmpty();

        all.values().forEach(taskId -> cloud.decide(taskId, "SUCCESS"));
        assertThat(awaitEnd("bbpress", taskNumber, JobStatus.COMPLETED).cloud())
                .isEqualTo(new TaskStatus.Cloud(JobStatus.COMPLETED, 6, 0, 0));
        assertThat(api.names("bbpress", "donebox")).hasSize(6);
    }

    @Test
    void errorThresholdEndsTaskAndLeavesOpenFilesInPendingbox() {
        api.dropFiles("bberrors", 4, "{}");
        String taskNumber = api.start("bberrors").taskNumber();
        Map<String, String> taskIds = awaitSubmitted(taskNumber, 4);

        // Schwelle 3: drei Datensätze scheitern, der vierte bleibt offen.
        List.of("bberrors-0.json", "bberrors-1.json", "bberrors-2.json")
                .forEach(file -> cloud.decide(taskIds.get(file), "ERROR"));

        assertThat(awaitEnd("bberrors", taskNumber, JobStatus.ERROR).cloud())
                .isEqualTo(new TaskStatus.Cloud(JobStatus.ERROR, 0, 3, 1));
        assertThat(api.names("bberrors", "errorbox")).hasSize(3);
        assertThat(api.names("bberrors", "pendingbox")).containsExactly("bberrors-3.json");
        assertThat(api.markers("bberrors")).containsExactly("bberrors-3.json");
        assertRejected(api.startResponse("bberrors"), 409, "PENDINGBOX_NOT_EMPTY");
    }

    @Test
    void cancelWritesTerminatedAndLeavesFiles() {
        api.dropFiles("bbcancel", 2, "{}");
        String taskNumber = api.start("bbcancel").taskNumber();
        awaitSubmitted(taskNumber, 2);
        eventually().untilAsserted(() -> assertThat(api.status("bbcancel", taskNumber).local().pending()).isEqualTo(2));

        PipelineApi.Response cancelled = api.cancel("bbcancel", taskNumber);

        assertThat(cancelled.status()).isEqualTo(202);
        assertThat(cancelled.body().get("state")).isEqualTo("TERMINATED");
        awaitEnd("bbcancel", taskNumber, JobStatus.TERMINATED);
        assertThat(cloud.job(taskNumber).path("status").asString()).isEqualTo("TERMINATED");
        assertThat(api.names("bbcancel", "pendingbox")).containsExactly("bbcancel-0.json", "bbcancel-1.json");
        assertThat(api.markers("bbcancel")).containsExactly("bbcancel-0.json", "bbcancel-1.json");
        assertRejected(api.startResponse("bbcancel"), 409, "PENDINGBOX_NOT_EMPTY");
    }

    @Test
    void onlyOneTaskPerUserAndOnlyForItsOwner() {
        api.dropFiles("bbowner", 1, "{}");
        String taskNumber = api.start("bbowner").taskNumber();

        assertRejected(api.startResponse("bbowner"), 409, "USER_TASK_RUNNING");
        assertRejected(api.statusResponse("bbintruder", taskNumber), 404, "TASK_NOT_FOUND");
        assertRejected(api.cancel("bbintruder", taskNumber), 404, "TASK_NOT_FOUND");

        cloud.decide(awaitSubmitted(taskNumber, 1).get("bbowner-0.json"), "SUCCESS");
        awaitEnd("bbowner", taskNumber, JobStatus.COMPLETED);

        // Nach der Abmeldung ist die Benutzer:in wieder frei; eine leere inbox endet sofort mit COMPLETED.
        String next = api.start("bbowner").taskNumber();
        assertThat(next).isNotEqualTo(taskNumber);
        awaitEnd("bbowner", next, JobStatus.COMPLETED);
    }

    // --- Hilfsmethoden ---------------------------------------------------------------------------------------

    private static ConditionFactory eventually() {
        return await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100));
    }

    /** Wartet, bis die Cloud so viele Datensätze des BatchgenAuftrags kennt; liefert TaskId je Dateiname. */
    private static Map<String, String> awaitSubmitted(String taskNumber, int count) {
        return eventually().until(() -> cloud.taskIds(taskNumber), taskIds -> taskIds.size() == count);
    }

    /** Wartet, bis die Cloud den Endzustand kennt und die Anwendung die Sandbox abgemeldet hat. */
    private static TaskStatus awaitEnd(String user, String taskNumber, JobStatus expected) {
        AtomicReference<TaskStatus> last = new AtomicReference<>();
        eventually().untilAsserted(() -> {
            TaskStatus status = api.status(user, taskNumber);
            last.set(status);
            assertThat(status.local()).as("lokaler Stand nach der Abmeldung").isNull();
            assertThat(status.cloud().status()).isEqualTo(expected);
        });
        return last.get();
    }

    private static void assertRejected(PipelineApi.Response response, int status, String code) {
        assertThat(response.status()).isEqualTo(status);
        assertThat(response.code()).isEqualTo(code);
    }
}
