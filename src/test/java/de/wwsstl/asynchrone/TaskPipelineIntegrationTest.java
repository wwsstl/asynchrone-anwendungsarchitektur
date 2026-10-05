package de.wwsstl.asynchrone;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.FileSystemUtils;

import de.wwsstl.asynchrone.cloud.BatchJobId;
import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.Sandbox;
import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.context.TaskState;
import de.wwsstl.asynchrone.registry.RunRegistry;
import de.wwsstl.asynchrone.registry.TaskRegistry;
import de.wwsstl.asynchrone.taskmanager.TaskManager;

/**
 * Ende-zu-Ende-Test über die REST-API gegen einen lokalen Fake der Cloud-Dienste (echter HTTP-Server, sodass der
 * {@code WebClient} tatsächlich benutzt wird). Alle Tests teilen sich einen Spring-Kontext — und damit dieselben
 * {@code TaskManager}-, {@code TaskRegistry}- und {@code RunRegistry}-Singletons. Beendete Läufe werden nebenläufig
 * abgemeldet, sobald ihre Threads auslaufen; die Tests prüfen daher einzelne Aufgabennummern statt der
 * Registergröße. Jeder Test verwendet eigene Benutzer:innen.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TaskPipelineIntegrationTest {

    private static final FakeCloud cloud = new FakeCloud();
    private static final Path base = createBase();

    private static Path createBase() {
        try {
            return Files.createTempDirectory("pipeline-it");
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
        registry.add("pipeline.batch-size", () -> "10");
        registry.add("pipeline.error-threshold", () -> "3");
        registry.add("pipeline.cloud.status-retries", () -> "0");
    }

    @AfterAll
    static void cleanUp() throws IOException {
        cloud.close();
        FileSystemUtils.deleteRecursively(base);
    }

    @Value("${local.server.port}")
    int port;

    @Autowired
    ApplicationContext context;
    @Autowired
    TaskManager taskManager;
    @Autowired
    TaskRegistry registry;
    @Autowired
    RunRegistry runs;

    PipelineApi api;

    @BeforeEach
    void setUp() {
        api = new PipelineApi(port, base);
    }

    @Test
    void taskManagerUndRegisterSindSingletonsUndBleibenDieGanzeLaufzeitBestehen() {
        assertThat(context.getBean(TaskManager.class)).isSameAs(taskManager);
        assertThat(context.getBean(TaskRegistry.class)).isSameAs(registry);
        assertThat(context.getBean(RunRegistry.class)).isSameAs(runs);
        assertThat(context.getBeansOfType(TaskManager.class)).hasSize(1);
        assertThat(context.getBeansOfType(TaskRegistry.class)).hasSize(1);
        assertThat(context.getBeansOfType(RunRegistry.class)).hasSize(1);

        api.dropFiles("singleton-user", 1, "ok");
        api.awaitFinished("singleton-user", api.start("singleton-user").taskId());

        // Auch nach vielen Requests und beendeten Tasks sind es dieselben Instanzen.
        assertThat(context.getBean(TaskManager.class)).isSameAs(taskManager);
        assertThat(context.getBean(TaskRegistry.class)).isSameAs(registry);
        assertThat(context.getBean(RunRegistry.class)).isSameAs(runs);
    }

    @Test
    void startLegtEinenBatchgenAuftragAnUndVerwendetDessenNummerAlsAufgabennummer() {
        PipelineApi.Response response = api.startResponse("numbered");

        assertThat(response.status()).isEqualTo(202);
        assertThat(response.body()).containsEntry("state", "RUNNING").containsEntry("run", 1);
        assertThat(cloud.jobsOf("numbered")).containsExactly((String) response.body().get("taskId"));
        assertThat(api.awaitFinished("numbered", (String) response.body().get("taskId")).state())
                .isEqualTo(TaskState.COMPLETED);
    }

    @Test
    void nBenutzerErgebenNTasksImRegisterJeMitEigenerSandbox() {
        int n = 6;
        List<String> users = new ArrayList<>();
        List<String> files = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String user = "many" + i;
            users.add(user);
            // SLOW: Die Tasks laufen, bis ihre Aufträge freigegeben werden.
            api.dropFiles(user, 3, "SLOW");
            for (int f = 0; f < 3; f++) {
                files.add(user + "-" + f + ".txt");
            }
        }

        List<TaskSnapshot> started = users.stream().map(api::start).toList();

        // n externe Benutzer -> n laufende Tasks im Register, jeder mit seiner eigenen, vollständigen Sandbox
        assertThat(started).extracting(TaskSnapshot::taskId).doesNotHaveDuplicates();
        List<Sandbox> sandboxes = started.stream().map(s -> registry.find(jobId(s)).orElseThrow()).toList();
        assertThat(registry.all()).containsAll(sandboxes);
        assertThat(distinct(sandboxes.stream().map(Sandbox::context).toList())).hasSize(n);
        assertThat(distinct(sandboxes.stream().map(Sandbox::pool).toList())).hasSize(n);
        assertThat(distinct(sandboxes.stream().map(Sandbox::producerThread).toList())).hasSize(n);
        assertThat(distinct(sandboxes.stream().map(Sandbox::consumerThread).toList())).hasSize(n);
        assertThat(sandboxes).allSatisfy(s -> {
            assertThat(s.producerThread().isVirtual()).isTrue();
            assertThat(s.consumerThread().isVirtual()).isTrue();
        });

        cloud.release(files);
        for (int i = 0; i < n; i++) {
            TaskSnapshot done = api.awaitFinished(users.get(i), started.get(i).taskId());
            assertThat(done.state()).isEqualTo(TaskState.COMPLETED);
            assertThat(done.submitted()).isEqualTo(3);
            assertThat(done.succeeded()).isEqualTo(3);
            assertThat(api.names(users.get(i), "donebox")).hasSize(3);
            assertThat(api.names(users.get(i), "inbox")).isEmpty();
        }

        // Beendete Tasks werden samt Threads, Status-Pool und Sandbox abgemeldet ...
        sandboxes.forEach(s -> Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> registry.find(s.context().jobId()).isEmpty() && !s.isAlive()));
        assertThat(registry.all()).doesNotContainAnyElementsOf(sandboxes);
        // ... der BatchgenAuftrag ist abgeschlossen, und der Endzustand bleibt über die REST-API abfragbar.
        for (int i = 0; i < n; i++) {
            assertThat(cloud.jobStatus(started.get(i).taskId())).isEqualTo("COMPLETED");
            TaskSnapshot finished = api.status(users.get(i), started.get(i).taskId());
            assertThat(finished.state()).isEqualTo(TaskState.COMPLETED);
            assertThat(finished.succeeded()).isEqualTo(3);
            assertThat(finished.finishedAt()).isNotNull();
        }
    }

    @Test
    void jederConsumerFragtNurTaskIdsDerEigenenSandboxAb() {
        List<String> users = List.of("iso-a", "iso-b", "iso-c");
        users.forEach(u -> api.dropFiles(u, 12, "ok"));

        List<TaskSnapshot> started = users.stream().map(api::start).toList();
        for (int i = 0; i < users.size(); i++) {
            assertThat(api.awaitFinished(users.get(i), started.get(i).taskId()).succeeded()).isEqualTo(12);
        }

        List<FakeCloud.StatusCall> calls = cloud.statusCalls().stream()
                .filter(call -> call.fileNames().get(0).startsWith("iso-")).toList();
        assertThat(calls).isNotEmpty();
        assertThat(calls).allSatisfy(call -> assertThat(call.fileNames().stream()
                .map(name -> name.substring(0, name.lastIndexOf('-'))).distinct()).hasSize(1));
    }

    @Test
    void statusAbfragenwerdenAlsBulkAufrufGebuendeltUndBatchesRespektierenDieBatchgroesse() {
        api.dropFiles("bulk", 25, "ok");

        TaskSnapshot done = api.awaitFinished("bulk", api.start("bulk").taskId());

        assertThat(done.state()).isEqualTo(TaskState.COMPLETED);
        assertThat(done.succeeded()).isEqualTo(25);
        assertThat(cloud.statusCalls().stream().filter(c -> c.fileNames().get(0).startsWith("bulk-"))
                .mapToInt(c -> c.fileNames().size()).max().orElse(0)).isGreaterThan(1);
        assertThat(cloud.submitBatchSizes()).allSatisfy(size -> assertThat(size).isLessThanOrEqualTo(10));
    }

    @Test
    void fehlerhafteDateienLandenInDerErrorbox() {
        api.drop("mixed", "gut-1.txt", "ok");
        api.drop("mixed", "gut-2.txt", "ok");
        api.drop("mixed", "schlecht.txt", "FAIL");

        TaskSnapshot done = api.awaitFinished("mixed", api.start("mixed").taskId());

        assertThat(done.state()).isEqualTo(TaskState.COMPLETED);
        assertThat(done.succeeded()).isEqualTo(2);
        assertThat(done.failed()).isEqualTo(1);
        assertThat(api.names("mixed", "donebox")).containsExactly("gut-1.txt", "gut-2.txt");
        assertThat(api.names("mixed", "errorbox")).containsExactly("schlecht.txt");
        assertThat(api.names("mixed", "inbox")).isEmpty();
    }

    @Test
    void leereInboxSchliesstDenTaskSofortAb() {
        TaskSnapshot task = api.start("empty");
        TaskSnapshot done = api.awaitFinished("empty", task.taskId());

        assertThat(done.state()).isEqualTo(TaskState.COMPLETED);
        assertThat(done.submitted()).isZero();
        awaitJobStatus(task.taskId(), "COMPLETED");
    }

    @Test
    void abbruchVonAussenLaeuftUeberCloudApi3UndBeendetNurDenEigenenTask() {
        api.dropFiles("cancel-me", 4, "SLOW");
        api.dropFiles("cancel-neighbor", 3, "ok");

        TaskSnapshot mine = api.start("cancel-me");
        TaskSnapshot neighbor = api.start("cancel-neighbor");
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> api.status("cancel-me", mine.taskId())
                .pending() == 4);
        Sandbox sandbox = registry.find(jobId(mine)).orElseThrow();

        PipelineApi.Response cancelled = api.cancelResponse("cancel-me", mine.taskId());

        // Die REST-API markiert nur den BatchgenAuftrag und antwortet sofort mit CANCELLING.
        assertThat(cancelled.status()).isEqualTo(202);
        assertThat(cancelled.body()).containsEntry("taskId", mine.taskId()).containsEntry("state", "CANCELLING");
        assertThat(cloud.jobStatus(mine.taskId())).isEqualTo("CANCELLED");
        assertThat(api.status("cancel-me", mine.taskId()).state()).isIn(TaskState.CANCELLING, TaskState.CANCELLED);

        // Der Consumer erkennt den Abbruch; der Lauf endet mit CANCELLED und bleibt abfragbar.
        TaskSnapshot done = api.awaitFinished("cancel-me", mine.taskId());
        assertThat(done.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(done.cancelReason()).isEqualTo(CancelReason.USER_REQUEST);
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> registry.find(jobId(mine)).isEmpty()
                && !sandbox.isAlive());
        TaskSnapshot archived = api.status("cancel-me", mine.taskId());
        assertThat(archived.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(archived.pending()).isZero();
        assertThat(archived.abandoned()).isEqualTo(4);
        assertThat(runs.finished(jobId(mine), 1)).contains(archived);
        // übermittelte Dateien liegen nicht mehr in der inbox, sondern samt TaskId in der pendingbox
        assertThat(api.names("cancel-me", "inbox")).isEmpty();
        assertThat(api.names("cancel-me", "pendingbox")).hasSize(4);
        // Ein zweiter Abbruch ändert nichts.
        assertThat(api.cancelStatus("cancel-me", mine.taskId())).as("zweiter Abbruch").isEqualTo(202);

        // der Nachbar-Task ist davon unberührt: Er läuft zu Ende und bleibt mit seinem Endzustand abfragbar.
        assertThat(api.awaitFinished("cancel-neighbor", neighbor.taskId()).state()).isEqualTo(TaskState.COMPLETED);
        assertThat(api.statusCode("cancel-neighbor", neighbor.taskId())).isEqualTo(200);
    }

    @Test
    void wegA_fortsetzenNimmtDieUebermitteltenDateienUnterDerselbenAufgabennummerWiederAuf() {
        api.dropFiles("resume", 3, "SLOW");
        List<String> files = List.of("resume-0.txt", "resume-1.txt", "resume-2.txt");

        TaskSnapshot first = api.start("resume");
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> api.status("resume", first.taskId())
                .pending() == 3);
        api.cancel("resume", first.taskId());
        assertThat(api.awaitFinished("resume", first.taskId()).state()).isEqualTo(TaskState.CANCELLED);
        assertThat(api.names("resume", "pendingbox")).containsExactlyElementsOf(files);
        awaitUserReleased("resume", first.taskId());

        // Ohne Entscheidung für a oder b wird der Start abgewiesen.
        PipelineApi.Response rejected = api.startResponse("resume");
        assertThat(rejected.status()).isEqualTo(409);
        assertThat(rejected.code()).isEqualTo("PENDINGBOX_NOT_EMPTY");

        // a) BatchgenAuftrag von Hand wieder auf RUNNING setzen; die Cloud schließt die Aufträge inzwischen ab.
        cloud.setJobStatus(first.taskId(), "RUNNING");
        cloud.release(files);
        TaskSnapshot second = api.start("resume");

        assertThat(second.taskId()).isEqualTo(first.taskId());
        assertThat(second.run()).isEqualTo(2);
        TaskSnapshot done = api.awaitFinished("resume", second.taskId());
        assertThat(done.run()).isEqualTo(2);
        assertThat(done.state()).isEqualTo(TaskState.COMPLETED);
        assertThat(done.resumed()).isEqualTo(3);
        assertThat(done.submitted()).isZero();
        assertThat(done.succeeded()).isEqualTo(3);
        assertThat(api.names("resume", "donebox")).containsExactlyElementsOf(files);
        assertThat(api.names("resume", "pendingbox")).isEmpty();
        assertThat(api.box("resume", "pendingbox").resolve(".taskids")).isEmptyDirectory();
        assertThat(cloud.submittedFileNames()).filteredOn(name -> name.startsWith("resume-"))
                .as("jede Datei genau einmal an Cloud-API 1").containsExactlyInAnyOrderElementsOf(files);
        assertThat(cloud.jobsOf("resume")).as("kein zweiter BatchgenAuftrag").containsExactly(first.taskId());
        // Der Endzustand des abgebrochenen ersten Laufs bleibt erhalten.
        assertThat(runs.finished(jobId(first), 1)).hasValueSatisfying(run1 ->
                assertThat(run1.state()).isEqualTo(TaskState.CANCELLED));
        awaitJobStatus(first.taskId(), "COMPLETED");
    }

    @Test
    void wegB_nachDemLeerenDerPendingboxBeginntEinNeuerBatchgenAuftrag() {
        api.dropFiles("restart", 2, "SLOW");
        TaskSnapshot first = api.start("restart");
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> api.status("restart", first.taskId())
                .pending() == 2);
        api.cancel("restart", first.taskId());
        api.awaitFinished("restart", first.taskId());
        awaitUserReleased("restart", first.taskId());

        api.clearPendingbox("restart");
        TaskSnapshot second = api.start("restart");

        assertThat(second.taskId()).isNotEqualTo(first.taskId());
        assertThat(second.run()).isEqualTo(1);
        assertThat(api.awaitFinished("restart", second.taskId()).state()).isEqualTo(TaskState.COMPLETED);
        assertThat(cloud.jobsOf("restart")).containsExactlyInAnyOrder(first.taskId(), second.taskId());
        assertThat(cloud.jobStatus(first.taskId())).isEqualTo("CANCELLED");
        assertThat(api.status("restart", first.taskId()).state()).as("alter Endzustand").isEqualTo(TaskState.CANCELLED);
    }

    @Test
    void dateiOhneTaskIdInDerPendingboxWirdBeimFortsetzenNichtErneutUebermittelt() throws IOException {
        // Zustand nach einem Absturz zwischen Beanspruchen und Antwort von Cloud-API 1: Der BatchgenAuftrag läuft
        // in der Cloud weiter, die Datei liegt ohne TaskId-Marker in der pendingbox.
        String job = cloud.createRunningJob("orphan");
        Files.createDirectories(api.box("orphan", "pendingbox"));
        Files.writeString(api.box("orphan", "pendingbox").resolve("orphan-0.txt"), "ok");

        TaskSnapshot task = api.start("orphan");
        TaskSnapshot done = api.awaitFinished("orphan", task.taskId());

        assertThat(task.taskId()).isEqualTo(job);
        assertThat(done.failed()).isEqualTo(1);
        assertThat(api.names("orphan", "errorbox")).containsExactly("orphan-0.txt");
        assertThat(api.names("orphan", "pendingbox")).isEmpty();
        assertThat(cloud.submittedFileNames()).doesNotContain("orphan-0.txt");
    }

    @Test
    void abbruchEinerAbgeschlossenenAufgabeWirdMitTaskNotRunningAbgewiesen() {
        TaskSnapshot task = api.start("cancel-finished");
        api.awaitFinished("cancel-finished", task.taskId());
        awaitJobStatus(task.taskId(), "COMPLETED");

        PipelineApi.Response response = api.cancelResponse("cancel-finished", task.taskId());

        assertThat(response.status()).isEqualTo(409);
        assertThat(response.code()).isEqualTo("TASK_NOT_RUNNING");
        assertThat(api.status("cancel-finished", task.taskId()).state()).isEqualTo(TaskState.COMPLETED);
    }

    @Test
    void gleichzeitigeAbbruecheSindIdempotent() throws Exception {
        api.dropFiles("cancel-race", 2, "SLOW");
        TaskSnapshot task = api.start("cancel-race");
        int attempts = 10;

        List<Integer> codes = new ArrayList<>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int i = 0; i < attempts; i++) {
                futures.add(executor.submit(() -> api.cancelStatus("cancel-race", task.taskId())));
            }
            for (var future : futures) {
                codes.add(future.get());
            }
        }

        assertThat(codes).containsOnly(202);
        TaskSnapshot done = api.awaitFinished("cancel-race", task.taskId());
        assertThat(done.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(done.cancelReason()).isEqualTo(CancelReason.USER_REQUEST);
    }

    @Test
    void zweiterStartDerselbenBenutzerinWirdAbgewiesenSolangeIhrTaskLaeuft() {
        api.dropFiles("dup", 2, "SLOW");

        TaskSnapshot first = api.start("dup");
        PipelineApi.Response second = api.startResponse("dup");

        assertThat(second.status()).isEqualTo(409);
        assertThat(second.code()).isEqualTo("USER_TASK_RUNNING");
        assertThat(tasksOf("dup")).containsExactly(first.taskId());
        assertThat(cloud.jobsOf("dup")).as("kein BatchgenAuftrag für den abgewiesenen Start")
                .containsExactly(first.taskId());

        // Nach dem Abschluss ist die Benutzer:in wieder frei; die leere pendingbox führt zu einem neuen Auftrag.
        cloud.release(List.of("dup-0.txt", "dup-1.txt"));
        api.awaitFinished("dup", first.taskId());
        awaitUserReleased("dup", first.taskId());
        TaskSnapshot third = api.start("dup");

        assertThat(third.taskId()).isNotEqualTo(first.taskId());
        assertThat(api.awaitFinished("dup", third.taskId()).state()).isEqualTo(TaskState.COMPLETED);
    }

    @Test
    void fehlerschwellenwertBrichtNurDenBetroffenenTaskUeberCloudApi3Ab() {
        api.dropFiles("threshold-bad", 6, "FAIL");
        api.dropFiles("threshold-ok", 3, "ok");

        TaskSnapshot bad = api.start("threshold-bad");
        TaskSnapshot good = api.start("threshold-ok");

        TaskSnapshot badDone = api.awaitFinished("threshold-bad", bad.taskId());
        assertThat(badDone.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(badDone.cancelReason()).isEqualTo(CancelReason.ERROR_THRESHOLD);
        assertThat(badDone.failed()).isGreaterThanOrEqualTo(3);
        assertThat(cloud.jobStatus(bad.taskId())).isEqualTo("CANCELLED");

        TaskSnapshot goodDone = api.awaitFinished("threshold-ok", good.taskId());
        assertThat(goodDone.state()).isEqualTo(TaskState.COMPLETED);
        assertThat(goodDone.succeeded()).isEqualTo(3);
    }

    @Test
    void nichtErreichbareCloudLiefert502UndGibtDieBenutzerinWiederFrei() {
        cloud.makeUnavailableFor("cloud-down");

        PipelineApi.Response first = api.startResponse("cloud-down");
        PipelineApi.Response second = api.startResponse("cloud-down");

        assertThat(first.status()).isEqualTo(502);
        assertThat(first.code()).isEqualTo("CLOUD_UNAVAILABLE");
        assertThat(second.status()).as("nicht 409: Die Benutzer:in wurde wieder freigegeben").isEqualTo(502);
    }

    @Test
    void apiVertragUndFehlerantworten() {
        TaskSnapshot task = api.start("contract");
        api.awaitFinished("contract", task.taskId());

        assertThat(api.status("contract", task.taskId()).taskId()).isEqualTo(task.taskId());
        assertThat(api.statusCode("contract", "unbekannt-1")).as("unbekannte Aufgabennummer").isEqualTo(404);
        assertThat(api.statusCode("other-user", task.taskId())).as("fremde Benutzer:in").isEqualTo(404);
        assertThat(api.statusCode("contract", "keine gültige Nummer")).isEqualTo(400);
        assertThat(api.startStatus("bad user")).as("ungültige Benutzerkennung").isEqualTo(400);
        assertThat(api.cancelStatus("other-user", task.taskId())).as("fremde Benutzer:in darf nicht abbrechen")
                .isEqualTo(404);
        assertThat(api.cancelStatus("contract", "unbekannt-1")).isEqualTo(404);
        assertThat(api.cancelStatus("contract", "keine gültige Nummer")).isEqualTo(400);
        assertThat(api.status("contract", task.taskId()).state()).as("fremder Abbruch ändert nichts")
                .isEqualTo(TaskState.COMPLETED);
    }

    /** Wartet, bis der TaskManager den BatchgenAuftrag über Cloud-API 3 auf {@code status} gesetzt hat. */
    private void awaitJobStatus(String jobId, String status) {
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> status.equals(cloud.jobStatus(jobId)));
    }

    /** Wartet, bis der Lauf abgemeldet ist und die Benutzer:in damit wieder starten darf. */
    private void awaitUserReleased(String user, String taskId) {
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> registry.find(new BatchJobId(taskId))
                .filter(sandbox -> sandbox.context().userId().equals(user)).isEmpty());
    }

    /** Die Aufgabennummern der Benutzer:in im Register. */
    private List<String> tasksOf(String user) {
        return registry.all().stream().filter(sandbox -> sandbox.context().userId().equals(user))
                .map(sandbox -> sandbox.context().jobId().value()).toList();
    }

    private static BatchJobId jobId(TaskSnapshot snapshot) {
        return new BatchJobId(snapshot.taskId());
    }

    private static <T> Set<T> distinct(List<T> items) {
        Set<T> identities = Collections.newSetFromMap(new IdentityHashMap<>());
        identities.addAll(items);
        return identities;
    }
}
