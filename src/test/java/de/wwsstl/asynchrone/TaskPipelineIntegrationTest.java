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
import java.util.UUID;

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

import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.Sandbox;
import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.context.TaskState;
import de.wwsstl.asynchrone.registry.TaskRegistry;
import de.wwsstl.asynchrone.taskmanager.TaskManager;

/**
 * Ende-zu-Ende-Test über die REST-API gegen einen lokalen Fake der Cloud-Dienste (echter HTTP-Server, sodass der
 * {@code WebClient} tatsächlich benutzt wird). Alle Tests teilen sich einen Spring-Kontext — und damit dieselben
 * {@code TaskManager}- und {@code TaskRegistry}-Singletons; das Register wächst über die Tests hinweg, daher
 * vergleichen sie stets gegen den Stand vor dem Test.
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

    PipelineApi api;

    @BeforeEach
    void setUp() {
        api = new PipelineApi(port, base);
    }

    @Test
    void taskManagerUndRegisterSindSingletonsUndBleibenDieGanzeLaufzeitBestehen() {
        assertThat(context.getBean(TaskManager.class)).isSameAs(taskManager);
        assertThat(context.getBean(TaskRegistry.class)).isSameAs(registry);
        assertThat(context.getBeansOfType(TaskManager.class)).hasSize(1);
        assertThat(context.getBeansOfType(TaskRegistry.class)).hasSize(1);

        api.dropFiles("singleton-user", 1, "ok");
        api.awaitFinished("singleton-user", api.start("singleton-user").taskId());

        // Auch nach vielen Requests und beendeten Tasks sind es dieselben Instanzen.
        assertThat(context.getBean(TaskManager.class)).isSameAs(taskManager);
        assertThat(context.getBean(TaskRegistry.class)).isSameAs(registry);
    }

    @Test
    void nBenutzerErgebenNTasksImRegisterJeMitEigenerSandbox() {
        int n = 6;
        int before = registry.size();
        List<String> users = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String user = "many" + i;
            users.add(user);
            api.dropFiles(user, 3, "ok");
        }

        List<TaskSnapshot> started = users.stream().map(api::start).toList();

        // n externe Benutzer -> genau n Tasks im Register
        assertThat(registry.size()).isEqualTo(before + n);
        assertThat(started).extracting(TaskSnapshot::taskId).doesNotHaveDuplicates();

        // jeder Task hat seine eigene, vollständige Sandbox
        List<Sandbox> sandboxes = started.stream().map(s -> registry.find(s.taskId()).orElseThrow()).toList();
        assertThat(distinct(sandboxes.stream().map(Sandbox::context).toList())).hasSize(n);
        assertThat(distinct(sandboxes.stream().map(Sandbox::pool).toList())).hasSize(n);
        assertThat(distinct(sandboxes.stream().map(Sandbox::producerThread).toList())).hasSize(n);
        assertThat(distinct(sandboxes.stream().map(Sandbox::consumerThread).toList())).hasSize(n);
        assertThat(sandboxes).allSatisfy(s -> {
            assertThat(s.producerThread().isVirtual()).isTrue();
            assertThat(s.consumerThread().isVirtual()).isTrue();
        });

        for (int i = 0; i < n; i++) {
            TaskSnapshot done = api.awaitFinished(users.get(i), started.get(i).taskId());
            assertThat(done.state()).isEqualTo(TaskState.COMPLETED);
            assertThat(done.submitted()).isEqualTo(3);
            assertThat(done.succeeded()).isEqualTo(3);
            assertThat(api.names(users.get(i), "donebox")).hasSize(3);
            assertThat(api.names(users.get(i), "inbox")).isEmpty();
        }

        // beendete Tasks bleiben im Register, ihre Threads sind beendet
        assertThat(registry.size()).isEqualTo(before + n);
        sandboxes.forEach(s -> {
            assertThat(registry.find(s.context().taskId())).containsSame(s);
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !s.producerThread().isAlive()
                    && !s.consumerThread().isAlive());
        });
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
        TaskSnapshot done = api.awaitFinished("empty", api.start("empty").taskId());

        assertThat(done.state()).isEqualTo(TaskState.COMPLETED);
        assertThat(done.submitted()).isZero();
    }

    @Test
    void abbruchVonAussenBeendetNurDenEigenenTask() {
        api.dropFiles("cancel-me", 4, "SLOW");
        api.dropFiles("cancel-neighbor", 3, "ok");

        TaskSnapshot mine = api.start("cancel-me");
        TaskSnapshot neighbor = api.start("cancel-neighbor");
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> api.status("cancel-me", mine.taskId())
                .pending() == 4);

        TaskSnapshot cancelled = api.cancel("cancel-me", mine.taskId());

        assertThat(cancelled.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(cancelled.cancelReason()).isEqualTo(CancelReason.USER_REQUEST);
        assertThat(api.cancelStatus("cancel-me", mine.taskId())).as("zweiter Abbruch").isEqualTo(409);

        Sandbox sandbox = registry.find(mine.taskId()).orElseThrow();
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !sandbox.producerThread().isAlive()
                && !sandbox.consumerThread().isAlive());
        TaskSnapshot after = api.status("cancel-me", mine.taskId());
        assertThat(after.pending()).isZero();
        assertThat(after.abandoned()).isEqualTo(4);
        assertThat(api.names("cancel-me", "inbox")).hasSize(4);

        // der Nachbar-Task ist davon unberührt
        assertThat(api.awaitFinished("cancel-neighbor", neighbor.taskId()).state()).isEqualTo(TaskState.COMPLETED);
        assertThat(registry.find(mine.taskId())).isPresent();
    }

    @Test
    void zweiterStartDesselbenBenutzersWirdAbgewiesenSolangeDerTaskLaeuft() {
        api.dropFiles("dup", 2, "SLOW");
        int before = registry.size();

        TaskSnapshot first = api.start("dup");
        assertThat(api.startStatus("dup")).isEqualTo(409);
        assertThat(registry.size()).isEqualTo(before + 1);

        api.cancel("dup", first.taskId());
        TaskSnapshot second = api.start("dup");

        assertThat(second.taskId()).isNotEqualTo(first.taskId());
        assertThat(registry.size()).as("beide Tasks bleiben im Register").isEqualTo(before + 2);
        assertThat(api.status("dup", first.taskId()).state()).isEqualTo(TaskState.CANCELLED);
        api.cancel("dup", second.taskId());
    }

    @Test
    void fehlerschwellenwertBrichtNurDenBetroffenenTaskAb() {
        api.dropFiles("threshold-bad", 6, "FAIL");
        api.dropFiles("threshold-ok", 3, "ok");

        TaskSnapshot bad = api.start("threshold-bad");
        TaskSnapshot good = api.start("threshold-ok");

        TaskSnapshot badDone = api.awaitFinished("threshold-bad", bad.taskId());
        assertThat(badDone.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(badDone.cancelReason()).isEqualTo(CancelReason.ERROR_THRESHOLD);
        assertThat(badDone.failed()).isGreaterThanOrEqualTo(3);

        TaskSnapshot goodDone = api.awaitFinished("threshold-ok", good.taskId());
        assertThat(goodDone.state()).isEqualTo(TaskState.COMPLETED);
        assertThat(goodDone.succeeded()).isEqualTo(3);
    }

    @Test
    void apiVertragUndFehlerantworten() {
        TaskSnapshot task = api.start("contract");
        api.awaitFinished("contract", task.taskId());

        assertThat(api.status("contract", task.taskId()).taskId()).isEqualTo(task.taskId());
        assertThat(api.statusCode("contract", UUID.randomUUID().toString())).as("unbekannte Task-ID").isEqualTo(404);
        assertThat(api.statusCode("other-user", task.taskId().toString())).as("fremder Benutzer").isEqualTo(404);
        assertThat(api.statusCode("contract", "keine-uuid")).isEqualTo(400);
        assertThat(api.startStatus("bad user")).as("ungültige Benutzerkennung").isEqualTo(400);
        assertThat(api.cancelStatus("contract", task.taskId())).as("Abbruch eines beendeten Tasks").isEqualTo(409);
    }

    private static <T> Set<T> distinct(List<T> items) {
        Set<T> identities = Collections.newSetFromMap(new IdentityHashMap<>());
        identities.addAll(items);
        return identities;
    }
}
