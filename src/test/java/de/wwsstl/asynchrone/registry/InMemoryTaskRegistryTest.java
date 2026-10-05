package de.wwsstl.asynchrone.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import de.wwsstl.asynchrone.cloud.BatchJobId;
import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.Sandbox;
import de.wwsstl.asynchrone.context.TaskContext;
import de.wwsstl.asynchrone.pool.InMemoryStatusPool;

class InMemoryTaskRegistryTest {

    private final TaskRegistry registry = new InMemoryTaskRegistry();

    private static Sandbox sandbox(String user, String job) {
        TaskContext context = new TaskContext(user, new BatchJobId(job), 1, Duration.ofMinutes(1), 10,
                Clock.systemUTC());
        return new Sandbox(context, new InMemoryStatusPool(), Thread.ofVirtual().unstarted(() -> { }),
                Thread.ofVirtual().unstarted(() -> { }));
    }

    @Test
    void nachNRegistrierungenEnthaeltDasRegisterNTasks() {
        List<Sandbox> sandboxes = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            Sandbox s = sandbox("user-" + i, "BJ-" + i);
            sandboxes.add(s);
            registry.register(s);
        }

        assertThat(registry.size()).isEqualTo(25);
        assertThat(registry.all()).containsExactlyInAnyOrderElementsOf(sandboxes);
        sandboxes.forEach(s -> assertThat(registry.find(s.context().jobId())).containsSame(s));
    }

    @Test
    void dasRegisterEntferntBeendeteTasksNichtVonSelbst() {
        Sandbox finished = sandbox("alice", "BJ-1");
        Sandbox cancelled = sandbox("bob", "BJ-2");
        registry.register(finished);
        registry.register(cancelled);

        finished.context().complete();
        cancelled.context().cancel(CancelReason.USER_REQUEST);

        assertThat(registry.size()).isEqualTo(2);
        assertThat(registry.find(finished.context().jobId())).isPresent();
        assertThat(registry.find(cancelled.context().jobId())).isPresent();
    }

    @Test
    void removeLoeschtNurDieAngegebeneSandbox() {
        Sandbox alice = sandbox("alice", "BJ-1");
        Sandbox bob = sandbox("bob", "BJ-2");
        registry.register(alice);
        registry.register(bob);

        assertThat(registry.remove(alice)).isTrue();

        assertThat(registry.find(alice.context().jobId())).isEmpty();
        assertThat(registry.find(bob.context().jobId())).containsSame(bob);
        assertThat(registry.size()).isEqualTo(1);
        assertThat(registry.remove(alice)).as("zweites remove").isFalse();
    }

    @Test
    void zweiterLaufDerselbenAufgabennummerWirdAbgelehntSolangeDerErsteLaeuft() {
        Sandbox first = sandbox("alice", "BJ-1");
        registry.register(first);

        assertThatThrownBy(() -> registry.register(sandbox("alice", "BJ-1"))).isInstanceOf(IllegalStateException.class);
        assertThat(registry.find(first.context().jobId())).containsSame(first);
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void nachfolgerErsetztEinenBeendetenLaufUndUeberlebtDessenAbmeldung() {
        Sandbox first = sandbox("alice", "BJ-1");
        registry.register(first);
        first.context().cancel(CancelReason.USER_REQUEST);

        Sandbox second = sandbox("alice", "BJ-1");
        registry.register(second);
        // Die verspätete Abmeldung des ersten Laufs entfernt nicht den Nachfolger.
        assertThat(registry.remove(first)).isFalse();

        assertThat(registry.find(second.context().jobId())).containsSame(second);
    }

    @Test
    void unbekannteAufgabennummerLiefertLeer() {
        assertThat(registry.find(new BatchJobId("unbekannt"))).isEmpty();
    }

    @Test
    void nebenlaeufigeRegistrierungenGehenNichtVerloren() throws Exception {
        int n = 500;
        CountDownLatch go = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < n; i++) {
                String user = "user-" + i;
                String job = "BJ-" + i;
                executor.submit(() -> {
                    go.await();
                    registry.register(sandbox(user, job));
                    return null;
                });
            }
            go.countDown();
        }

        assertThat(registry.size()).isEqualTo(n);
    }
}
