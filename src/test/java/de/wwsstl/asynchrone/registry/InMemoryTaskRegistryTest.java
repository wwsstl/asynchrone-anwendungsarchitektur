package de.wwsstl.asynchrone.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.Test;

import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.Sandbox;
import de.wwsstl.asynchrone.context.TaskContext;
import de.wwsstl.asynchrone.pool.InMemoryStatusPool;

class InMemoryTaskRegistryTest {

    private final TaskRegistry registry = new InMemoryTaskRegistry();

    private static Sandbox sandbox(String user) {
        TaskContext context = new TaskContext(user, Duration.ofMinutes(1), 10, Clock.systemUTC());
        return new Sandbox(context, new InMemoryStatusPool(), Thread.ofVirtual().unstarted(() -> { }),
                Thread.ofVirtual().unstarted(() -> { }));
    }

    @Test
    void nachNRegistrierungenEnthaeltDasRegisterNTasks() {
        List<Sandbox> sandboxes = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            Sandbox s = sandbox("user-" + i);
            sandboxes.add(s);
            registry.register(s);
        }

        assertThat(registry.size()).isEqualTo(25);
        assertThat(registry.all()).containsExactlyInAnyOrderElementsOf(sandboxes);
        sandboxes.forEach(s -> assertThat(registry.find(s.context().taskId())).containsSame(s));
    }

    @Test
    void beendeteTasksBleibenImRegister() {
        Sandbox finished = sandbox("alice");
        Sandbox cancelled = sandbox("bob");
        registry.register(finished);
        registry.register(cancelled);

        finished.context().complete();
        cancelled.context().cancel(CancelReason.USER_REQUEST);

        assertThat(registry.size()).isEqualTo(2);
        assertThat(registry.find(finished.context().taskId())).isPresent();
        assertThat(registry.find(cancelled.context().taskId())).isPresent();
    }

    @Test
    void doppelteRegistrierungDesselbenTasksWirdAbgelehnt() {
        Sandbox s = sandbox("alice");
        registry.register(s);

        assertThatThrownBy(() -> registry.register(s)).isInstanceOf(IllegalStateException.class);
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void unbekannteTaskIdLiefertLeer() {
        assertThat(registry.find(java.util.UUID.randomUUID())).isEmpty();
    }

    @Test
    void nebenlaeufigeRegistrierungenGehenNichtVerloren() throws Exception {
        int n = 500;
        CountDownLatch go = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < n; i++) {
                String user = "user-" + i;
                executor.submit(() -> {
                    go.await();
                    registry.register(sandbox(user));
                    return null;
                });
            }
            go.countDown();
        }

        assertThat(registry.size()).isEqualTo(n);
    }
}
