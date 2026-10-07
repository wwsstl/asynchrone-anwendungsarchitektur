package de.wwsstl.asynchrone.pool;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import de.wwsstl.asynchrone.cloud.TaskId;

class InMemoryStatusPoolTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final InMemoryStatusPool pool = new InMemoryStatusPool();
    private final TaskId first = new TaskId("t-1");
    private final TaskId second = new TaskId("t-2");

    @Test
    void returnsOnlyDueEntries() {
        pool.add(first, Path.of("a.json"), NOW);
        pool.add(second, Path.of("b.json"), NOW.plusSeconds(5));

        assertThat(pool.due(NOW)).extracting(PoolEntry::taskId).containsExactly(first);
        assertThat(pool.due(NOW.plusSeconds(5))).extracting(PoolEntry::taskId).containsExactlyInAnyOrder(first, second);
        assertThat(pool.size()).isEqualTo(2);
    }

    @Test
    void rescheduleKeepsFileAndMovesNextCheck() {
        pool.add(first, Path.of("a.json"), NOW);

        pool.reschedule(first, NOW.plusSeconds(10));

        assertThat(pool.due(NOW)).isEmpty();
        assertThat(pool.due(NOW.plusSeconds(10)))
                .containsExactly(new PoolEntry(first, Path.of("a.json"), NOW.plusSeconds(10)));
    }

    @Test
    void rescheduleOfRemovedEntryDoesNotAddIt() {
        pool.add(first, Path.of("a.json"), NOW);
        pool.remove(first);

        pool.reschedule(first, NOW);

        assertThat(pool.isEmpty()).isTrue();
    }
}
