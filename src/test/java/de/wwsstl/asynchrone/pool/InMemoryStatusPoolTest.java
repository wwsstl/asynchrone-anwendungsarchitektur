package de.wwsstl.asynchrone.pool;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Instant;

import org.junit.jupiter.api.Test;

class InMemoryStatusPoolTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final StatusPool pool = new InMemoryStatusPool();

    @Test
    void liefertNurFaelligeEintraege() {
        pool.add(new TaskId("a"), Path.of("a.txt"), NOW);
        pool.add(new TaskId("b"), Path.of("b.txt"), NOW.plusSeconds(10));

        assertThat(pool.due(NOW)).containsOnlyKeys(new TaskId("a"));
        assertThat(pool.due(NOW.plusSeconds(10))).containsOnlyKeys(new TaskId("a"), new TaskId("b"));
    }

    @Test
    void rescheduleVerschiebtPruefzeitpunktUndZaehltVersuche() {
        TaskId id = new TaskId("a");
        pool.add(id, Path.of("a.txt"), NOW);

        pool.reschedule(id, NOW.plusSeconds(5));

        assertThat(pool.due(NOW)).isEmpty();
        assertThat(pool.due(NOW.plusSeconds(5)).get(id).attempts()).isEqualTo(1);
        assertThat(pool.size()).isEqualTo(1);
    }

    @Test
    void rescheduleEinesEntferntenEintragsLegtIhnNichtNeuAn() {
        TaskId id = new TaskId("a");
        pool.add(id, Path.of("a.txt"), NOW);
        pool.remove(id);

        pool.reschedule(id, NOW);

        assertThat(pool.isEmpty()).isTrue();
    }

    @Test
    void removeUndDrain() {
        pool.add(new TaskId("a"), Path.of("a.txt"), NOW);
        pool.add(new TaskId("b"), Path.of("b.txt"), NOW);
        pool.add(new TaskId("c"), Path.of("c.txt"), NOW);

        assertThat(pool.remove(new TaskId("a"))).isPresent();
        assertThat(pool.remove(new TaskId("a"))).isEmpty();
        assertThat(pool.drain()).hasSize(2);
        assertThat(pool.isEmpty()).isTrue();
    }

    @Test
    void jederPoolHatSeineEigeneMap() {
        StatusPool other = new InMemoryStatusPool();

        pool.add(new TaskId("a"), Path.of("a.txt"), NOW);

        assertThat(other.isEmpty()).isTrue();
    }
}
