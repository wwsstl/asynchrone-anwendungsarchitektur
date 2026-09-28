package de.wwsstl.asynchrone.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.context.TaskState;

class InMemoryTaskHistoryTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final InMemoryTaskHistory history = new InMemoryTaskHistory();

    private static TaskSnapshot finished(Instant finishedAt) {
        return new TaskSnapshot("alice", UUID.randomUUID(), TaskState.COMPLETED, null, T0, finishedAt, true, 1, 0,
                1, 0, 0, 0);
    }

    @Test
    void legtEndzustaendeAbUndLoeschtSie() {
        TaskSnapshot snapshot = finished(T0);

        history.record(snapshot);

        assertThat(history.find(snapshot.taskId())).contains(snapshot);
        assertThat(history.remove(snapshot.taskId())).contains(snapshot);
        assertThat(history.remove(snapshot.taskId())).as("zweites remove").isEmpty();
        assertThat(history.size()).isZero();
    }

    @Test
    void lehntLaufendeTasksAb() {
        assertThatThrownBy(() -> history.record(finished(null))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void entferntGenauDieBisZumStichtagBeendetenEintraege() {
        TaskSnapshot before = finished(T0.minusSeconds(1));
        TaskSnapshot atCutoff = finished(T0);
        TaskSnapshot after = finished(T0.plusSeconds(1));
        history.record(before);
        history.record(atCutoff);
        history.record(after);

        assertThat(history.removeFinishedUntil(T0)).isEqualTo(2);

        assertThat(history.find(before.taskId())).isEmpty();
        assertThat(history.find(atCutoff.taskId())).isEmpty();
        assertThat(history.find(after.taskId())).contains(after);
    }
}
