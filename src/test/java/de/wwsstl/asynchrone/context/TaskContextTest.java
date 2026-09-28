package de.wwsstl.asynchrone.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import de.wwsstl.asynchrone.MutableClock;

class TaskContextTest {

    private final MutableClock clock = new MutableClock();
    private final TaskContext context = new TaskContext("alice", Duration.ofMinutes(1), 3, clock);

    @Test
    void neuerTaskLaeuftUndHatEineEigeneId() {
        assertThat(context.state()).isEqualTo(TaskState.RUNNING);
        assertThat(context.isCancelled()).isFalse();
        assertThat(new TaskContext("alice", Duration.ofMinutes(1), 3, clock).taskId()).isNotEqualTo(context.taskId());
    }

    @Test
    void ersterAbbruchgrundGewinnt() {
        assertThat(context.cancel(CancelReason.USER_REQUEST)).isTrue();
        assertThat(context.cancel(CancelReason.TIMEOUT)).isFalse();

        assertThat(context.state()).isEqualTo(TaskState.CANCELLED);
        assertThat(context.cancelReason()).isEqualTo(CancelReason.USER_REQUEST);
        assertThat(context.isCancelled()).isTrue();
    }

    @Test
    void beendeterTaskLaesstSichNichtMehrAbbrechen() {
        assertThat(context.complete()).isTrue();

        assertThat(context.cancel(CancelReason.USER_REQUEST)).isFalse();
        assertThat(context.state()).isEqualTo(TaskState.COMPLETED);
        assertThat(context.isCancelled()).isFalse();
    }

    @Test
    void endzeitpunktWirdBeimErstenEndzustandGesetzt() {
        assertThat(context.finishedAt()).as("läuft noch").isNull();
        clock.advance(Duration.ofSeconds(5));
        Instant completedAt = clock.instant();

        context.complete();
        clock.advance(Duration.ofSeconds(5));
        context.cancel(CancelReason.USER_REQUEST);

        assertThat(context.finishedAt()).isEqualTo(completedAt);
        assertThat(context.snapshot(0).finishedAt()).isEqualTo(completedAt);
    }

    @Test
    void abbruchSetztDenEndzeitpunkt() {
        clock.advance(Duration.ofSeconds(5));

        context.cancel(CancelReason.TIMEOUT);

        assertThat(context.finishedAt()).isEqualTo(clock.instant());
    }

    @Test
    void fehlerschwellenwertBrichtDenTaskAb() {
        context.recordError();
        context.recordError();
        assertThat(context.isCancelled()).isFalse();

        context.recordError();

        assertThat(context.isCancelled()).isTrue();
        assertThat(context.cancelReason()).isEqualTo(CancelReason.ERROR_THRESHOLD);
        assertThat(context.errorCount()).isEqualTo(3);
    }

    @Test
    void timeoutWirdErstNachAblaufDerLaufzeitGesetzt() {
        clock.advance(Duration.ofSeconds(59));
        assertThat(context.checkTimeout()).isFalse();

        clock.advance(Duration.ofSeconds(1));
        assertThat(context.checkTimeout()).isTrue();
        assertThat(context.cancelReason()).isEqualTo(CancelReason.TIMEOUT);
        assertThat(context.checkTimeout()).as("nur der auslösende Aufruf meldet true").isFalse();
    }

    @Test
    void awaitCancelKehrtSofortBeiAbbruchZurueck() {
        assertThat(context.awaitCancel(Duration.ofMillis(10))).isFalse();

        context.cancel(CancelReason.USER_REQUEST);

        assertThat(context.awaitCancel(Duration.ofMinutes(5))).isTrue();
    }

    @Test
    void snapshotSpiegeltZaehlerWider() {
        context.recordSubmitted();
        context.recordSubmitted();
        context.recordSucceeded();
        context.recordError();
        context.recordAbandoned(4);

        TaskSnapshot snapshot = context.snapshot(7);

        assertThat(snapshot.userId()).isEqualTo("alice");
        assertThat(snapshot.taskId()).isEqualTo(context.taskId());
        assertThat(snapshot.submitted()).isEqualTo(2);
        assertThat(snapshot.succeeded()).isEqualTo(1);
        assertThat(snapshot.failed()).isEqualTo(1);
        assertThat(snapshot.pending()).isEqualTo(7);
        assertThat(snapshot.abandoned()).isEqualTo(4);
    }
}
