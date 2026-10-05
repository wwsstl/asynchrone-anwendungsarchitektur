package de.wwsstl.asynchrone.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import de.wwsstl.asynchrone.MutableClock;
import de.wwsstl.asynchrone.cloud.BatchJobId;

class TaskContextTest {

    private final MutableClock clock = new MutableClock();
    private final TaskContext context = new TaskContext("alice", new BatchJobId("BJ-1"), 2, Duration.ofMinutes(1), 3,
            clock);

    @Test
    void neuerTaskLaeuftUnterSeinerAufgabennummerUndLaufnummer() {
        assertThat(context.state()).isEqualTo(TaskState.RUNNING);
        assertThat(context.isCancelled()).isFalse();
        assertThat(context.isStopping()).isFalse();
        assertThat(context.jobId()).isEqualTo(new BatchJobId("BJ-1"));
        assertThat(context.run()).isEqualTo(2);
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
        assertThat(context.requestAbort(CancelReason.TIMEOUT)).isFalse();
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
    void fehlerschwellenwertWirdNurFestgestelltUndBrichtNichtSelbstAb() {
        context.recordError();
        context.recordError();
        assertThat(context.errorThresholdReached()).isFalse();

        context.recordError();

        assertThat(context.errorThresholdReached()).isTrue();
        assertThat(context.isCancelled()).as("der Abbruch folgt über Cloud-API 3").isFalse();
        assertThat(context.errorCount()).isEqualTo(3);
    }

    @Test
    void timeoutGiltErstNachAblaufDerLaufzeit() {
        clock.advance(Duration.ofSeconds(59));
        assertThat(context.isTimedOut()).isFalse();

        clock.advance(Duration.ofSeconds(1));
        assertThat(context.isTimedOut()).isTrue();
        assertThat(context.isCancelled()).isFalse();
    }

    @Test
    void abbruchmeldungHaeltDenTaskAnOhneIhnZuBeenden() {
        assertThat(context.requestAbort(CancelReason.ERROR_THRESHOLD)).isTrue();
        assertThat(context.requestAbort(CancelReason.TIMEOUT)).as("die erste Meldung gewinnt").isFalse();

        assertThat(context.isStopping()).isTrue();
        assertThat(context.abortRequest()).contains(CancelReason.ERROR_THRESHOLD);
        assertThat(context.state()).isEqualTo(TaskState.RUNNING);
        assertThat(context.finishedAt()).isNull();
    }

    @Test
    void awaitStopKehrtBeiAbbruchmeldungUndAbbruchSofortZurueck() {
        assertThat(context.awaitStop(Duration.ofMillis(10))).isFalse();

        context.requestAbort(CancelReason.TIMEOUT);
        assertThat(context.awaitStop(Duration.ofMinutes(5))).isTrue();

        TaskContext other = new TaskContext("bob", new BatchJobId("BJ-2"), 1, Duration.ofMinutes(1), 3, clock);
        other.cancel(CancelReason.USER_REQUEST);
        assertThat(other.awaitStop(Duration.ofMinutes(5))).isTrue();
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
        assertThat(snapshot.taskId()).isEqualTo("BJ-1");
        assertThat(snapshot.run()).isEqualTo(2);
        assertThat(snapshot.submitted()).isEqualTo(2);
        assertThat(snapshot.succeeded()).isEqualTo(1);
        assertThat(snapshot.failed()).isEqualTo(1);
        assertThat(snapshot.pending()).isEqualTo(7);
        assertThat(snapshot.abandoned()).isEqualTo(4);
        assertThat(snapshot.withState(TaskState.CANCELLING).state()).isEqualTo(TaskState.CANCELLING);
    }
}
