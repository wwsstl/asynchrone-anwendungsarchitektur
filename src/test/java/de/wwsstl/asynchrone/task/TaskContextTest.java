package de.wwsstl.asynchrone.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import de.wwsstl.asynchrone.MutableClock;
import de.wwsstl.asynchrone.cloud.JobStatus;

class TaskContextTest {

    private final MutableClock clock = new MutableClock();
    private final TaskContext context = new TaskContext("anna", "BJ-1", Duration.ofMinutes(1), 2, clock);

    @Test
    void firstEndStateWinsAndSetsStopSignal() {
        assertThat(context.state()).isEqualTo(TaskState.RUNNING);
        assertThat(context.isStopping()).isFalse();

        assertThat(context.reason()).isNull();

        clock.advance(Duration.ofSeconds(3));
        assertThat(context.finish(TaskState.TERMINATED, "Abbruch")).isTrue();
        assertThat(context.finish(TaskState.COMPLETED, "fertig")).isFalse();

        assertThat(context.state()).isEqualTo(TaskState.TERMINATED);
        assertThat(context.reason()).isEqualTo("Abbruch");
        assertThat(context.isStopping()).isTrue();
        assertThat(context.finishedAt()).isEqualTo(clock.instant());
        assertThat(context.snapshot(0).reason()).isEqualTo("Abbruch");
    }

    @Test
    void runningIsNoEndState() {
        assertThatThrownBy(() -> context.finish(TaskState.RUNNING, "läuft"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void awaitStopWakesUpOnEndState() throws InterruptedException {
        Thread waiter = Thread.ofVirtual().start(() -> assertThat(context.awaitStop(Duration.ofMinutes(5))).isTrue());

        context.finish(TaskState.ERROR, "Fehlerschwelle");

        waiter.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(waiter.isAlive()).isFalse();
    }

    @Test
    void awaitStopReturnsFalseWhileRunning() {
        assertThat(context.awaitStop(Duration.ofMillis(10))).isFalse();
    }

    @Test
    void timesOutAfterTaskTimeout() {
        clock.advance(Duration.ofSeconds(59));
        assertThat(context.isTimedOut()).isFalse();

        clock.advance(Duration.ofSeconds(1));
        assertThat(context.isTimedOut()).isTrue();
    }

    @Test
    void errorThresholdIsReachedAtConfiguredCount() {
        context.recordError();
        assertThat(context.errorThresholdReached()).isFalse();

        context.recordError();
        assertThat(context.errorThresholdReached()).isTrue();
    }

    @Test
    void snapshotContainsCounters() {
        context.recordSubmitted();
        context.recordSubmitted();
        context.recordSucceeded();
        context.recordError();
        context.recordUnclear(3);

        TaskSnapshot snapshot = context.snapshot(1);

        assertThat(snapshot).isEqualTo(new TaskSnapshot("BJ-1", "anna", TaskState.RUNNING, null, clock.instant(), null,
                2, 1, 1, 1, 3));
    }

    @Test
    void onlyEndStatesOfTheApplicationAreWrittenToTheCloud() {
        assertThat(TaskState.COMPLETED.jobStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(TaskState.TIMEOUT.jobStatus()).isEqualTo(JobStatus.TIMEOUT);
        assertThat(TaskState.ERROR.jobStatus()).isEqualTo(JobStatus.ERROR);
        assertThat(TaskState.TERMINATED.jobStatus()).isEqualTo(JobStatus.TERMINATED);
        assertThat(TaskState.RUNNING.jobStatus()).isNull();
        assertThat(TaskState.ABORTED.jobStatus()).isNull();
    }
}
