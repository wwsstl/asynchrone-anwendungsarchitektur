package de.wwsstl.asynchrone.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import de.wwsstl.asynchrone.cloud.BatchJobId;
import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.context.TaskState;

class InMemoryRunRegistryTest {

    private static final BatchJobId JOB = new BatchJobId("BJ-1");
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final RunRegistry runs = new InMemoryRunRegistry();

    private static TaskSnapshot finished(Run run, TaskState state, Instant finishedAt) {
        return new TaskSnapshot(run.userId(), run.jobId().value(), run.number(), state,
                state == TaskState.CANCELLED ? CancelReason.USER_REQUEST : null, T0, finishedAt, true, 0, 0, 0, 0,
                0, 0);
    }

    private Run start(String user, BatchJobId job) {
        Run run = runs.claimUser(user).orElseThrow();
        assertThat(runs.claimTask(run, job)).isTrue();
        return run;
    }

    @Test
    void eineBenutzerinKannNurEinenLaufBelegen() {
        Optional<Run> first = runs.claimUser("alice");

        assertThat(first).isPresent();
        assertThat(runs.claimUser("alice")).isEmpty();
        assertThat(runs.claimUser("bob")).as("andere Benutzer:in").isPresent();

        runs.release(first.orElseThrow());
        assertThat(runs.claimUser("alice")).as("nach der Freigabe").isPresent();
    }

    @Test
    void eineAufgabennummerKannNurEinLaufBelegen() {
        Run alice = runs.claimUser("alice").orElseThrow();
        Run bob = runs.claimUser("bob").orElseThrow();

        assertThat(runs.claimTask(alice, JOB)).isTrue();
        assertThat(runs.claimTask(bob, JOB)).isFalse();

        assertThat(alice.jobId()).isEqualTo(JOB);
        assertThat(alice.number()).isEqualTo(1);
        assertThat(bob.jobId()).as("bleibt STARTING").isNull();
    }

    @Test
    void ohneBelegungDerBenutzerinLaesstSichKeineAufgabennummerBelegen() {
        Run run = runs.claimUser("alice").orElseThrow();
        runs.release(run);

        assertThatThrownBy(() -> runs.claimTask(run, JOB)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void laufnummernZaehlenJeAufgabennummerFortlaufend() {
        Run first = start("alice", JOB);
        runs.finish(first, finished(first, TaskState.CANCELLED, T0));

        Run second = start("alice", JOB);
        Run other = start("bob", new BatchJobId("BJ-2"));

        assertThat(second.number()).isEqualTo(2);
        assertThat(other.number()).as("eigene Zählung je Aufgabennummer").isEqualTo(1);
    }

    @Test
    void einAbgewiesenerOderGescheiterterStartHinterlaesstKeinenEndzustand() {
        Run run = start("alice", JOB);

        runs.release(run);

        assertThat(runs.latestFinished(JOB)).isEmpty();
        assertThat(start("alice", JOB).number()).as("Nummer wird wieder vergeben").isEqualTo(1);
    }

    @Test
    void endzustaendeBleibenJeLaufErhalten() {
        Run first = start("alice", JOB);
        TaskSnapshot cancelled = finished(first, TaskState.CANCELLED, T0);
        runs.finish(first, cancelled);
        Run second = start("alice", JOB);
        TaskSnapshot completed = finished(second, TaskState.COMPLETED, T0.plusSeconds(60));
        runs.finish(second, completed);

        assertThat(runs.latestFinished(JOB)).contains(completed);
        assertThat(runs.finished(JOB, 1)).contains(cancelled);
        assertThat(runs.finished(JOB, 2)).contains(completed);
        // Mit dem Endzustand sind Benutzer:in und Aufgabennummer wieder frei.
        assertThat(runs.claimUser("alice")).isPresent();
    }

    @Test
    void endzustandOhneEndzeitpunktWirdAbgelehnt() {
        Run run = start("alice", JOB);

        assertThatThrownBy(() -> runs.finish(run, finished(run, TaskState.RUNNING, null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void abgelaufeneEndzustaendeWerdenEntferntUndNichtMehrGezaehlt() {
        Run first = start("alice", JOB);
        runs.finish(first, finished(first, TaskState.CANCELLED, T0));
        Run other = start("bob", new BatchJobId("BJ-2"));
        runs.finish(other, finished(other, TaskState.COMPLETED, T0.plusSeconds(10)));

        assertThat(runs.removeFinishedUntil(T0)).isEqualTo(1);

        assertThat(runs.latestFinished(JOB)).isEmpty();
        assertThat(runs.latestFinished(new BatchJobId("BJ-2"))).isPresent();
        assertThat(start("alice", JOB).number()).as("Zählung beginnt wieder bei 1").isEqualTo(1);
    }

    @Test
    void gleichzeitigeBelegungenDerselbenBenutzerinGelingenGenauEinmal() throws Exception {
        int attempts = 50;
        CountDownLatch go = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < attempts; i++) {
                results.add(executor.submit(() -> {
                    go.await();
                    return runs.claimUser("alice").isPresent();
                }));
            }
            go.countDown();
        }

        assertThat(results.stream().filter(result -> result.resultNow()).count()).isEqualTo(1);
    }
}
