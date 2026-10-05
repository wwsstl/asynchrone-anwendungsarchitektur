package de.wwsstl.asynchrone.registry;

import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import de.wwsstl.asynchrone.cloud.BatchJobId;
import de.wwsstl.asynchrone.context.TaskSnapshot;

/**
 * {@link RunRegistry} für Phase 1: ein Prozess, keine Datenbank (funktionsweise_sequenz.md, „Phase 1 ohne
 * Datenbank“).
 *
 * <ul>
 *   <li>Benutzer:in und Aufgabennummer belegen: {@code putIfAbsent} auf je einer {@link ConcurrentHashMap}</li>
 *   <li>Lauf freigeben: {@code remove(key, value)} in beiden Maps</li>
 *   <li>Endzustände: eine Map mit dem Schlüssel Aufgabennummer und Laufnummer</li>
 *   <li>Lease: entfällt. Endet der Prozess, verschwindet der gesamte Zustand, als wären alle Leases abgelaufen.</li>
 * </ul>
 */
@Component
public class InMemoryRunRegistry implements RunRegistry {

    private record RunKey(BatchJobId jobId, int number) {
    }

    private final ConcurrentHashMap<String, Run> activeByUser = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<BatchJobId, Run> activeByJob = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RunKey, TaskSnapshot> finished = new ConcurrentHashMap<>();

    @Override
    public Optional<Run> claimUser(String userId) {
        Run run = new Run(userId);
        return activeByUser.putIfAbsent(userId, run) == null ? Optional.of(run) : Optional.empty();
    }

    @Override
    public boolean claimTask(Run run, BatchJobId jobId) {
        if (activeByUser.get(run.userId()) != run || run.jobId() != null) {
            throw new IllegalStateException(run + " belegt die Benutzer:in nicht oder hat bereits eine Nummer");
        }
        if (activeByJob.putIfAbsent(jobId, run) != null) {
            return false;
        }
        // Solange dieser Lauf die Aufgabennummer belegt, legt niemand sonst darunter einen Endzustand ab.
        run.assign(jobId, nextNumber(jobId));
        return true;
    }

    private int nextNumber(BatchJobId jobId) {
        return finished.keySet().stream()
                .filter(key -> key.jobId().equals(jobId))
                .mapToInt(RunKey::number)
                .max().orElse(0) + 1;
    }

    @Override
    public void release(Run run) {
        BatchJobId jobId = run.jobId();
        if (jobId != null) {
            activeByJob.remove(jobId, run);
        }
        activeByUser.remove(run.userId(), run);
    }

    @Override
    public void finish(Run run, TaskSnapshot finalSnapshot) {
        if (run.jobId() == null) {
            throw new IllegalStateException(run + " ist nie gestartet");
        }
        if (finalSnapshot.finishedAt() == null) {
            throw new IllegalArgumentException(run + " ist noch nicht beendet");
        }
        // Erst ablegen, dann freigeben: Der nächste Lauf derselben Aufgabennummer zählt diesen Endzustand mit.
        finished.put(new RunKey(run.jobId(), run.number()), finalSnapshot);
        release(run);
    }

    @Override
    public Optional<TaskSnapshot> latestFinished(BatchJobId jobId) {
        return finished.entrySet().stream()
                .filter(entry -> entry.getKey().jobId().equals(jobId))
                .max(Comparator.comparingInt(entry -> entry.getKey().number()))
                .map(Map.Entry::getValue);
    }

    @Override
    public Optional<TaskSnapshot> finished(BatchJobId jobId, int run) {
        return Optional.ofNullable(finished.get(new RunKey(jobId, run)));
    }

    @Override
    public int removeFinishedUntil(Instant cutoff) {
        int removed = 0;
        for (Map.Entry<RunKey, TaskSnapshot> entry : finished.entrySet()) {
            if (!entry.getValue().finishedAt().isAfter(cutoff) && finished.remove(entry.getKey(), entry.getValue())) {
                removed++;
            }
        }
        return removed;
    }
}
