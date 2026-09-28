package de.wwsstl.asynchrone.registry;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import de.wwsstl.asynchrone.context.TaskSnapshot;

/** {@link TaskHistory} auf Basis einer {@link ConcurrentHashMap} (Phase 1: rein In-Memory). */
@Component
public class InMemoryTaskHistory implements TaskHistory {

    private final ConcurrentHashMap<UUID, TaskSnapshot> entries = new ConcurrentHashMap<>();

    @Override
    public void record(TaskSnapshot snapshot) {
        if (snapshot.finishedAt() == null) {
            throw new IllegalArgumentException("Task " + snapshot.taskId() + " ist noch nicht beendet");
        }
        entries.put(snapshot.taskId(), snapshot);
    }

    @Override
    public Optional<TaskSnapshot> find(UUID taskId) {
        return Optional.ofNullable(entries.get(taskId));
    }

    @Override
    public Optional<TaskSnapshot> remove(UUID taskId) {
        return Optional.ofNullable(entries.remove(taskId));
    }

    @Override
    public int removeFinishedUntil(Instant cutoff) {
        int removed = 0;
        for (Map.Entry<UUID, TaskSnapshot> entry : entries.entrySet()) {
            if (!entry.getValue().finishedAt().isAfter(cutoff) && entries.remove(entry.getKey(), entry.getValue())) {
                removed++;
            }
        }
        return removed;
    }

    @Override
    public int size() {
        return entries.size();
    }
}
