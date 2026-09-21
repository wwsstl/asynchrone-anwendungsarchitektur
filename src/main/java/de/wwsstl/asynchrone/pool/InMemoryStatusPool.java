package de.wwsstl.asynchrone.pool;

import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link StatusPool} auf Basis einer eigenen {@link ConcurrentHashMap} je Sandbox — nie eine gemeinsame Map
 * für mehrere Benutzer (loesung_final.md 4.6).
 */
public final class InMemoryStatusPool implements StatusPool {

    private final ConcurrentHashMap<TaskId, PollEntry> entries = new ConcurrentHashMap<>();

    @Override
    public void add(TaskId id, Path file, Instant firstPollAt) {
        entries.put(id, new PollEntry(file, firstPollAt, 0));
    }

    @Override
    public Map<TaskId, PollEntry> due(Instant now) {
        Map<TaskId, PollEntry> due = new HashMap<>();
        entries.forEach((id, entry) -> {
            if (entry.isDue(now)) {
                due.put(id, entry);
            }
        });
        return due;
    }

    @Override
    public void reschedule(TaskId id, Instant nextPollAt) {
        entries.computeIfPresent(id, (key, entry) -> entry.rescheduled(nextPollAt));
    }

    @Override
    public Optional<PollEntry> remove(TaskId id) {
        return Optional.ofNullable(entries.remove(id));
    }

    @Override
    public Map<TaskId, PollEntry> drain() {
        Map<TaskId, PollEntry> drained = new HashMap<>();
        for (TaskId id : entries.keySet()) {
            PollEntry entry = entries.remove(id);
            if (entry != null) {
                drained.put(id, entry);
            }
        }
        return drained;
    }

    @Override
    public int size() {
        return entries.size();
    }
}
