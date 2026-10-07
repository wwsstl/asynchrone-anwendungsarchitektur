package de.wwsstl.asynchrone.pool;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import de.wwsstl.asynchrone.cloud.TaskId;

/** {@link StatusPool} im Speicher; je Sandbox eine eigene Instanz. */
public final class InMemoryStatusPool implements StatusPool {

    private final ConcurrentHashMap<TaskId, PoolEntry> entries = new ConcurrentHashMap<>();

    @Override
    public void add(TaskId taskId, Path file, Instant nextCheck) {
        entries.put(taskId, new PoolEntry(taskId, file, nextCheck));
    }

    @Override
    public List<PoolEntry> due(Instant now) {
        return entries.values().stream().filter(entry -> !entry.nextCheck().isAfter(now)).toList();
    }

    @Override
    public void reschedule(TaskId taskId, Instant nextCheck) {
        entries.computeIfPresent(taskId, (id, entry) -> new PoolEntry(id, entry.file(), nextCheck));
    }

    @Override
    public void remove(TaskId taskId) {
        entries.remove(taskId);
    }

    @Override
    public int size() {
        return entries.size();
    }
}
