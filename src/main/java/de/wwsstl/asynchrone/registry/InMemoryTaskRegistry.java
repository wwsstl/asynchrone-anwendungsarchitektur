package de.wwsstl.asynchrone.registry;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import de.wwsstl.asynchrone.context.Sandbox;

/**
 * {@link TaskRegistry} auf Basis einer {@link ConcurrentHashMap} (Phase 1: rein In-Memory).
 *
 * <p>Singleton-Bean: Es gibt genau eine Instanz, die nie ersetzt, geleert oder verkleinert wird. Die Map ist
 * unbegrenzt — es gibt weder Eviction noch TTL —; ein Task verlässt sie nur durch {@link #remove(UUID)}.
 */
@Component
public class InMemoryTaskRegistry implements TaskRegistry {

    private final ConcurrentHashMap<UUID, Sandbox> tasks = new ConcurrentHashMap<>();

    @Override
    public void register(Sandbox sandbox) {
        UUID taskId = sandbox.context().taskId();
        if (tasks.putIfAbsent(taskId, sandbox) != null) {
            throw new IllegalStateException("Task " + taskId + " ist bereits registriert");
        }
    }

    @Override
    public Optional<Sandbox> find(UUID taskId) {
        return Optional.ofNullable(tasks.get(taskId));
    }

    @Override
    public Optional<Sandbox> remove(UUID taskId) {
        return Optional.ofNullable(tasks.remove(taskId));
    }

    @Override
    public Collection<Sandbox> all() {
        return List.copyOf(tasks.values());
    }

    @Override
    public int size() {
        return tasks.size();
    }
}
