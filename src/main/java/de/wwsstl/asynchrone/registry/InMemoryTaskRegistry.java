package de.wwsstl.asynchrone.registry;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import de.wwsstl.asynchrone.cloud.BatchJobId;
import de.wwsstl.asynchrone.context.Sandbox;
import de.wwsstl.asynchrone.context.TaskState;

/**
 * {@link TaskRegistry} auf Basis einer {@link ConcurrentHashMap}.
 *
 * <p>Singleton-Bean: Es gibt genau eine Instanz, die nie ersetzt oder geleert wird. Die Map selbst kennt weder
 * Eviction noch TTL; eine Sandbox verlässt sie nur durch {@link #remove(Sandbox)} oder wird beim nächsten Lauf
 * derselben Aufgabennummer ersetzt.
 */
@Component
public class InMemoryTaskRegistry implements TaskRegistry {

    private final ConcurrentHashMap<BatchJobId, Sandbox> tasks = new ConcurrentHashMap<>();

    @Override
    public void register(Sandbox sandbox) {
        tasks.compute(sandbox.context().jobId(), (jobId, existing) -> {
            if (existing != null && existing.context().state() == TaskState.RUNNING) {
                throw new IllegalStateException("Unter Aufgabennummer " + jobId + " ist bereits ein Lauf registriert");
            }
            return sandbox;
        });
    }

    @Override
    public Optional<Sandbox> find(BatchJobId jobId) {
        return Optional.ofNullable(tasks.get(jobId));
    }

    @Override
    public boolean remove(Sandbox sandbox) {
        return tasks.remove(sandbox.context().jobId(), sandbox);
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
