package de.wwsstl.asynchrone.task;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/** {@link TaskRegistry} auf Basis einer {@link ConcurrentHashMap} (Phase 1: ein Server, keine Middleware). */
@Component
public class InMemoryTaskRegistry implements TaskRegistry {

    private final ConcurrentHashMap<String, Sandbox> sandboxes = new ConcurrentHashMap<>();

    @Override
    public void register(Sandbox sandbox) {
        String taskNumber = sandbox.context().taskNumber();
        if (sandboxes.putIfAbsent(taskNumber, sandbox) != null) {
            throw new IllegalStateException("Aufgabe " + taskNumber + " ist bereits registriert");
        }
    }

    @Override
    public Optional<Sandbox> find(String taskNumber) {
        return Optional.ofNullable(sandboxes.get(taskNumber));
    }

    @Override
    public void remove(Sandbox sandbox) {
        sandboxes.remove(sandbox.context().taskNumber(), sandbox);
    }

    @Override
    public Collection<Sandbox> all() {
        return List.copyOf(sandboxes.values());
    }

    @Override
    public int size() {
        return sandboxes.size();
    }
}
