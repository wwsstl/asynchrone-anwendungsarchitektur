package de.wwsstl.asynchrone.pool;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import de.wwsstl.asynchrone.cloud.TaskId;

/**
 * Status-Pool einer Sandbox (in anforderungen.md „TestdatenAnlegenStatusPool“): die übermittelten Dateien, deren
 * Ergebnis noch aussteht. Der Producer trägt ein, der Consumer fragt ab und trägt aus.
 *
 * <p>Die Abstraktion erlaubt es, den Pool später durch eine verteilte Message Queue zu ersetzen.
 */
public interface StatusPool {

    void add(TaskId taskId, Path file, Instant nextCheck);

    /** Einträge, deren nächste Prüfung spätestens {@code now} fällig ist. */
    List<PoolEntry> due(Instant now);

    void reschedule(TaskId taskId, Instant nextCheck);

    void remove(TaskId taskId);

    int size();

    default boolean isEmpty() {
        return size() == 0;
    }
}
