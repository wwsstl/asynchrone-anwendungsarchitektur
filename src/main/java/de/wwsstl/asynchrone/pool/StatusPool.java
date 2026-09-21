package de.wwsstl.asynchrone.pool;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Status-Pool genau einer Sandbox. Die Schnittstelle kapselt die Datenstruktur, damit später eine verteilte
 * Message Queue an ihre Stelle treten kann (anforderungen.md, Abschnitt Skalierbarkeit).
 */
public interface StatusPool {

    /** Nimmt eine frisch übermittelte TaskId auf. */
    void add(TaskId id, Path file, Instant firstPollAt);

    /** Alle Einträge, deren Prüfzeitpunkt erreicht ist. */
    Map<TaskId, PollEntry> due(Instant now);

    /** Setzt den nächsten Prüfzeitpunkt (Status unverändert). */
    void reschedule(TaskId id, Instant nextPollAt);

    Optional<PollEntry> remove(TaskId id);

    /** Entnimmt alle verbliebenen Einträge (beim Beenden der Sandbox). */
    Map<TaskId, PollEntry> drain();

    int size();

    default boolean isEmpty() {
        return size() == 0;
    }
}
