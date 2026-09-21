package de.wwsstl.asynchrone.pool;

import java.nio.file.Path;
import java.time.Instant;

/**
 * Eintrag im Status-Pool: Ursprungsdatei, nächster Prüfzeitpunkt und bisherige Versuche
 * (loesung_final.md 4.6).
 */
public record PollEntry(Path file, Instant nextPollAt, int attempts) {

    public PollEntry rescheduled(Instant next) {
        return new PollEntry(file, next, attempts + 1);
    }

    public boolean isDue(Instant now) {
        return !nextPollAt.isAfter(now);
    }
}
