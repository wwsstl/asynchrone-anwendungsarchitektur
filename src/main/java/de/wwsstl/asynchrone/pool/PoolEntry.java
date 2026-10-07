package de.wwsstl.asynchrone.pool;

import java.nio.file.Path;
import java.time.Instant;

import de.wwsstl.asynchrone.cloud.TaskId;

/** Eine übermittelte Datei im Status-Pool: TaskId, Pfad in der {@code pendingbox} und Zeitpunkt der nächsten Prüfung. */
public record PoolEntry(TaskId taskId, Path file, Instant nextCheck) {
}
