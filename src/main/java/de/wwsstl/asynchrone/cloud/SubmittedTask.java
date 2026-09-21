package de.wwsstl.asynchrone.cloud;

import de.wwsstl.asynchrone.pool.TaskId;

/** Antwort von Cloud-API 1: die TaskId, unter der die Datei {@code fileName} verarbeitet wird. */
public record SubmittedTask(String fileName, TaskId taskId) {
}
