package de.wwsstl.asynchrone.cloud;

import de.wwsstl.asynchrone.pool.TaskId;

/** Antwort von Cloud-API 2 für eine einzelne TaskId. */
public record TaskStatusResult(TaskId taskId, CloudStatus status) {
}
