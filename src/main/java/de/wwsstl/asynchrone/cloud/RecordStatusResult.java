package de.wwsstl.asynchrone.cloud;

/** Antwort von Cloud-API 2 für eine TaskId. */
public record RecordStatusResult(TaskId taskId, RecordStatus status) {
}
