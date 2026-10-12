package de.wwsstl.asynchrone.cloud;

/**
 * Antwort von Cloud-API 2 für eine TaskId.
 *
 * @param fehlermeldung Begründung der Cloud bei {@code ERROR}; sonst leer oder {@code null}
 */
public record RecordStatusResult(TaskId taskId, RecordStatus status, String fehlermeldung) {
}
