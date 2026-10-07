package de.wwsstl.asynchrone.cloud;

/** Antwort von Cloud-API 1 für eine erfolgreich umgewandelte Datei. */
public record SubmittedFile(String fileName, TaskId taskId) {
}
