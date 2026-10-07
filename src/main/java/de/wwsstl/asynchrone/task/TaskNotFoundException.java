package de.wwsstl.asynchrone.task;

/** Die Aufgabe ist nicht bekannt oder gehört einer anderen Benutzer:in. */
public class TaskNotFoundException extends RuntimeException {

    public TaskNotFoundException(String userId, String taskNumber) {
        super("Aufgabe " + taskNumber + " von Benutzer '" + userId + "' existiert nicht");
    }
}
