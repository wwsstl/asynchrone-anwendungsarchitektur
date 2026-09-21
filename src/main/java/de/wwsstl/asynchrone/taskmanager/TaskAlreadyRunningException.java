package de.wwsstl.asynchrone.taskmanager;

import java.util.UUID;

/**
 * Ein Benutzer hat nur eine {@code inbox}; zwei gleichzeitige Tasks würden dieselben Dateien doppelt übermitteln.
 */
public class TaskAlreadyRunningException extends RuntimeException {

    private final UUID runningTaskId;

    public TaskAlreadyRunningException(String userId, UUID runningTaskId) {
        super("Für Benutzer '" + userId + "' läuft bereits Task " + runningTaskId);
        this.runningTaskId = runningTaskId;
    }

    public UUID runningTaskId() {
        return runningTaskId;
    }
}
