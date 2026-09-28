package de.wwsstl.asynchrone.taskmanager;

import java.util.UUID;

/**
 * Ein Benutzer hat nur eine {@code inbox} und {@code pendingbox}; zwei gleichzeitige Tasks würden dieselben Dateien
 * doppelt verarbeiten. Gilt auch für einen abgebrochenen Task, dessen Threads noch nicht ausgelaufen sind.
 */
public class TaskAlreadyRunningException extends RuntimeException {

    private final UUID runningTaskId;

    public TaskAlreadyRunningException(String userId, UUID runningTaskId) {
        super("Für Benutzer '" + userId + "' ist Task " + runningTaskId + " noch aktiv");
        this.runningTaskId = runningTaskId;
    }

    public UUID runningTaskId() {
        return runningTaskId;
    }
}
