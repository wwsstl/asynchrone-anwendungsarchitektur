package de.wwsstl.asynchrone.taskmanager;

import java.util.UUID;

public class TaskNotFoundException extends RuntimeException {

    public TaskNotFoundException(String userId, UUID taskId) {
        super("Task " + taskId + " von Benutzer '" + userId + "' existiert nicht");
    }
}
