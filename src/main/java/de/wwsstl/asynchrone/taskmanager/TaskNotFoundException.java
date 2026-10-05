package de.wwsstl.asynchrone.taskmanager;

public class TaskNotFoundException extends RuntimeException {

    public TaskNotFoundException(String userId, String taskId) {
        super("Task " + taskId + " von Benutzer '" + userId + "' existiert nicht");
    }
}
