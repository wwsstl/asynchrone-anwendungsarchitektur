package de.wwsstl.asynchrone.taskmanager;

import java.util.UUID;

import de.wwsstl.asynchrone.context.TaskState;

public class TaskAlreadyFinishedException extends RuntimeException {

    public TaskAlreadyFinishedException(UUID taskId, TaskState state) {
        super("Task " + taskId + " ist bereits beendet (" + state + ")");
    }
}
