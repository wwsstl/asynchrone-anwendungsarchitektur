package de.wwsstl.asynchrone.taskmanager;

import de.wwsstl.asynchrone.context.TaskState;

/**
 * Antwort auf einen Abbruch: Der BatchgenAuftrag ist als {@code CANCELLED} markiert, der Task beendet sich
 * spätestens nach einem {@code sweep-interval}. Bis dahin meldet die Statusabfrage {@link TaskState#CANCELLING}.
 */
public record CancelResult(String taskId, TaskState state) {
}
