package de.wwsstl.asynchrone.task;

/** Ursachencode eines mit {@code 409 Conflict} abgewiesenen Starts. */
public enum RejectReason {
    /** Für die Benutzer:in läuft bereits eine Aufgabe. */
    USER_TASK_RUNNING,
    /** In der {@code pendingbox} liegen noch Restdateien einer früheren Aufgabe (funktionsweise_sequenz.md, Abschnitt 7). */
    PENDINGBOX_NOT_EMPTY
}
