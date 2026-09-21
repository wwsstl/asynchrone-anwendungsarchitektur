package de.wwsstl.asynchrone.context;

/** Lebenszyklus eines Tasks. {@code COMPLETED} und {@code CANCELLED} sind Endzustände. */
public enum TaskState {
    RUNNING,
    COMPLETED,
    CANCELLED
}
