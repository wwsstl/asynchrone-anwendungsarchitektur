package de.wwsstl.asynchrone.context;

/**
 * Status einer Aufgabe, wie ihn die REST-API meldet. {@code COMPLETED} und {@code CANCELLED} sind Endzustände.
 *
 * <p>{@code CANCELLING} wird nirgends gespeichert und nie von einer Sandbox angenommen: Die Statusabfrage leitet ihn
 * ab, solange der Lauf noch {@code RUNNING} ist, Cloud-API 3 für den BatchgenAuftrag aber bereits
 * {@code CANCELLED} meldet (funktionsweise_sequenz.md, Abschnitt 4).
 */
public enum TaskState {
    RUNNING,
    CANCELLING,
    COMPLETED,
    CANCELLED
}
