package de.wwsstl.asynchrone.taskmanager;

/**
 * Ursachencode einer mit {@code 409 Conflict} abgewiesenen Anfrage. Er steht in der Antwort der REST-API, damit die
 * Oberfläche je Ursache einen passenden Hinweis anzeigen kann (funktionsweise_sequenz.md, Abschnitt 1).
 */
public enum RejectReason {
    /** Die Benutzer:in hat bereits einen Lauf in {@code STARTING} oder {@code RUNNING}. */
    USER_TASK_RUNNING,
    /** Unter der Aufgabennummer läuft bereits ein Lauf. */
    TASK_ALREADY_RUNNING,
    /**
     * Kein laufender BatchgenAuftrag, aber Dateien in der {@code pendingbox}: Die Benutzer:in setzt zuerst fort (a)
     * oder leert die {@code pendingbox} (b).
     */
    PENDINGBOX_NOT_EMPTY,
    /** Abbruch einer Aufgabe, deren BatchgenAuftrag bereits abgeschlossen ist. */
    TASK_NOT_RUNNING
}
