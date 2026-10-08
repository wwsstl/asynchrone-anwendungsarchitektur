package de.wwsstl.asynchrone.task;

import de.wwsstl.asynchrone.cloud.JobStatus;

/**
 * Lokaler Zustand einer Aufgabe. Jeder Zustand außer {@code RUNNING} ist ein Endzustand; die Endzustände der
 * Anwendung schreibt der TaskManager über Cloud-API 3 in den BatchgenAuftrag (funktionsweise_sequenz.md, Abschnitte 5
 * und 6).
 */
public enum TaskState {
    RUNNING(null),
    /** Der Producer ist fertig (alle Dateien der inbox sind übermittelt), und der Status-Pool ist leer. */
    COMPLETED(JobStatus.COMPLETED),
    /**
     * Die maximale Laufzeit ({@code task-timeout}) ist überschritten, oder das Übermitteln eines Batches an Cloud-API 1
     * hatte ein unklares Ergebnis (z. B. keine Antwort innerhalb von {@code submit-timeout}).
     */
    TIMEOUT(JobStatus.TIMEOUT),
    /** Der Fehlerzähler hat {@code error-threshold} erreicht. */
    ERROR(JobStatus.ERROR),
    /** Abbruch von außen über die REST-API. */
    TERMINATED(JobStatus.TERMINATED),
    /**
     * Die Anwendung wird beendet oder ein unerwarteter Fehler hat die Sandbox gestoppt. Wie bei einem Absturz wird
     * kein Endzustand geschrieben; der BatchgenAuftrag bleibt RUNNING und muss manuell korrigiert werden.
     */
    ABORTED(null);

    private final JobStatus jobStatus;

    TaskState(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    /** Der Status, den der TaskManager in den BatchgenAuftrag schreibt; {@code null}, wenn nichts geschrieben wird. */
    public JobStatus jobStatus() {
        return jobStatus;
    }
}
