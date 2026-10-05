package de.wwsstl.asynchrone.registry;

import de.wwsstl.asynchrone.cloud.BatchJobId;

/**
 * Ein Lauf im {@link RunRegistry Laufregister}. Er entsteht mit {@link RunRegistry#claimUser} im Status
 * {@code STARTING}, in dem nur die Benutzer:in belegt ist, und wechselt mit {@link RunRegistry#claimTask} nach
 * {@code RUNNING}. Erst dann hat er Aufgabennummer und Laufnummer.
 */
public final class Run {

    private final String userId;
    private volatile BatchJobId jobId;
    private volatile int number;

    Run(String userId) {
        this.userId = userId;
    }

    public String userId() {
        return userId;
    }

    /** Aufgabennummer; {@code null}, solange der Lauf im Status {@code STARTING} ist. */
    public BatchJobId jobId() {
        return jobId;
    }

    /** Laufnummer unter der Aufgabennummer (1, 2, …); {@code 0}, solange der Lauf im Status {@code STARTING} ist. */
    public int number() {
        return number;
    }

    void assign(BatchJobId jobId, int number) {
        this.jobId = jobId;
        this.number = number;
    }

    @Override
    public String toString() {
        return jobId == null ? "Lauf von " + userId + " (STARTING)" : "Lauf " + jobId + "#" + number + " von " + userId;
    }
}
