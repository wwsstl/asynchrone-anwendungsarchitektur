package de.wwsstl.asynchrone.task;

import de.wwsstl.asynchrone.cloud.BatchJob;
import de.wwsstl.asynchrone.cloud.JobStatus;

/**
 * Antwort der Statusabfrage (funktionsweise_sequenz.md, Abschnitt 4).
 *
 * @param cloud Status des BatchgenAuftrags und seiner Datensätze aus der Cloud; {@code null}, wenn die Cloud gerade
 *              nicht erreichbar ist, die Aufgabe aber lokal noch läuft
 * @param local lokaler Stand aus der Sandbox; {@code null}, sobald die Aufgabe beendet und abgemeldet ist
 */
public record TaskStatus(String taskNumber, String userId, Cloud cloud, TaskSnapshot local) {

    /** Status des BatchgenAuftrags und die Zahl seiner Datensätze je Status. */
    public record Cloud(JobStatus status, int success, int error, int pending) {

        static Cloud of(BatchJob job) {
            return new Cloud(job.status(), job.success(), job.error(), job.pending());
        }
    }
}
