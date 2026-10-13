package de.wwsstl.asynchrone.cloud;

/**
 * Ein BatchgenAuftrag laut Cloud-API 3: Status und die Zahl seiner Datensätze je Status.
 *
 * @param jobId   Nummer des BatchgenAuftrags, zugleich die Aufgabennummer
 * @param jobType Art des BatchgenAuftrags, wie die Cloud sie liefert
 * @param jobName Name des BatchgenAuftrags
 */
public record BatchJob(String jobId, String userId, String jobType, String jobName, JobStatus status, int success,
        int error, int pending) {
}
