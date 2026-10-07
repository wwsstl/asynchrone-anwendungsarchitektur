package de.wwsstl.asynchrone.cloud;

/**
 * Ein BatchgenAuftrag laut Cloud-API 3: Status und die Zahl seiner Datensätze je Status.
 *
 * @param jobId Nummer des BatchgenAuftrags, zugleich die Aufgabennummer
 */
public record BatchJob(String jobId, String userId, JobStatus status, int success, int error, int pending) {
}
