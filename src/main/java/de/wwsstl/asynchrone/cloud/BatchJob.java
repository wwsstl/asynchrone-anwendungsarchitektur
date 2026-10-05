package de.wwsstl.asynchrone.cloud;

/** Ein BatchgenAuftrag laut Cloud-API 3: Nummer, zugehörige Benutzer:in und Status. */
public record BatchJob(BatchJobId id, String userId, BatchJobStatus status) {
}
