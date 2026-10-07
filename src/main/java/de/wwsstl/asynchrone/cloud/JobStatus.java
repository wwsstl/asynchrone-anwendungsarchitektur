package de.wwsstl.asynchrone.cloud;

/**
 * Status eines BatchgenAuftrags. Die Cloud legt ihn mit {@code RUNNING} an; die Endzustände schreibt die Anwendung über
 * Cloud-API 3.
 */
public enum JobStatus {
    RUNNING,
    COMPLETED,
    TIMEOUT,
    ERROR,
    TERMINATED
}
