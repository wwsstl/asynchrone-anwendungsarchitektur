package de.wwsstl.asynchrone.cloud;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import de.wwsstl.asynchrone.pool.TaskId;

/**
 * Nicht-blockierender Zugriff auf die Cloud-Dienste. Alle Aufrufe kehren sofort zurück; das Ergebnis liefert das
 * {@link CompletableFuture}.
 */
public interface CloudClient {

    /** Cloud-API 1: legt für die Benutzer:in einen BatchgenAuftrag an und liefert dessen Nummer. */
    CompletableFuture<BatchJobId> createBatchJob(String userId);

    /** Cloud-API 1: übermittelt einen Batch zum BatchgenAuftrag; liefert je umgewandelter Datei die TaskId. */
    CompletableFuture<List<SubmittedTask>> submit(BatchJobId job, List<TestdataItem> items);

    /** Cloud-API 2: Bulk-Statusabfrage für mehrere TaskIds in einem Aufruf. */
    CompletableFuture<List<TaskStatusResult>> queryStatus(List<TaskId> taskIds);

    /** Cloud-API 3: der BatchgenAuftrag der Benutzer:in im Status {@code RUNNING}; leer, wenn es keinen gibt. */
    CompletableFuture<Optional<BatchJobId>> findRunningBatchJob(String userId);

    /** Cloud-API 3: ein BatchgenAuftrag; leer, wenn es ihn nicht gibt. */
    CompletableFuture<Optional<BatchJob>> batchJob(BatchJobId job);

    /** Cloud-API 3: setzt den Status eines BatchgenAuftrags. */
    CompletableFuture<Void> setBatchJobStatus(BatchJobId job, BatchJobStatus status);
}
