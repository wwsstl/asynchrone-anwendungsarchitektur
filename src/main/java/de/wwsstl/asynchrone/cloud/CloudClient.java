package de.wwsstl.asynchrone.cloud;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Nicht-blockierender Zugriff auf die Cloud-Dienste. Alle Aufrufe kehren sofort zurück; das Ergebnis liefert das
 * {@link CompletableFuture}.
 */
public interface CloudClient {

    /**
     * Cloud-API 1: legt für die Benutzer:in einen BatchgenAuftrag (Status RUNNING) an und liefert seine Nummer.
     *
     * <p>Schlägt der Aufruf fehl, endet das Future wie bei {@link #submit} mit einer {@link SubmitFailedException}.
     */
    CompletableFuture<String> createBatchJob(String userId);

    /**
     * Cloud-API 1: übermittelt einen Batch zum BatchgenAuftrag. Die Antwort enthält je erfolgreich umgewandelter Datei
     * die TaskId; Dateien ohne TaskId wurden nicht umgewandelt.
     *
     * <p>Schlägt der Aufruf fehl, endet das Future mit einer {@link SubmitFailedException}, die angibt, ob der Batch
     * eindeutig nicht verarbeitet wurde oder ob das Ergebnis unklar ist. Der Aufruf wird nie wiederholt, weil Cloud-API 1
     * nicht idempotent ist.
     */
    CompletableFuture<List<SubmittedFile>> submit(String jobId, List<TestdataItem> items);

    /** Cloud-API 2: Status mehrerer Datensätze in einer Anfrage. */
    CompletableFuture<List<RecordStatusResult>> queryStatus(List<TaskId> taskIds);

    /** Cloud-API 3: setzt den Status des BatchgenAuftrags (Endzustand). */
    CompletableFuture<Void> setJobStatus(String jobId, JobStatus status);

    /** Cloud-API 3: Status des BatchgenAuftrags und Zahl seiner Datensätze je Status; leer, wenn es ihn nicht gibt. */
    CompletableFuture<Optional<BatchJob>> batchJob(String jobId);
}
