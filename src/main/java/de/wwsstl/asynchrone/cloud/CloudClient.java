package de.wwsstl.asynchrone.cloud;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import de.wwsstl.asynchrone.pool.TaskId;

/**
 * Nicht-blockierender Zugriff auf die beiden Cloud-Dienste. Beide Aufrufe kehren sofort zurück; das Ergebnis
 * liefert das {@link CompletableFuture}.
 */
public interface CloudClient {

    /** Cloud-API 1: übermittelt einen Batch; liefert je Datei die vergebene TaskId. */
    CompletableFuture<List<SubmittedTask>> submit(List<TestdataItem> items);

    /** Cloud-API 2: Bulk-Statusabfrage für mehrere TaskIds in einem Aufruf. */
    CompletableFuture<List<TaskStatusResult>> queryStatus(List<TaskId> taskIds);
}
