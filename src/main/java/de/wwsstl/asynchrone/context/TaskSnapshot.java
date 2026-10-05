package de.wwsstl.asynchrone.context;

import java.time.Instant;

/**
 * Unveränderliche Momentaufnahme eines Laufs für die REST-API und das Laufregister.
 *
 * @param taskId          Aufgabennummer, zugleich die Nummer des BatchgenAuftrags
 * @param run             Laufnummer unter dieser Aufgabennummer (1, 2, …)
 * @param finishedAt      Zeitpunkt des Endzustands; {@code null}, solange der Lauf läuft. Ab
 *                        {@code finishedAt + pipeline.task-retention} wird der Lauf ignoriert
 * @param submitted       Dateien, für die Cloud-API 1 in diesem Lauf eine TaskId geliefert hat
 * @param resumed         bereits von einem früheren Lauf übermittelte Dateien ({@code pendingbox}), deren
 *                        Statusabfrage dieser Lauf ohne erneute Übermittlung wieder aufgenommen hat
 * @param succeeded       Dateien, die nach {@code donebox} verschoben wurden
 * @param failed          Dateien, die fehlerhaft waren (nach {@code errorbox} verschoben oder nicht verschiebbar)
 * @param pending         TaskIds, die im Status-Pool noch auf einen Endzustand warten
 * @param abandoned       übermittelte Dateien, die bei einem Abbruch noch nicht entschieden waren; sie verbleiben
 *                        samt TaskId in der {@code pendingbox} und werden beim Fortsetzen weiter abgefragt
 */
public record TaskSnapshot(
        String userId,
        String taskId,
        int run,
        TaskState state,
        CancelReason cancelReason,
        Instant startedAt,
        Instant finishedAt,
        boolean producerFinished,
        int submitted,
        int resumed,
        int succeeded,
        int failed,
        int pending,
        int abandoned) {

    /** Dieselbe Momentaufnahme mit einem anderen Status, etwa dem abgeleiteten {@link TaskState#CANCELLING}. */
    public TaskSnapshot withState(TaskState newState) {
        return new TaskSnapshot(userId, taskId, run, newState, cancelReason, startedAt, finishedAt, producerFinished,
                submitted, resumed, succeeded, failed, pending, abandoned);
    }
}
