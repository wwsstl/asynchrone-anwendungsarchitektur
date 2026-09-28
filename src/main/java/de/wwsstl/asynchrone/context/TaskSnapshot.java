package de.wwsstl.asynchrone.context;

import java.time.Instant;
import java.util.UUID;

/**
 * Unveränderliche Momentaufnahme eines Tasks für die REST-API.
 *
 * @param submitted       Dateien, für die Cloud-API 1 in diesem Task eine TaskId geliefert hat
 * @param resumed         bereits von einem früheren Task übermittelte Dateien ({@code pendingbox}), deren
 *                        Statusabfrage dieser Task ohne erneute Übermittlung wieder aufgenommen hat
 * @param succeeded       Dateien, die nach {@code donebox} verschoben wurden
 * @param failed          Dateien, die fehlerhaft waren (nach {@code errorbox} verschoben oder nicht verschiebbar)
 * @param pending         TaskIds, die im Status-Pool noch auf einen Endzustand warten
 * @param abandoned       übermittelte Dateien, die bei einem Abbruch noch nicht entschieden waren; sie verbleiben
 *                        samt TaskId in der {@code pendingbox} und werden vom nächsten Task weiter abgefragt
 */
public record TaskSnapshot(
        String userId,
        UUID taskId,
        TaskState state,
        CancelReason cancelReason,
        Instant startedAt,
        boolean producerFinished,
        int submitted,
        int resumed,
        int succeeded,
        int failed,
        int pending,
        int abandoned) {
}
