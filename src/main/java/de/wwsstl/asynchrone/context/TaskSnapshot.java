package de.wwsstl.asynchrone.context;

import java.time.Instant;
import java.util.UUID;

/**
 * Unveränderliche Momentaufnahme eines Tasks für die REST-API.
 *
 * @param submitted       Dateien, für die Cloud-API 1 eine TaskId geliefert hat
 * @param succeeded       Dateien, die nach {@code donebox} verschoben wurden
 * @param failed          Dateien, die fehlerhaft waren (nach {@code errorbox} verschoben oder nicht verschiebbar)
 * @param pending         TaskIds, die im Status-Pool noch auf einen Endzustand warten
 * @param abandoned       Dateien, die bei einem Abbruch noch nicht entschieden waren (verbleiben in {@code inbox})
 * @param inFlightBatches Batches, auf deren Antwort von Cloud-API 1 noch gewartet wird
 */
public record TaskSnapshot(
        String userId,
        UUID taskId,
        TaskState state,
        CancelReason cancelReason,
        Instant startedAt,
        boolean producerFinished,
        int submitted,
        int succeeded,
        int failed,
        int pending,
        int abandoned,
        int inFlightBatches) {
}
