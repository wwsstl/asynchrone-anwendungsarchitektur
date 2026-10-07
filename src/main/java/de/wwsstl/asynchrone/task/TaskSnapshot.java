package de.wwsstl.asynchrone.task;

import java.time.Instant;

/**
 * Lokaler Stand einer laufenden Aufgabe (funktionsweise_sequenz.md, Abschnitt 4). Er enthält nur Ergebnisse, die der
 * Consumer bereits verarbeitet hat, und kann den Zahlen der Cloud etwas hinterherhinken.
 *
 * @param taskNumber Aufgabennummer, zugleich die Nummer des BatchgenAuftrags
 * @param finishedAt Zeitpunkt des Endzustands; {@code null}, solange die Aufgabe läuft
 * @param submitted  Dateien, für die Cloud-API 1 eine TaskId geliefert hat
 * @param succeeded  Dateien, die in die {@code donebox} verschoben wurden
 * @param failed     Stand des Fehlerzählers: nicht umgewandelte, fehlerhafte und unlesbare Dateien
 * @param pending    Dateien im Status-Pool, deren Ergebnis noch aussteht
 * @param unclear    Dateien, deren Übermittlungsergebnis unklar ist; sie bleiben ohne Marker in der {@code pendingbox}
 */
public record TaskSnapshot(
        String taskNumber,
        String userId,
        TaskState state,
        Instant startedAt,
        Instant finishedAt,
        int submitted,
        int succeeded,
        int failed,
        int pending,
        int unclear) {
}
