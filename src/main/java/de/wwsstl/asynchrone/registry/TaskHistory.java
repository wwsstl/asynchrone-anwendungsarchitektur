package de.wwsstl.asynchrone.registry;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import de.wwsstl.asynchrone.context.TaskSnapshot;

/**
 * Historie beendeter Tasks. Sobald beide Threads eines Tasks ausgelaufen sind, meldet der {@code TaskManager} ihn
 * ab: Die Sandbox (Threads, Status-Pool, Kontext) verlässt das {@link TaskRegistry}, und nur ihre letzte
 * Momentaufnahme wird hier abgelegt — damit bleibt der Task über die Status-Abfrage mit seinem Endzustand
 * abfragbar, ohne dass seine Laufzeitobjekte weiter Speicher belegen.
 *
 * <p>Einträge entfernt der {@code TaskManager} nach Ablauf von {@code pipeline.task-retention} oder beim externen
 * Abbruch. Hinter dieser Abstraktion kann später ein verteilter Speicher stehen (Redis mit TTL, DB-Tabelle).
 */
public interface TaskHistory {

    /**
     * Legt den Endzustand eines Tasks ab.
     *
     * @throws IllegalArgumentException wenn der Task noch keinen Endzeitpunkt hat
     */
    void record(TaskSnapshot snapshot);

    Optional<TaskSnapshot> find(UUID taskId);

    /** @return den gelöschten Endzustand; leer, wenn es keinen (mehr) gab */
    Optional<TaskSnapshot> remove(UUID taskId);

    /**
     * Entfernt alle Einträge, die spätestens zum Zeitpunkt {@code cutoff} beendet wurden.
     *
     * @return Anzahl der entfernten Einträge
     */
    int removeFinishedUntil(Instant cutoff);

    int size();
}
