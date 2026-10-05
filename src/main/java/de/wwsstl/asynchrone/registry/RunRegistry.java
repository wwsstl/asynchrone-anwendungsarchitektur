package de.wwsstl.asynchrone.registry;

import java.time.Instant;
import java.util.Optional;

import de.wwsstl.asynchrone.cloud.BatchJobId;
import de.wwsstl.asynchrone.context.TaskSnapshot;

/**
 * Laufregister (funktionsweise_sequenz.md, Abschnitt 7): der Zustand, der auf jeder Instanz gleich sein muss. Dazu
 * gehören die belegten Benutzer:innen und Aufgabennummern, die Laufnummern und die Endzustände der Läufe.
 *
 * <p>Es bietet nur atomare Operationen an; Prüfen und Belegen sind nie getrennte Schritte. Ein Lauf hat die Status
 * {@code STARTING} (nach {@link #claimUser}), {@code RUNNING} (nach {@link #claimTask}) sowie {@code COMPLETED}
 * oder {@code CANCELLED} (nach {@link #finish}).
 *
 * <p>Phase 1 ist eine In-Memory-Implementierung. In Phase 2 steht dahinter eine Datenbanktabelle mit partiellen
 * Unique-Indizes auf Benutzer:in und Aufgabennummer, ergänzt um eine Lease.
 */
public interface RunRegistry {

    /**
     * Start, Schritt 1: belegt die Benutzer:in mit einem neuen Lauf im Status {@code STARTING}.
     *
     * @return den Lauf; leer, wenn die Benutzer:in bereits einen Lauf in {@code STARTING} oder {@code RUNNING} hat
     */
    Optional<Run> claimUser(String userId);

    /**
     * Start, Schritt 3: belegt die Aufgabennummer für den Lauf, vergibt die nächste Laufnummer und setzt ihn auf
     * {@code RUNNING}. Gezählt werden die Läufe dieser Aufgabennummer, die noch im Register stehen.
     *
     * @return {@code false}, wenn unter dieser Aufgabennummer bereits ein Lauf {@code RUNNING} ist
     * @throws IllegalStateException wenn der Lauf die Benutzer:in nicht (mehr) belegt oder schon eine Nummer hat
     */
    boolean claimTask(Run run, BatchJobId jobId);

    /** Gibt einen Lauf frei, ohne einen Endzustand abzulegen (Start abgewiesen oder gescheitert). */
    void release(Run run);

    /**
     * Legt den Endzustand des Laufs unter Aufgabennummer und Laufnummer ab und gibt danach Benutzer:in und
     * Aufgabennummer frei.
     *
     * @throws IllegalArgumentException wenn die Momentaufnahme keinen Endzeitpunkt hat
     */
    void finish(Run run, TaskSnapshot finalSnapshot);

    /** Der Endzustand des letzten beendeten Laufs dieser Aufgabennummer. */
    Optional<TaskSnapshot> latestFinished(BatchJobId jobId);

    /** Der Endzustand eines bestimmten Laufs. */
    Optional<TaskSnapshot> finished(BatchJobId jobId, int run);

    /**
     * Entfernt die Endzustände aller Läufe, die spätestens zum Zeitpunkt {@code cutoff} beendet wurden.
     *
     * @return Anzahl der entfernten Endzustände
     */
    int removeFinishedUntil(Instant cutoff);
}
