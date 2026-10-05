package de.wwsstl.asynchrone.registry;

import java.util.Collection;
import java.util.Optional;

import de.wwsstl.asynchrone.cloud.BatchJobId;
import de.wwsstl.asynchrone.context.Sandbox;

/**
 * Lokales Aufgabenregister einer Instanz (loesung_final.md 4.3): je lebender Sandbox genau ein Eintrag unter ihrer
 * Aufgabennummer. Bei {@code n} laufenden Tasks liefert {@link #size()} {@code n}.
 *
 * <p>Das Register hält nur, was sich nicht teilen lässt: Threads, Status-Pool und Kontext. Belegungen, Laufnummern
 * und Endzustände stehen im {@link RunRegistry Laufregister} (funktionsweise_sequenz.md, Abschnitt 7).
 *
 * <p>Das Register selbst entfernt nichts; das übernimmt der {@code TaskManager} per {@link #remove(Sandbox)},
 * sobald beide Threads einer Sandbox ausgelaufen sind und ihr Endzustand im Laufregister liegt.
 */
public interface TaskRegistry {

    /**
     * Trägt die Sandbox unter ihrer Aufgabennummer ein. Ein Eintrag derselben Nummer, dessen Lauf bereits beendet
     * ist, wird ersetzt: Seine Abmeldung kann noch ausstehen, während der nächste Lauf schon startet.
     *
     * @throws IllegalStateException wenn unter dieser Aufgabennummer noch ein Lauf aktiv ist
     */
    void register(Sandbox sandbox);

    Optional<Sandbox> find(BatchJobId jobId);

    /**
     * Löscht genau diese Sandbox aus dem Register; ein inzwischen eingetragener Nachfolger bleibt erhalten. Das
     * Beenden ihrer Threads ist nicht Sache des Registers.
     *
     * @return {@code true}, wenn die Sandbox registriert war
     */
    boolean remove(Sandbox sandbox);

    /** Alle derzeit registrierten Sandboxen. */
    Collection<Sandbox> all();

    int size();
}
