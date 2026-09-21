package de.wwsstl.asynchrone.registry;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

import de.wwsstl.asynchrone.context.Sandbox;

/**
 * Zentrales Aufgabenregister (loesung_final.md 4.3). Es lebt als Singleton über die gesamte Laufzeit der
 * Anwendung und enthält je Task genau einen Eintrag: Nach {@code n} gestarteten Tasks liefert {@link #size()}
 * {@code n}.
 *
 * <p>Tasks verschwinden nie von selbst: Ein abgeschlossener oder abgebrochener Task bleibt mit seinem Endzustand
 * abfragbar (nur seine Threads sind dann beendet), bis er ausdrücklich per {@link #remove(UUID)} gelöscht wird —
 * das geschieht ausschließlich durch den externen Abbruch. Hinter dieser Abstraktion kann später ein verteilter
 * Cache (Redis) stehen.
 */
public interface TaskRegistry {

    /**
     * @throws IllegalStateException wenn zu dieser Task-ID bereits ein Eintrag existiert
     */
    void register(Sandbox sandbox);

    Optional<Sandbox> find(UUID taskId);

    /**
     * Löscht den Task aus dem Register. Das Beenden seiner Threads ist nicht Sache des Registers.
     *
     * @return den gelöschten Task; leer, wenn er nicht (mehr) registriert war
     */
    Optional<Sandbox> remove(UUID taskId);

    /** Alle jemals registrierten Tasks. */
    Collection<Sandbox> all();

    int size();
}
