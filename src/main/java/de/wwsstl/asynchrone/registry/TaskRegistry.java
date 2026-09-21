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
 * <p>Die Schnittstelle kennt bewusst <b>keine</b> Lösch-Operation. Beendete Tasks (abgeschlossen oder abgebrochen)
 * bleiben mit ihrem Endzustand abfragbar; nur ihre Threads sind dann beendet. Hinter dieser Abstraktion kann
 * später ein verteilter Cache (Redis) stehen.
 */
public interface TaskRegistry {

    /**
     * @throws IllegalStateException wenn zu dieser Task-ID bereits ein Eintrag existiert
     */
    void register(Sandbox sandbox);

    Optional<Sandbox> find(UUID taskId);

    /** Alle jemals registrierten Tasks. */
    Collection<Sandbox> all();

    int size();
}
