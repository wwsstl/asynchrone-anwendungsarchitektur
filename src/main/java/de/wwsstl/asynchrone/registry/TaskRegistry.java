package de.wwsstl.asynchrone.registry;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

import de.wwsstl.asynchrone.context.Sandbox;

/**
 * Zentrales Aufgabenregister (loesung_final.md 4.3). Es lebt als Singleton über die gesamte Laufzeit der
 * Anwendung und enthält je lebender Sandbox genau einen Eintrag: Bei {@code n} laufenden Tasks liefert
 * {@link #size()} {@code n}.
 *
 * <p>Das Register selbst entfernt nichts; das übernimmt der {@code TaskManager} per {@link #remove(UUID)} — beim
 * externen Abbruch oder automatisch, sobald beide Threads eines Tasks ausgelaufen sind. Im zweiten Fall wandert
 * der Endzustand vorher in die {@link TaskHistory}. Hinter dieser Abstraktion kann später ein verteilter Cache
 * (Redis) stehen.
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

    /** Alle derzeit registrierten Tasks. */
    Collection<Sandbox> all();

    int size();
}
