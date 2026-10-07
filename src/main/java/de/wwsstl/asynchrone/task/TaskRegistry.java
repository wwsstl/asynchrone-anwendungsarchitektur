package de.wwsstl.asynchrone.task;

import java.util.Collection;
import java.util.Optional;

/**
 * Zentrales Aufgabenregister: je laufender Sandbox ein Eintrag unter ihrer Aufgabennummer. Es lebt als Singleton über
 * die gesamte Laufzeit; bei {@code n} laufenden Aufgaben liefert {@link #size()} {@code n}.
 *
 * <p>Die Abstraktion erlaubt es, das Register später in einen verteilten Cache zu überführen.
 */
public interface TaskRegistry {

    /**
     * @throws IllegalStateException wenn unter dieser Aufgabennummer bereits eine Sandbox eingetragen ist
     */
    void register(Sandbox sandbox);

    Optional<Sandbox> find(String taskNumber);

    /** Entfernt genau diese Sandbox. */
    void remove(Sandbox sandbox);

    Collection<Sandbox> all();

    int size();
}
