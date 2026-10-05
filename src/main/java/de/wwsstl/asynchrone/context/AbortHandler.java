package de.wwsstl.asynchrone.context;

/**
 * Empfänger der Abbruchmeldung einer Sandbox bei Zeitüberschreitung oder Fehlerschwelle (funktionsweise_sequenz.md,
 * Abschnitt 6). Der {@code TaskManager} markiert daraufhin den BatchgenAuftrag über Cloud-API 3 als
 * {@code CANCELLED}; der Consumer erkennt das im nächsten Durchlauf, und die Sandbox beendet sich.
 */
@FunctionalInterface
public interface AbortHandler {

    /**
     * @return {@code true}, wenn der BatchgenAuftrag als {@code CANCELLED} markiert wurde; {@code false}, wenn
     *         Cloud-API 3 nicht erreichbar war
     */
    boolean abort(TaskContext context, CancelReason reason);
}
