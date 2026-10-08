package de.wwsstl.asynchrone.cloud;

/**
 * Ein Aufruf von Cloud-API 1 (BatchgenAuftrag anlegen oder Batch übermitteln) ist fehlgeschlagen. Beide Aufrufe sind
 * nicht idempotent; die Art des Fehlers gibt an, ob die Cloud die Anfrage verarbeitet haben kann.
 */
public class SubmitFailedException extends RuntimeException {

    /** Art des Fehlers; bestimmt, wie es weitergeht (funktionsweise_sequenz.md, Abschnitte 1 und 2). */
    public enum Kind {
        /**
         * Eindeutig nicht verarbeitet (Verbindungsfehler, Anfrage abgelehnt): Start abweisen bzw. Batch erneut
         * übermitteln.
         */
        NOT_PROCESSED,
        /**
         * Ergebnis unklar (Timeout, keine Antwort, sonstige 5xx): nicht wiederholen; der Start bzw. die Aufgabe endet
         * mit TIMEOUT.
         */
        UNKNOWN
    }

    private final Kind kind;

    public SubmitFailedException(Kind kind, Throwable cause) {
        super("Aufruf von Cloud-API 1 fehlgeschlagen (" + kind + "): " + cause, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }
}
