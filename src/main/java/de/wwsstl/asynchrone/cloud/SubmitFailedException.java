package de.wwsstl.asynchrone.cloud;

/** Der Aufruf von Cloud-API 1 zum Übermitteln eines Batches ist fehlgeschlagen. */
public class SubmitFailedException extends RuntimeException {

    /** Art des Fehlers; bestimmt, was mit den Dateien des Batches geschieht (funktionsweise_sequenz.md, Abschnitt 2). */
    public enum Kind {
        /** Eindeutig nicht verarbeitet (Verbindungsfehler, Anfrage abgelehnt): erneut übermitteln. */
        NOT_PROCESSED,
        /** Ergebnis unklar (Timeout, keine Antwort): nicht erneut übermitteln. */
        UNKNOWN
    }

    private final Kind kind;

    public SubmitFailedException(Kind kind, Throwable cause) {
        super("Übermittlung an Cloud-API 1 fehlgeschlagen (" + kind + "): " + cause, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }
}
