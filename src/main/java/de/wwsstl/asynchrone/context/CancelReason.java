package de.wwsstl.asynchrone.context;

/** Grund, aus dem ein Task abgebrochen wurde. */
public enum CancelReason {
    /** Abbruch von außen über die REST-API. */
    USER_REQUEST,
    /** Fehlerschwellenwert überschritten. */
    ERROR_THRESHOLD,
    /** Maximale Laufzeit überschritten. */
    TIMEOUT,
    /** Unerwarteter Fehler in Producer oder Consumer. */
    INTERNAL_ERROR,
    /** Die Anwendung wird beendet. */
    SHUTDOWN
}
