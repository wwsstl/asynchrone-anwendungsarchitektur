package de.wwsstl.asynchrone.context;

/** Grund, aus dem ein Task abgebrochen wurde. */
public enum CancelReason {
    /** Der BatchgenAuftrag wurde von außen über Cloud-API 3 abgebrochen (REST-API oder manuell). */
    USER_REQUEST,
    /** Fehlerschwellenwert erreicht. */
    ERROR_THRESHOLD,
    /** Maximale Laufzeit überschritten. */
    TIMEOUT,
    /** Unerwarteter Fehler in Producer oder Consumer. */
    INTERNAL_ERROR,
    /** Die Anwendung wird beendet. */
    SHUTDOWN
}
