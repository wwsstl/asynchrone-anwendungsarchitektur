package de.wwsstl.asynchrone.cloud;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** Kurze Beschreibungen von Fehlern der Cloud-Dienste für Gründe und Protokolle. */
public final class CloudErrors {

    /** Steht im Grund, wenn die Cloud zu einer abgelehnten oder fehlerhaften Datei keine Fehlermeldung liefert. */
    static final String NO_FEHLERMELDUNG = "keine Fehlermeldung der Cloud";

    private CloudErrors() {
    }

    /**
     * Die Fehlermeldung der Cloud (Feld {@code fehlermeldung}) ohne Leerzeichen am Rand; fehlt sie oder ist sie leer,
     * ein Hinweis darauf.
     */
    public static String fehlermeldung(String fehlermeldung) {
        return fehlermeldung == null || fehlermeldung.isBlank() ? NO_FEHLERMELDUNG : fehlermeldung.strip();
    }

    /**
     * Ursache eines fehlgeschlagenen Aufrufs, z. B. {@code keine Antwort innerhalb von submit-timeout} oder
     * {@code 500 Internal Server Error from POST http://…/tasks}. {@link ExecutionException} und
     * {@link SubmitFailedException} werden ausgepackt.
     *
     * @param timeoutProperty der für den Aufruf geltende Timeout, z. B. {@code submit-timeout}
     */
    public static String describe(Throwable error, String timeoutProperty) {
        Throwable cause = error;
        while ((cause instanceof ExecutionException || cause instanceof SubmitFailedException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof TimeoutException) {
            return "keine Antwort innerhalb von " + timeoutProperty;
        }
        if (cause == null) {
            return "unbekannte Ursache";
        }
        return cause.getMessage() != null ? cause.getMessage() : cause.toString();
    }
}
