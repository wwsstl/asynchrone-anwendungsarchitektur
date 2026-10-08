package de.wwsstl.asynchrone.cloud;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** Kurze Beschreibung fehlgeschlagener Aufrufe der Cloud-Dienste für den Grund eines Endzustands. */
public final class CloudErrors {

    private CloudErrors() {
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
