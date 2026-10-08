package de.wwsstl.asynchrone.cloud;

/**
 * Das Ergebnis eines Aufrufs von Cloud-API 1 ist unklar, z. B. weil innerhalb von {@code submit-timeout} keine Antwort
 * kam. Beim Start wird wie bei einer Aufgabe mit unklarem Ergebnis nichts weiter unternommen: Es wird keine Sandbox
 * angelegt, und der Start wird mit dem Ursachencode {@code TIMEOUT} abgewiesen.
 */
public class CloudTimeoutException extends RuntimeException {

    /** @param reason welcher Aufruf welcher Cloud-API fehlgeschlagen ist und warum */
    public CloudTimeoutException(String reason) {
        super(reason);
    }
}
