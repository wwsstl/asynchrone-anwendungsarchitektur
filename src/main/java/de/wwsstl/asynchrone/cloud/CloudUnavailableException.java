package de.wwsstl.asynchrone.cloud;

/** Ein Cloud-Dienst war nicht erreichbar, hat einen Fehler gemeldet oder nicht rechtzeitig geantwortet. */
public class CloudUnavailableException extends RuntimeException {

    public CloudUnavailableException(String operation, Throwable cause) {
        super(operation + " fehlgeschlagen: " + cause, cause);
    }
}
