package de.wwsstl.asynchrone.cloud;

/** Ein Aufruf eines Cloud-Dienstes ist gescheitert (nicht erreichbar, Zeitüberschreitung, Fehlerantwort). */
public class CloudCallFailedException extends RuntimeException {

    public CloudCallFailedException(String call, Throwable cause) {
        super(call + " fehlgeschlagen: " + cause, cause);
    }
}
