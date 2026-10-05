package de.wwsstl.asynchrone.cloud;

import java.util.regex.Pattern;

/**
 * Nummer eines BatchgenAuftrags im Cloud-Dienst. Sie ist zugleich die Aufgabennummer, unter der die REST-API eine
 * Aufgabe anspricht (funktionsweise_sequenz.md, Abschnitt 1). Erlaubt sind daher nur Zeichen, die ohne Kodierung in
 * einem URL-Pfad stehen können.
 */
public record BatchJobId(String value) {

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    public BatchJobId {
        if (value == null || !VALID.matcher(value).matches()) {
            throw new InvalidBatchJobIdException(value);
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
