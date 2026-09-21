package de.wwsstl.asynchrone.files;

public class InvalidUserIdException extends RuntimeException {

    public InvalidUserIdException(String userId) {
        super("Ungültige Benutzerkennung: '" + userId + "' (erlaubt: 1-64 Zeichen aus A-Z, a-z, 0-9, '.', '_', '-', "
                + "beginnend mit Buchstabe oder Ziffer)");
    }
}
