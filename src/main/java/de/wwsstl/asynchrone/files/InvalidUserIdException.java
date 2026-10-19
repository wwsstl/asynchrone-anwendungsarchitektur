package de.wwsstl.asynchrone.files;

import java.util.regex.Pattern;

/** Eine Benutzerkennung, die sich nicht als Ordnername eignet. */
public class InvalidUserIdException extends RuntimeException {

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");

    public InvalidUserIdException(String userId) {
        super("Ungültige Benutzerkennung: '" + userId + "' (erlaubt: 1-64 Zeichen aus A-Z, a-z, 0-9, '.', '_', '-', "
                + "beginnend mit Buchstabe oder Ziffer)");
    }

    /**
     * @throws InvalidUserIdException wenn sich die Kennung nicht als Ordnername eignet
     */
    public static String requireValid(String userId) {
        if (userId == null || !VALID.matcher(userId).matches()) {
            throw new InvalidUserIdException(userId);
        }
        return userId;
    }
}
