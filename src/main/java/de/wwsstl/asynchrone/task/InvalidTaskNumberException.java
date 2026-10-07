package de.wwsstl.asynchrone.task;

import java.util.regex.Pattern;

/** Eine Aufgabennummer mit unzulässigem Format. */
public class InvalidTaskNumberException extends RuntimeException {

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    public InvalidTaskNumberException(String taskNumber) {
        super("Ungültige Aufgabennummer: '" + taskNumber + "'");
    }

    /** {@code true}, wenn die Aufgabennummer ohne Kodierung in einem URL-Pfad stehen kann. */
    public static boolean isValid(String taskNumber) {
        return taskNumber != null && VALID.matcher(taskNumber).matches();
    }

    /**
     * @throws InvalidTaskNumberException wenn die Aufgabennummer ungültig ist
     */
    public static String requireValid(String taskNumber) {
        if (!isValid(taskNumber)) {
            throw new InvalidTaskNumberException(taskNumber);
        }
        return taskNumber;
    }
}
