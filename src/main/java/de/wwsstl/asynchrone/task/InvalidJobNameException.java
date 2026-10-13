package de.wwsstl.asynchrone.task;

/** {@code jobName} in der Startanfrage ist zu lang. */
public class InvalidJobNameException extends RuntimeException {

    public InvalidJobNameException(int length, int maxLength) {
        super("jobName ist zu lang: " + length + " Zeichen (höchstens " + maxLength + ")");
    }
}
