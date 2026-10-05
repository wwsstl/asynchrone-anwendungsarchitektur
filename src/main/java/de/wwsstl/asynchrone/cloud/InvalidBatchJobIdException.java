package de.wwsstl.asynchrone.cloud;

/** Eine Aufgabennummer (BatchgenAuftrag-Nummer) mit unzulässigem Format. */
public class InvalidBatchJobIdException extends IllegalArgumentException {

    public InvalidBatchJobIdException(String value) {
        super("Ungültige Aufgabennummer: '" + value + "'");
    }
}
