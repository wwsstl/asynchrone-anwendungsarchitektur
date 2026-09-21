package de.wwsstl.asynchrone.pool;

/** Vom Cloud-Dienst vergebene Kennung eines Erstellungsauftrags (nicht zu verwechseln mit der ID des Tasks). */
public record TaskId(String value) {

    public TaskId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("TaskId darf nicht leer sein");
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
