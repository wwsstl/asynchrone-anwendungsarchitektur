package de.wwsstl.asynchrone.cloud;

/**
 * Nummer eines Datensatzes in der Untertabelle des BatchgenAuftrags. Sie gehört zu genau einer lokalen Datei und wird
 * als Marker zusammen mit der Datei in der {@code pendingbox} gespeichert.
 */
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
