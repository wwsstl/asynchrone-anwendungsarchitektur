package de.wwsstl.asynchrone.cloud;

/**
 * Antwort von Cloud-API 1 für eine Datei des Batches.
 *
 * @param taskId        TaskId des angelegten Datensatzes; {@code null}, wenn die Cloud die Datei nicht umgewandelt hat
 * @param fehlermeldung Begründung der Cloud, warum sie die Datei nicht umgewandelt hat; leer oder {@code null}, wenn
 *                      die Datei umgewandelt wurde oder die Cloud keinen Grund nennt
 */
public record SubmittedFile(String fileName, TaskId taskId, String fehlermeldung) {

    /** {@code true}, wenn die Cloud die Datei umgewandelt und eine TaskId geliefert hat. */
    public boolean converted() {
        return taskId != null;
    }
}
