package de.wwsstl.asynchrone.files;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

import de.wwsstl.asynchrone.pool.TaskId;

/**
 * Die exklusiven Ordner eines Benutzers; alle Zugriffe laufen über Java NIO.2 (loesung_final.md 4.9).
 *
 * <p>Neben {@code inbox}, {@code errorbox} und {@code donebox} gibt es die {@code pendingbox}: Der Producer
 * verschiebt jede Datei <b>vor</b> der Übermittlung dorthin und legt nach der Antwort von Cloud-API 1 deren TaskId
 * als Marker in {@code pendingbox/.taskids/<Dateiname>} ab. Eine Datei in der {@code pendingbox} gilt damit als
 * übermittelt und wird nie erneut an Cloud-API 1 geschickt (Cloud-API 1 ist nicht idempotent); ein späterer Task
 * nimmt ihre Statusabfrage anhand des Markers wieder auf. Verlässt die Datei die {@code pendingbox} Richtung
 * {@code donebox}/{@code errorbox}, wird der Marker entfernt.
 */
public record UserFolders(String userId, Path inbox, Path pendingbox, Path errorbox, Path donebox) {

    /** Unterordner der {@code pendingbox} für die TaskId-Marker; als Verzeichnis nie Teil von {@link #openPendingbox()}. */
    static final String TASK_ID_DIR = ".taskids";

    /** Öffnet einen Verzeichnisstrom über die regulären Dateien der {@code inbox}; der Aufrufer schließt ihn. */
    public DirectoryStream<Path> openInbox() throws IOException {
        return Files.newDirectoryStream(inbox, Files::isRegularFile);
    }

    /** Öffnet einen Verzeichnisstrom über die regulären Dateien der {@code pendingbox}; der Aufrufer schließt ihn. */
    public DirectoryStream<Path> openPendingbox() throws IOException {
        return Files.newDirectoryStream(pendingbox, Files::isRegularFile);
    }

    /**
     * {@code true}, wenn die {@code pendingbox} noch Dateien enthält, also Dateien eines früheren BatchgenAuftrags
     * (funktionsweise_sequenz.md, Abschnitt 1).
     */
    public boolean hasPendingFiles() throws IOException {
        try (DirectoryStream<Path> files = openPendingbox()) {
            return files.iterator().hasNext();
        }
    }

    /** Beansprucht eine Datei der {@code inbox} für die Übermittlung; liefert ihren neuen Pfad. */
    public Path moveToPending(Path file) throws IOException {
        return moveInto(file, pendingbox);
    }

    public Path moveToDone(Path file) throws IOException {
        return settle(file, donebox);
    }

    public Path moveToError(Path file) throws IOException {
        return settle(file, errorbox);
    }

    /** Hält fest, dass Cloud-API 1 die Datei der {@code pendingbox} unter {@code taskId} angenommen hat. */
    public void recordTaskId(Path pendingFile, TaskId taskId) throws IOException {
        Path marker = taskIdMarker(pendingFile);
        Files.createDirectories(marker.getParent());
        Files.writeString(marker, taskId.value());
    }

    /**
     * Die TaskId, unter der die Datei der {@code pendingbox} übermittelt wurde — leer, wenn kein Marker existiert,
     * der Übermittlungsstatus also unbekannt ist.
     */
    public Optional<TaskId> submittedTaskId(Path pendingFile) throws IOException {
        Path marker = taskIdMarker(pendingFile);
        if (!Files.isRegularFile(marker)) {
            return Optional.empty();
        }
        String value = Files.readString(marker).strip();
        return value.isEmpty() ? Optional.empty() : Optional.of(new TaskId(value));
    }

    private Path taskIdMarker(Path pendingFile) {
        return pendingbox.resolve(TASK_ID_DIR).resolve(pendingFile.getFileName());
    }

    /** Verschiebt in einen Endordner; ein etwaiger TaskId-Marker wird erst danach entfernt. */
    private Path settle(Path file, Path targetDir) throws IOException {
        Path moved = moveInto(file, targetDir);
        if (pendingbox.equals(file.getParent())) {
            Files.deleteIfExists(taskIdMarker(file));
        }
        return moved;
    }

    /** Verschiebt ohne Überschreiben; bei Namenskollision wird ein eindeutiger Suffix angehängt. */
    private static Path moveInto(Path file, Path targetDir) throws IOException {
        String name = file.getFileName().toString();
        Path target = targetDir.resolve(name);
        try {
            return Files.move(file, target);
        } catch (FileAlreadyExistsException e) {
            int dot = name.lastIndexOf('.');
            String unique = (dot > 0 ? name.substring(0, dot) : name) + "-" + UUID.randomUUID().toString().substring(0, 8)
                    + (dot > 0 ? name.substring(dot) : "");
            return Files.move(file, targetDir.resolve(unique));
        }
    }
}
