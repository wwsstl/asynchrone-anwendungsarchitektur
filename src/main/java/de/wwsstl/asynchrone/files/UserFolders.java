package de.wwsstl.asynchrone.files;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import de.wwsstl.asynchrone.cloud.TaskId;

/**
 * Die Ordner einer Benutzer:in; alle Zugriffe laufen über Java NIO.2.
 *
 * <ul>
 *   <li>{@code inbox}: Dateien, die auf die Übermittlung warten</li>
 *   <li>{@code pendingbox}: Dateien, die an Cloud-API 1 übergeben wurden und noch kein Ergebnis haben. Die TaskId
 *       einer Datei steht als Marker in {@code pendingbox/.taskids/<Dateiname>}; Dateien ohne Marker haben ein
 *       unklares Übermittlungsergebnis.</li>
 *   <li>{@code donebox}: erfolgreich erzeugte Dateien</li>
 *   <li>{@code errorbox}: Dateien, die nicht umgewandelt werden konnten oder bei deren Erzeugung ein Fehler auftrat</li>
 * </ul>
 */
public record UserFolders(String userId, Path inbox, Path pendingbox, Path donebox, Path errorbox) {

    /** Unterordner der {@code pendingbox} für die TaskId-Marker. */
    static final String MARKER_DIR = ".taskids";

    /** Bis zu {@code max} reguläre Dateien der {@code inbox}, ohne die übergebenen. */
    public List<Path> listInbox(int max, Set<Path> excluded) throws IOException {
        List<Path> files = new ArrayList<>(max);
        try (DirectoryStream<Path> inboxFiles = Files.newDirectoryStream(inbox, Files::isRegularFile)) {
            for (Path file : inboxFiles) {
                if (!excluded.contains(file)) {
                    files.add(file);
                    if (files.size() == max) {
                        break;
                    }
                }
            }
        }
        return files;
    }

    /** {@code true}, wenn die {@code pendingbox} Dateien enthält (der Marker-Ordner zählt nicht). */
    public boolean hasPendingFiles() throws IOException {
        try (DirectoryStream<Path> pendingFiles = Files.newDirectoryStream(pendingbox, Files::isRegularFile)) {
            return pendingFiles.iterator().hasNext();
        }
    }

    /** Verschiebt eine Datei der {@code inbox} vor dem Aufruf von Cloud-API 1 in die {@code pendingbox}. */
    public Path moveToPending(Path inboxFile) throws IOException {
        return moveInto(inboxFile, pendingbox);
    }

    /** Legt eine Datei, die eindeutig nicht übermittelt wurde, zurück in die {@code inbox}. */
    public Path moveBackToInbox(Path pendingFile) throws IOException {
        return moveInto(pendingFile, inbox);
    }

    /** Sichert die TaskId einer übermittelten Datei als Marker. */
    public void writeMarker(Path pendingFile, TaskId taskId) throws IOException {
        Path marker = markerOf(pendingFile);
        Files.createDirectories(marker.getParent());
        Files.writeString(marker, taskId.value());
    }

    /** TaskId-Marker einer Datei der {@code pendingbox}; der Pfad existiert nur, wenn ein Marker geschrieben wurde. */
    public Path markerOf(Path pendingFile) {
        return pendingbox.resolve(MARKER_DIR).resolve(pendingFile.getFileName());
    }

    public Path moveToDone(Path file) throws IOException {
        return settle(file, donebox);
    }

    public Path moveToError(Path file) throws IOException {
        return settle(file, errorbox);
    }

    /** Verschiebt in einen Endordner und entfernt danach den Marker, falls die Datei aus der {@code pendingbox} kam. */
    private Path settle(Path file, Path targetDir) throws IOException {
        Path moved = moveInto(file, targetDir);
        if (pendingbox.equals(file.getParent())) {
            Files.deleteIfExists(markerOf(file));
        }
        return moved;
    }

    /** Verschiebt ohne zu überschreiben; bei gleichem Namen erhält die Datei einen eindeutigen Zusatz. */
    private static Path moveInto(Path file, Path targetDir) throws IOException {
        String name = file.getFileName().toString();
        try {
            return Files.move(file, targetDir.resolve(name));
        } catch (FileAlreadyExistsException e) {
            int dot = name.lastIndexOf('.');
            String suffix = "-" + UUID.randomUUID().toString().substring(0, 8);
            String unique = dot > 0 ? name.substring(0, dot) + suffix + name.substring(dot) : name + suffix;
            return Files.move(file, targetDir.resolve(unique));
        }
    }
}
