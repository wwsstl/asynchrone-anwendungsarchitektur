package de.wwsstl.asynchrone.files;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/** Die exklusiven Ordner eines Benutzers; alle Zugriffe laufen über Java NIO.2 (loesung_final.md 4.9). */
public record UserFolders(String userId, Path inbox, Path errorbox, Path donebox) {

    /** Öffnet einen Verzeichnisstrom über die regulären Dateien der {@code inbox}; der Aufrufer schließt ihn. */
    public DirectoryStream<Path> openInbox() throws IOException {
        return Files.newDirectoryStream(inbox, Files::isRegularFile);
    }

    public Path moveToDone(Path file) throws IOException {
        return moveInto(file, donebox);
    }

    public Path moveToError(Path file) throws IOException {
        return moveInto(file, errorbox);
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
