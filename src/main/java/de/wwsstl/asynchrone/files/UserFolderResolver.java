package de.wwsstl.asynchrone.files;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import org.springframework.stereotype.Component;

import de.wwsstl.asynchrone.cloud.JobType;
import de.wwsstl.asynchrone.config.PipelineProperties;

/**
 * Liefert die Ordner einer Benutzer:in für eine Art von BatchgenAuftrag unter {@code pipeline.base-directory} und legt
 * sie bei Bedarf an: {@code <base>/<userId>/<jobType>/{inbox,pendingbox,donebox,errorbox}}. Jede Art hat eigene
 * Ordner; eine Aufgabe sieht nur die Dateien ihrer Art.
 */
@Component
public class UserFolderResolver {

    private final Path baseDirectory;

    public UserFolderResolver(PipelineProperties properties) {
        this.baseDirectory = properties.baseDirectory().toAbsolutePath().normalize();
    }

    /**
     * @throws InvalidUserIdException wenn sich die Kennung nicht als Ordnername eignet
     */
    public UserFolders resolve(String userId, JobType jobType) {
        InvalidUserIdException.requireValid(userId);
        Objects.requireNonNull(jobType, "jobType");
        Path root = baseDirectory.resolve(userId).resolve(jobType.name());
        UserFolders folders = new UserFolders(userId, root.resolve("inbox"), root.resolve("pendingbox"),
                root.resolve("donebox"), root.resolve("errorbox"));
        try {
            Files.createDirectories(folders.inbox());
            Files.createDirectories(folders.pendingbox());
            Files.createDirectories(folders.donebox());
            Files.createDirectories(folders.errorbox());
        } catch (IOException e) {
            throw new UncheckedIOException("Ordner von Benutzer '" + userId + "' (" + jobType
                    + ") konnten nicht angelegt werden", e);
        }
        return folders;
    }
}
