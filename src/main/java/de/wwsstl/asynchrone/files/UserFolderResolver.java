package de.wwsstl.asynchrone.files;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import de.wwsstl.asynchrone.config.PipelineProperties;

/** Liefert die Ordner einer Benutzer:in unter {@code pipeline.base-directory} und legt sie bei Bedarf an. */
@Component
public class UserFolderResolver {

    private static final Pattern VALID_USER_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");

    private final Path baseDirectory;

    public UserFolderResolver(PipelineProperties properties) {
        this.baseDirectory = properties.baseDirectory().toAbsolutePath().normalize();
    }

    /**
     * @throws InvalidUserIdException wenn sich die Kennung nicht als Ordnername eignet
     */
    public UserFolders resolve(String userId) {
        if (userId == null || !VALID_USER_ID.matcher(userId).matches()) {
            throw new InvalidUserIdException(userId);
        }
        Path root = baseDirectory.resolve(userId);
        UserFolders folders = new UserFolders(userId, root.resolve("inbox"), root.resolve("pendingbox"),
                root.resolve("donebox"), root.resolve("errorbox"));
        try {
            Files.createDirectories(folders.inbox());
            Files.createDirectories(folders.pendingbox());
            Files.createDirectories(folders.donebox());
            Files.createDirectories(folders.errorbox());
        } catch (IOException e) {
            throw new UncheckedIOException("Ordner von Benutzer '" + userId + "' konnten nicht angelegt werden", e);
        }
        return folders;
    }
}
