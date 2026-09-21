package de.wwsstl.asynchrone.files;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import de.wwsstl.asynchrone.config.PipelineProperties;

/** Ordnerkonvention {@code <basisverzeichnis>/<userId>/{inbox,errorbox,donebox}}. */
@Component
public class NioUserFolderResolver implements UserFolderResolver {

    /** Ein einzelner, harmloser Ordnername: kein Pfadtrenner, nicht mit '.' beginnend (also nie '.' oder '..'). */
    private static final Pattern VALID_USER_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");

    private final Path base;

    public NioUserFolderResolver(PipelineProperties properties) {
        this.base = properties.baseDirectory().toAbsolutePath().normalize();
    }

    @Override
    public UserFolders resolve(String userId) {
        if (userId == null || !VALID_USER_ID.matcher(userId).matches()) {
            throw new InvalidUserIdException(userId);
        }
        Path root = base.resolve(userId).normalize();
        if (!root.startsWith(base) || root.equals(base)) {
            throw new InvalidUserIdException(userId);
        }
        UserFolders folders = new UserFolders(userId, root.resolve("inbox"), root.resolve("errorbox"),
                root.resolve("donebox"));
        try {
            Files.createDirectories(folders.inbox());
            Files.createDirectories(folders.errorbox());
            Files.createDirectories(folders.donebox());
        } catch (IOException e) {
            throw new UncheckedIOException("Ordner für Benutzer '" + userId + "' konnten nicht angelegt werden", e);
        }
        return folders;
    }
}
