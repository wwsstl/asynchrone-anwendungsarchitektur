package de.wwsstl.asynchrone.files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import de.wwsstl.asynchrone.TestProperties;
import de.wwsstl.asynchrone.cloud.TaskId;

class UserFoldersTest {

    @TempDir
    Path base;

    private UserFolderResolver resolver;
    private UserFolders folders;

    @BeforeEach
    void setUp() {
        resolver = new UserFolderResolver(TestProperties.withBase(base));
        folders = resolver.resolve("anna");
    }

    @Test
    void resolverCreatesAllFolders() {
        assertThat(folders.inbox()).isEqualTo(base.resolve("anna").resolve("inbox")).isDirectory();
        assertThat(folders.pendingbox()).isDirectory();
        assertThat(folders.donebox()).isDirectory();
        assertThat(folders.errorbox()).isDirectory();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "..", "../anna", "a/b", "a\\b", "-anna", ".anna"})
    void resolverRejectsUnsuitableUserIds(String userId) {
        assertThatThrownBy(() -> resolver.resolve(userId)).isInstanceOf(InvalidUserIdException.class);
    }

    @Test
    void resolverRejectsTooLongUserId() {
        assertThatThrownBy(() -> resolver.resolve("a".repeat(65))).isInstanceOf(InvalidUserIdException.class);
    }

    @Test
    void listInboxRespectsMaxAndExclusions() throws IOException {
        Path a = inboxFile("a.json");
        inboxFile("b.json");
        inboxFile("c.json");
        Files.createDirectory(folders.inbox().resolve("sub"));

        assertThat(folders.listInbox(2, Set.of())).hasSize(2);
        assertThat(folders.listInbox(10, Set.of(a))).extracting(path -> path.getFileName().toString())
                .containsExactlyInAnyOrder("b.json", "c.json");
    }

    @Test
    void markerDirectoryAloneDoesNotCountAsPendingFile() throws IOException {
        Files.createDirectories(folders.pendingbox().resolve(UserFolders.MARKER_DIR));
        assertThat(folders.hasPendingFiles()).isFalse();

        folders.moveToPending(inboxFile("a.json"));
        assertThat(folders.hasPendingFiles()).isTrue();
    }

    @Test
    void settlingRemovesTheMarker() throws IOException {
        Path pending = folders.moveToPending(inboxFile("a.json"));
        folders.writeMarker(pending, new TaskId("t-1"));
        assertThat(folders.markerOf(pending)).hasContent("t-1");

        Path done = folders.moveToDone(pending);

        assertThat(done).isEqualTo(folders.donebox().resolve("a.json")).hasContent("content of a.json");
        assertThat(pending).doesNotExist();
        assertThat(folders.markerOf(pending)).doesNotExist();
    }

    @Test
    void moveBackToInboxRestoresTheFile() throws IOException {
        Path pending = folders.moveToPending(inboxFile("a.json"));

        Path back = folders.moveBackToInbox(pending);

        assertThat(back).isEqualTo(folders.inbox().resolve("a.json"));
        assertThat(folders.hasPendingFiles()).isFalse();
    }

    @Test
    void sameNameInTargetGetsUniqueSuffix() throws IOException {
        Files.writeString(folders.errorbox().resolve("a.json"), "older");
        Path pending = folders.moveToPending(inboxFile("a.json"));

        Path moved = folders.moveToError(pending);

        assertThat(moved.getParent()).isEqualTo(folders.errorbox());
        assertThat(moved.getFileName().toString()).matches("a-[0-9a-f]{8}\\.json");
        assertThat(folders.errorbox().resolve("a.json")).hasContent("older");
    }

    private Path inboxFile(String name) throws IOException {
        return Files.writeString(folders.inbox().resolve(name), "content of " + name);
    }
}
