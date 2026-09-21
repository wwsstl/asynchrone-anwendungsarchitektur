package de.wwsstl.asynchrone.files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import de.wwsstl.asynchrone.config.PipelineProperties;

class NioUserFolderResolverTest {

    @TempDir
    Path base;

    private NioUserFolderResolver resolver;

    @BeforeEach
    void setUp() {
        PipelineProperties properties = new PipelineProperties(base, 20, 4, Duration.ofSeconds(1),
                Duration.ofSeconds(5), 200, 10, Duration.ofMinutes(30),
                new PipelineProperties.Cloud(java.net.URI.create("http://localhost:8081"), "/tasks",
                        "/tasks/status", Duration.ofSeconds(30), Duration.ofSeconds(30), 0, 2));
        resolver = new NioUserFolderResolver(properties);
    }

    @Test
    void legtDieDreiOrdnerBeimErstenZugriffAn() {
        UserFolders folders = resolver.resolve("alice");

        assertThat(folders.inbox()).isEqualTo(base.toAbsolutePath().resolve("alice/inbox")).isDirectory();
        assertThat(folders.errorbox()).isEqualTo(base.toAbsolutePath().resolve("alice/errorbox")).isDirectory();
        assertThat(folders.donebox()).isEqualTo(base.toAbsolutePath().resolve("alice/donebox")).isDirectory();
    }

    @Test
    void jederBenutzerBekommtEigeneOrdner() {
        assertThat(resolver.resolve("alice").inbox()).isNotEqualTo(resolver.resolve("bob").inbox());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", " ", ".", "..", "../evil", "a/b", "a\\b", ".hidden", "-x", "a b", "ü",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
    void lehntUngueltigeBenutzerkennungenAb(String userId) {
        assertThatThrownBy(() -> resolver.resolve(userId)).isInstanceOf(InvalidUserIdException.class);
    }

    @Test
    void verschiebtDateienNachDoneUndError() throws IOException {
        UserFolders folders = resolver.resolve("alice");
        Path a = Files.writeString(folders.inbox().resolve("a.txt"), "a");
        Path b = Files.writeString(folders.inbox().resolve("b.txt"), "b");

        folders.moveToDone(a);
        folders.moveToError(b);

        assertThat(folders.donebox().resolve("a.txt")).hasContent("a");
        assertThat(folders.errorbox().resolve("b.txt")).hasContent("b");
        assertThat(folders.inbox()).isEmptyDirectory();
    }

    @Test
    void ueberschreibtBeiNamenskollisionNichts() throws IOException {
        UserFolders folders = resolver.resolve("alice");
        Files.writeString(folders.donebox().resolve("a.txt"), "alt");
        Path neu = Files.writeString(folders.inbox().resolve("a.txt"), "neu");

        Path moved = folders.moveToDone(neu);

        assertThat(folders.donebox().resolve("a.txt")).hasContent("alt");
        assertThat(moved).hasContent("neu").isNotEqualTo(folders.donebox().resolve("a.txt"));
        assertThat(moved.getFileName().toString()).startsWith("a-").endsWith(".txt");
    }
}
