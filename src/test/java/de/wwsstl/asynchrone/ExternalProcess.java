package de.wwsstl.asynchrone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Ein Prozess außerhalb der Test-JVM für die Black-Box-Integrationstests. Seine Ausgabe landet in
 * {@code target/it-logs/<name>.log}.
 */
final class ExternalProcess implements AutoCloseable {

    private final String name;
    private final Process process;
    private final Path log;

    private ExternalProcess(String name, Process process, Path log) {
        this.name = name;
        this.process = process;
        this.log = log;
    }

    static ExternalProcess start(String name, List<String> command) throws IOException {
        Path log = Path.of("target", "it-logs", name + ".log").toAbsolutePath();
        Files.createDirectories(log.getParent());
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        return new ExternalProcess(name, process, log);
    }

    /** Das {@code java} der laufenden JVM. */
    static String java() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }

    /**
     * Schreibt den Klassenpfad der Test-JVM in eine Argumentdatei für {@code java @datei}; so stößt ein langer
     * Klassenpfad nicht an die Längengrenze der Kommandozeile.
     */
    static Path classpathArgFile() throws IOException {
        String classpath = System.getProperty("java.class.path").replace('\\', '/');
        Path argFile = Path.of("target", "it-logs", "classpath.args").toAbsolutePath();
        Files.createDirectories(argFile.getParent());
        return Files.writeString(argFile, "-cp \"" + classpath + "\"");
    }

    static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Wartet, bis der Prozess unter der Adresse eine HTTP-Antwort liefert, gleich mit welchem Status. */
    void awaitHttp(String url, Duration timeout) throws InterruptedException {
        HttpClient http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(1))
                .build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build();
        Instant deadline = Instant.now().plus(timeout);
        while (true) {
            if (!process.isAlive()) {
                throw new IllegalStateException(name + " wurde mit Exit-Code " + process.exitValue()
                        + " beendet, siehe " + log);
            }
            try {
                http.send(request, HttpResponse.BodyHandlers.discarding());
                return;
            } catch (IOException notReady) {
                if (Instant.now().isAfter(deadline)) {
                    throw new IllegalStateException(name + " antwortet nach " + timeout + " nicht, siehe " + log);
                }
                Thread.sleep(200);
            }
        }
    }

    @Override
    public void close() throws InterruptedException {
        process.destroy();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
        }
    }
}
