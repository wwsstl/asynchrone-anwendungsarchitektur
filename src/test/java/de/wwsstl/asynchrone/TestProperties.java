package de.wwsstl.asynchrone;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

import de.wwsstl.asynchrone.config.PipelineProperties;

/** Konfiguration mit kurzen Intervallen für Unit-Tests ohne Spring-Kontext. */
public final class TestProperties {

    private TestProperties() {
    }

    public static PipelineProperties withBase(Path base) {
        return new PipelineProperties(base, 4, 2, Duration.ofMillis(20), Duration.ofMillis(50), 200, 3,
                Duration.ofMinutes(1), Duration.ofMillis(100), cloud(URI.create("http://localhost:8081")));
    }

    public static PipelineProperties.Cloud cloud(URI baseUrl) {
        return new PipelineProperties.Cloud(baseUrl, "/jobs", "/tasks", "/tasks/status", Duration.ofSeconds(1),
                Duration.ofSeconds(1), 1);
    }
}
