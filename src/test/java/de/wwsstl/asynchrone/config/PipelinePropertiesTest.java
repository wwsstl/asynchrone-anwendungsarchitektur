package de.wwsstl.asynchrone.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import de.wwsstl.asynchrone.TestProperties;

class PipelinePropertiesTest {

    @Test
    void defaultsMatchTheDocumentedValues() {
        PipelineProperties properties = new Binder(new MapConfigurationPropertySource(Map.of()))
                .bindOrCreate("pipeline", PipelineProperties.class);

        assertThat(properties.baseDirectory().normalize()).isEqualTo(Path.of("data", "users"));
        assertThat(properties.batchSize()).isEqualTo(4);
        assertThat(properties.poolResumeThreshold()).isEqualTo(2);
        assertThat(properties.sweepInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(properties.pollInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(properties.statusBulkSize()).isEqualTo(200);
        assertThat(properties.errorThreshold()).isEqualTo(10);
        assertThat(properties.taskTimeout()).isEqualTo(Duration.ofMinutes(30));
        assertThat(properties.submitRetryMaxWait()).isEqualTo(Duration.ofMinutes(1));
        assertThat(properties.cloud()).isEqualTo(new PipelineProperties.Cloud(URI.create("http://localhost:8081"),
                "/jobs", "/tasks", "/tasks/status", Duration.ofSeconds(30), Duration.ofSeconds(30), 2));
    }

    @Test
    void poolResumeThresholdMustBeBelowBatchSize() {
        PipelineProperties valid = TestProperties.withBase(Path.of("x"));

        assertThatThrownBy(() -> new PipelineProperties(valid.baseDirectory(), 4, 4, valid.sweepInterval(),
                valid.pollInterval(), valid.statusBulkSize(), valid.errorThreshold(), valid.taskTimeout(),
                valid.submitRetryMaxWait(), valid.cloud()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pool-resume-threshold");
    }

    @Test
    void durationsMustBePositive() {
        PipelineProperties valid = TestProperties.withBase(Path.of("x"));

        assertThatThrownBy(() -> new PipelineProperties(valid.baseDirectory(), 4, 2, Duration.ZERO,
                valid.pollInterval(), valid.statusBulkSize(), valid.errorThreshold(), valid.taskTimeout(),
                valid.submitRetryMaxWait(), valid.cloud()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sweep-interval");
    }

    @Test
    void retriesMustNotBeNegative() {
        assertThatThrownBy(() -> new PipelineProperties.Cloud(URI.create("http://localhost"), "/jobs", "/tasks",
                "/tasks/status", Duration.ofSeconds(1), Duration.ofSeconds(1), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retries");
    }
}
