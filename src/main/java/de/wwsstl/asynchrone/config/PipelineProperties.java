package de.wwsstl.asynchrone.config;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Konfigurationswerte der Pipeline (loesung_final.md, Abschnitt 6, Punkt 3).
 *
 * @param baseDirectory       Basisverzeichnis; je Benutzer {@code <base>/<userId>/{inbox,errorbox,donebox}}
 * @param batchSize           Dateien je Batch, den der Producer an Cloud-API 1 übergibt
 * @param poolResumeThreshold Schwellenwert für die Anzahl der Dateien im Status-Pool, ab dem der Producer die
 *                            nächste Dateicharge liest (anforderungen_datenverarbeitung.md, Punkte 1 und 3)
 * @param sweepInterval       Wartezeit zwischen zwei Durchläufen des Consumer-Threads
 * @param pollInterval        Wartezeit bis zur nächsten Statusprüfung einer TaskId
 * @param statusBulkSize      maximale Anzahl TaskIds je Bulk-Aufruf an Cloud-API 2
 * @param errorThreshold      Abbruch, sobald so viele Dateien fehlerhaft waren
 * @param taskTimeout         maximale Laufzeit einer Aufgabe
 * @param cloud               Einstellungen des Cloud-Clients
 */
@ConfigurationProperties("pipeline")
public record PipelineProperties(
        @DefaultValue("./data/users") Path baseDirectory,
        @DefaultValue("4") int batchSize,
        @DefaultValue("2") int poolResumeThreshold,
        @DefaultValue("1s") Duration sweepInterval,
        @DefaultValue("5s") Duration pollInterval,
        @DefaultValue("200") int statusBulkSize,
        @DefaultValue("10") int errorThreshold,
        @DefaultValue("30m") Duration taskTimeout,
        @DefaultValue Cloud cloud) {

    public PipelineProperties {
        requirePositive("batch-size", batchSize);
        if (poolResumeThreshold < 0) {
            throw new IllegalArgumentException("pipeline.pool-resume-threshold darf nicht negativ sein, ist aber "
                    + poolResumeThreshold);
        }
        if (poolResumeThreshold >= batchSize) {
            throw new IllegalArgumentException(
                    "pipeline.pool-resume-threshold muss kleiner als pipeline.batch-size sein, ist aber "
                            + poolResumeThreshold + " >= " + batchSize);
        }
        requirePositive("status-bulk-size", statusBulkSize);
        requirePositive("error-threshold", errorThreshold);
        requirePositive("sweep-interval", sweepInterval);
        requirePositive("poll-interval", pollInterval);
        requirePositive("task-timeout", taskTimeout);
    }

    /**
     * @param baseUrl        Basis-URL der Cloud-Dienste
     * @param submitPath     Pfad von Cloud-API 1 (Erstellungsauftrag übermitteln)
     * @param statusPath     Pfad von Cloud-API 2 (Bulk-Statusabfrage)
     * @param submitTimeout  Timeout je Submit-Aufruf
     * @param statusTimeout  Timeout je Statusabfrage
     * @param submitRetries  Wiederholungen bei Cloud-API 1 (Aufträge sind nicht idempotent, daher standardmäßig 0)
     * @param statusRetries  Wiederholungen bei Cloud-API 2
     */
    public record Cloud(
            @DefaultValue("http://localhost:8081") URI baseUrl,
            @DefaultValue("/tasks") String submitPath,
            @DefaultValue("/tasks/status") String statusPath,
            @DefaultValue("30s") Duration submitTimeout,
            @DefaultValue("120s") Duration statusTimeout,
            @DefaultValue("0") int submitRetries,
            @DefaultValue("5") int statusRetries) {

        public Cloud {
            requirePositive("cloud.submit-timeout", submitTimeout);
            requirePositive("cloud.status-timeout", statusTimeout);
            if (submitRetries < 0 || statusRetries < 0) {
                throw new IllegalArgumentException("pipeline.cloud.*-retries darf nicht negativ sein");
            }
        }
    }

    private static void requirePositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("pipeline." + name + " muss > 0 sein, ist aber " + value);
        }
    }

    private static void requirePositive(String name, Duration value) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("pipeline." + name + " muss eine positive Dauer sein, ist aber " + value);
        }
    }
}
