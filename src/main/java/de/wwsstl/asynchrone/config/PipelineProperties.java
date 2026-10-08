package de.wwsstl.asynchrone.config;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Konfiguration der Pipeline ({@code pipeline.*}).
 *
 * @param baseDirectory       Basisverzeichnis; je Benutzer {@code <base>/<userId>/{inbox,pendingbox,donebox,errorbox}}
 * @param batchSize           Dateien je Batch, den der Producer an Cloud-API 1 übermittelt
 * @param poolResumeThreshold der Producer liest den nächsten Batch erst, wenn der Status-Pool auf diesen Wert oder
 *                            darunter gesunken ist (Rückstau)
 * @param sweepInterval       Abstand zwischen zwei Durchläufen des Consumers
 * @param pollInterval        Abstand zwischen zwei Statusabfragen derselben TaskId
 * @param statusBulkSize      höchstens so viele TaskIds je Anfrage an Cloud-API 2
 * @param errorThreshold      die Aufgabe endet mit {@code ERROR}, sobald so viele Dateien fehlerhaft waren
 * @param taskTimeout         die Aufgabe endet mit {@code TIMEOUT}, sobald sie so lange läuft
 * @param submitRetryMaxWait  längste Wartezeit, bevor der Producer einen eindeutig nicht verarbeiteten Batch erneut
 *                            übermittelt; die Wartezeit beginnt bei {@code sweepInterval} und verdoppelt sich
 * @param cloud               Einstellungen der Cloud-Dienste
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
        @DefaultValue("1m") Duration submitRetryMaxWait,
        @DefaultValue Cloud cloud) {

    public PipelineProperties {
        requirePositive("batch-size", batchSize);
        if (poolResumeThreshold < 0 || poolResumeThreshold >= batchSize) {
            throw new IllegalArgumentException("pipeline.pool-resume-threshold muss zwischen 0 und batch-size - 1 "
                    + "liegen, ist aber " + poolResumeThreshold);
        }
        requirePositive("status-bulk-size", statusBulkSize);
        requirePositive("error-threshold", errorThreshold);
        requirePositive("sweep-interval", sweepInterval);
        requirePositive("poll-interval", pollInterval);
        requirePositive("task-timeout", taskTimeout);
        requirePositive("submit-retry-max-wait", submitRetryMaxWait);
    }

    /**
     * @param baseUrl       Basis-URL der Cloud-Dienste
     * @param jobPath       Pfad der BatchgenAufträge: anlegen (Cloud-API 1), lesen und Endzustand setzen (Cloud-API 3)
     * @param submitPath    Pfad zum Übermitteln eines Batches (Cloud-API 1)
     * @param statusPath    Pfad der Bulk-Statusabfrage (Cloud-API 2)
     * @param submitTimeout Timeout je Aufruf von Cloud-API 1; danach ist das Ergebnis unklar, und die Aufgabe endet bzw.
     *                      der Start wird mit {@code TIMEOUT} abgewiesen
     * @param statusTimeout Timeout je Aufruf von Cloud-API 2 und 3
     * @param retries       Wiederholungen bei Cloud-API 2 und 3 (idempotent); Cloud-API 1 wird nie wiederholt
     */
    public record Cloud(
            @DefaultValue("http://localhost:8081") URI baseUrl,
            @DefaultValue("/jobs") String jobPath,
            @DefaultValue("/tasks") String submitPath,
            @DefaultValue("/tasks/status") String statusPath,
            @DefaultValue("30s") Duration submitTimeout,
            @DefaultValue("30s") Duration statusTimeout,
            @DefaultValue("2") int retries) {

        public Cloud {
            requirePositive("cloud.submit-timeout", submitTimeout);
            requirePositive("cloud.status-timeout", statusTimeout);
            if (retries < 0) {
                throw new IllegalArgumentException("pipeline.cloud.retries darf nicht negativ sein, ist aber " + retries);
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
