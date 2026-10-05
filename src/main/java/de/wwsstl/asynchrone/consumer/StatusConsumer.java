package de.wwsstl.asynchrone.consumer;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.wwsstl.asynchrone.cloud.BatchJob;
import de.wwsstl.asynchrone.cloud.BatchJobStatus;
import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.CloudStatus;
import de.wwsstl.asynchrone.cloud.TaskStatusResult;
import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.context.AbortHandler;
import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.TaskContext;
import de.wwsstl.asynchrone.files.UserFolders;
import de.wwsstl.asynchrone.pool.PollEntry;
import de.wwsstl.asynchrone.pool.StatusPool;
import de.wwsstl.asynchrone.pool.TaskId;

/**
 * Consumer-Thread einer Sandbox — der „Verbraucher-Überwachungsthread" des Diagramms (loesung_final.md 4.8).
 *
 * <p>Jeder Durchlauf prüft zuerst über Cloud-API 3 den Status des BatchgenAuftrags. Steht er auf
 * {@code CANCELLED}, endet der Task (funktionsweise_sequenz.md, Abschnitt 5). Danach sammelt der Consumer die
 * fälligen Einträge des <b>eigenen</b> Status-Pools, übergibt sie in einem Bulk-Aufruf an Cloud-API 2 und routet
 * je Ergebnis:
 * <ul>
 *   <li>{@code SUCCESS} → Datei nach {@code donebox}, Eintrag entfernen</li>
 *   <li>{@code ERROR} → Datei nach {@code errorbox}, Fehlerzähler erhöhen, Eintrag entfernen</li>
 *   <li>{@code PENDING} → Eintrag bleibt im Pool, nächster Prüfzeitpunkt wird gesetzt</li>
 * </ul>
 * Ist der Producer fertig, alle Submit-Antworten sind eingegangen und der Pool ist leer, gilt der Task als
 * abgeschlossen.
 *
 * <p>Zeitüberschreitung und Fehlerschwelle meldet der Consumer über den {@link AbortHandler} an den
 * {@code TaskManager}, der den BatchgenAuftrag über Cloud-API 3 als {@code CANCELLED} markiert; im nächsten
 * Durchlauf endet der Task dann wie bei einem Abbruch von außen (Abschnitt 6). Bei Zeitüberschreitung wandern die
 * noch offenen Dateien in die {@code errorbox}. Sonst bleiben sie bei einem Abbruch samt TaskId in der
 * {@code pendingbox} und werden beim Fortsetzen weiter abgefragt, nicht erneut übermittelt.
 */
public final class StatusConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(StatusConsumer.class);

    /** Puffer, damit der Client-Timeout (der eigentliche Grenzwert) vor dem Future-Timeout greift. */
    private static final Duration RESULT_GRACE = Duration.ofSeconds(5);

    private final TaskContext context;
    private final StatusPool pool;
    private final UserFolders folders;
    private final CloudClient cloud;
    private final AbortHandler abortHandler;
    private final PipelineProperties properties;
    private final Clock clock;

    public StatusConsumer(TaskContext context, StatusPool pool, UserFolders folders, CloudClient cloud,
            AbortHandler abortHandler, PipelineProperties properties, Clock clock) {
        this.context = context;
        this.pool = pool;
        this.folders = folders;
        this.cloud = cloud;
        this.abortHandler = abortHandler;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void run() {
        try {
            loop();
        } catch (Throwable t) {
            log.error("[{}] Consumer von Task {} ist fehlgeschlagen", context.userId(), context.jobId(), t);
            context.cancel(CancelReason.INTERNAL_ERROR);
        } finally {
            abandonRemaining();
            log.info("[{}] Task {} beendet: {}", context.userId(), context.jobId(), context.snapshot(0));
        }
    }

    private void loop() {
        while (!context.isCancelled()) {
            // Abbruch von außen oder bereits gemeldeter Abbruch: Der BatchgenAuftrag steht auf CANCELLED.
            if (context.isStopping() || batchJobCancelled()) {
                context.cancel(context.abortRequest().orElse(CancelReason.USER_REQUEST));
                return;
            }
            if (context.isTimedOut()) {
                log.warn("[{}] Task {} hat die maximale Laufzeit von {} überschritten", context.userId(),
                        context.jobId(), properties.taskTimeout());
                requestAbort(CancelReason.TIMEOUT);
                failPendingAfterTimeout();
            } else {
                sweep();
                if (context.errorThresholdReached()) {
                    requestAbort(CancelReason.ERROR_THRESHOLD);
                } else if (!context.isStopping() && isFinished()) {
                    context.complete();
                    return;
                }
            }
            context.awaitStop(properties.sweepInterval());
        }
    }

    /** Cloud-API 3: {@code true}, wenn der BatchgenAuftrag als {@code CANCELLED} markiert ist. */
    private boolean batchJobCancelled() {
        try {
            Duration wait = properties.cloud().statusTimeout().plus(RESULT_GRACE);
            Optional<BatchJob> job = cloud.batchJob(context.jobId()).get(wait.toMillis(), TimeUnit.MILLISECONDS);
            return job.map(BatchJob::status).orElse(null) == BatchJobStatus.CANCELLED;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            context.cancel(CancelReason.SHUTDOWN);
            return false;
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            log.warn("[{}] Status des BatchgenAuftrags {} nicht abrufbar, nächster Versuch folgt: {}",
                    context.userId(), context.jobId(), e.toString());
            return false;
        }
    }

    /**
     * Meldet den Abbruch an den {@code TaskManager}. Ist Cloud-API 3 nicht erreichbar, endet der Task sofort, damit
     * er nicht auf einen Status wartet, der nie gesetzt wird.
     */
    private void requestAbort(CancelReason reason) {
        if (context.requestAbort(reason) && !abortHandler.abort(context, reason)) {
            context.cancel(reason);
        }
    }

    /**
     * Der Producer übermittelt jeden Batch synchron und trägt dessen TaskIds vollständig in den Pool ein, bevor er
     * die nächste Dateicharge liest (anforderungen_datenverarbeitung.md, Punkt 1). Ist der Producer also fertig,
     * sind alle Einträge bereits im Pool sichtbar.
     */
    private boolean isFinished() {
        return context.isProducerFinished() && pool.isEmpty();
    }

    private void sweep() {
        Instant now = clock.instant();
        List<Map.Entry<TaskId, PollEntry>> due = new ArrayList<>(pool.due(now).entrySet());
        for (int from = 0; from < due.size(); from += properties.statusBulkSize()) {
            if (context.isStopping()) {
                return;
            }
            int to = Math.min(from + properties.statusBulkSize(), due.size());
            queryAndRoute(due.subList(from, to));
        }
    }

    /** Ein Bulk-Aufruf an Cloud-API 2 für alle übergebenen (ausschließlich eigenen) TaskIds. */
    private void queryAndRoute(List<Map.Entry<TaskId, PollEntry>> chunk) {
        List<TaskId> ids = chunk.stream().map(Map.Entry::getKey).toList();
        Map<TaskId, CloudStatus> statusById = new HashMap<>();
        try {
            Duration wait = properties.cloud().statusTimeout().plus(RESULT_GRACE);
            for (TaskStatusResult result : cloud.queryStatus(ids).get(wait.toMillis(), TimeUnit.MILLISECONDS)) {
                statusById.put(result.taskId(), result.status());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            context.cancel(CancelReason.SHUTDOWN);
            return;
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            log.warn("[{}] Bulk-Statusabfrage für {} TaskIds fehlgeschlagen, nächster Versuch folgt: {}",
                    context.userId(), ids.size(), e.toString());
        }

        Instant next = clock.instant().plus(properties.pollInterval());
        for (Map.Entry<TaskId, PollEntry> entry : chunk) {
            TaskId id = entry.getKey();
            CloudStatus status = statusById.getOrDefault(id, CloudStatus.PENDING);
            switch (status) {
                case SUCCESS -> route(id, entry.getValue(), true);
                case ERROR -> route(id, entry.getValue(), false);
                case PENDING -> pool.reschedule(id, next);
            }
        }
    }

    private void route(TaskId id, PollEntry entry, boolean success) {
        boolean moved = move(entry, success);
        pool.remove(id);
        if (success && moved) {
            context.recordSucceeded();
        } else {
            context.recordError();
        }
    }

    private boolean move(PollEntry entry, boolean success) {
        try {
            if (success) {
                folders.moveToDone(entry.file());
            } else {
                folders.moveToError(entry.file());
            }
            return true;
        } catch (IOException e) {
            log.error("[{}] Datei {} konnte nicht verschoben werden", context.userId(), entry.file().getFileName(), e);
            return false;
        }
    }

    /** Zeitüberschreitung: alle noch offenen Dateien werden wie {@code ERROR} behandelt (loesung_final.md 4.8). */
    private void failPendingAfterTimeout() {
        pool.drain().values().forEach(entry -> {
            move(entry, false);
            context.recordError();
        });
    }

    /**
     * Beim Beenden verbliebene Einträge freigeben; die Dateien bleiben mit ihrer TaskId in der {@code pendingbox},
     * damit das Fortsetzen die Statusabfrage wieder aufnimmt.
     */
    private void abandonRemaining() {
        int abandoned = pool.drain().size();
        if (abandoned > 0) {
            context.recordAbandoned(abandoned);
            log.info("[{}] Task {}: {} übermittelte Dateien blieben unentschieden in der pendingbox",
                    context.userId(), context.jobId(), abandoned);
        }
    }
}
