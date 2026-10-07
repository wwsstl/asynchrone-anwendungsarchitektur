package de.wwsstl.asynchrone.consumer;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.RecordStatus;
import de.wwsstl.asynchrone.cloud.RecordStatusResult;
import de.wwsstl.asynchrone.cloud.TaskId;
import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.files.UserFolders;
import de.wwsstl.asynchrone.pool.PoolEntry;
import de.wwsstl.asynchrone.pool.StatusPool;
import de.wwsstl.asynchrone.task.TaskContext;
import de.wwsstl.asynchrone.task.TaskState;

/**
 * Consumer-Thread einer Sandbox (funktionsweise_sequenz.md, Abschnitte 3 und 6). In jedem {@code sweep-interval}
 * fragt er die fälligen TaskIds seines Status-Pools gebündelt bei Cloud-API 2 ab:
 * <ul>
 *   <li>{@code SUCCESS}: Datei in die {@code donebox}, Marker löschen</li>
 *   <li>{@code ERROR}: Datei in die {@code errorbox}, Marker löschen, Fehlerzähler erhöhen</li>
 *   <li>{@code PENDING}: nach {@code poll-interval} erneut prüfen</li>
 * </ul>
 * Außerdem erkennt er, wann die Aufgabe von selbst endet: {@code TIMEOUT} (maximale Laufzeit überschritten),
 * {@code ERROR} (Fehlerschwelle erreicht) oder {@code COMPLETED} (Producer fertig, Status-Pool leer). Dateien ohne
 * Ergebnis bleiben dabei mit ihrem Marker in der {@code pendingbox}.
 */
public final class StatusConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(StatusConsumer.class);

    /** Puffer, damit der Timeout des Clients (der eigentliche Grenzwert) vor dem Timeout des Futures greift. */
    private static final Duration RESULT_GRACE = Duration.ofSeconds(5);

    private final TaskContext context;
    private final StatusPool pool;
    private final UserFolders folders;
    private final CloudClient cloud;
    private final PipelineProperties properties;
    private final Clock clock;

    public StatusConsumer(TaskContext context, StatusPool pool, UserFolders folders, CloudClient cloud,
            PipelineProperties properties, Clock clock) {
        this.context = context;
        this.pool = pool;
        this.folders = folders;
        this.cloud = cloud;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void run() {
        try {
            loop();
        } catch (Throwable t) {
            log.error("[{}] Consumer von Aufgabe {} ist fehlgeschlagen", context.userId(), context.taskNumber(), t);
            context.finish(TaskState.ABORTED);
        } finally {
            log.info("[{}] Consumer von Aufgabe {} beendet: {}", context.userId(), context.taskNumber(),
                    context.snapshot(pool.size()));
        }
    }

    private void loop() {
        while (!context.isStopping()) {
            if (context.isTimedOut()) {
                end(TaskState.TIMEOUT);
                return;
            }
            sweep();
            if (context.isStopping()) {
                return;
            }
            if (context.errorThresholdReached()) {
                end(TaskState.ERROR);
                return;
            }
            if (context.isProducerFinished() && pool.isEmpty()) {
                end(TaskState.COMPLETED);
                return;
            }
            context.awaitStop(properties.sweepInterval());
        }
    }

    private void end(TaskState endState) {
        if (context.finish(endState)) {
            log.info("[{}] Aufgabe {} endet mit {}; {} Dateien ohne Ergebnis bleiben in der pendingbox",
                    context.userId(), context.taskNumber(), endState, pool.size());
        }
    }

    private void sweep() {
        List<PoolEntry> due = pool.due(clock.instant());
        for (int from = 0; from < due.size(); from += properties.statusBulkSize()) {
            if (context.isStopping()) {
                return;
            }
            queryAndRoute(due.subList(from, Math.min(from + properties.statusBulkSize(), due.size())));
        }
    }

    /** Eine Anfrage an Cloud-API 2 für alle übergebenen TaskIds dieser Sandbox. */
    private void queryAndRoute(List<PoolEntry> chunk) {
        Map<TaskId, RecordStatus> statusById = new HashMap<>();
        Future<List<RecordStatusResult>> call = cloud.queryStatus(chunk.stream().map(PoolEntry::taskId).toList());
        try {
            Duration timeout = properties.cloud().statusTimeout();
            Duration wait = timeout.multipliedBy(properties.cloud().retries() + 1L).plus(RESULT_GRACE);
            for (RecordStatusResult result : call.get(wait.toMillis(), TimeUnit.MILLISECONDS)) {
                statusById.put(result.taskId(), result.status());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            context.finish(TaskState.ABORTED);
            return;
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            call.cancel(true);
            log.warn("[{}] Statusabfrage für {} TaskIds fehlgeschlagen, nächster Versuch folgt: {}", context.userId(),
                    chunk.size(), e.toString());
        }

        Instant nextCheck = clock.instant().plus(properties.pollInterval());
        for (PoolEntry entry : chunk) {
            switch (statusById.getOrDefault(entry.taskId(), RecordStatus.PENDING)) {
                case SUCCESS -> settle(entry, true);
                case ERROR -> settle(entry, false);
                case PENDING -> pool.reschedule(entry.taskId(), nextCheck);
            }
        }
    }

    private void settle(PoolEntry entry, boolean success) {
        boolean moved = true;
        try {
            if (success) {
                folders.moveToDone(entry.file());
            } else {
                folders.moveToError(entry.file());
            }
        } catch (IOException e) {
            moved = false;
            log.error("[{}] Datei {} konnte nicht verschoben werden", context.userId(), entry.file().getFileName(), e);
        }
        pool.remove(entry.taskId());
        if (success && moved) {
            context.recordSucceeded();
        } else {
            context.recordError();
        }
    }
}
