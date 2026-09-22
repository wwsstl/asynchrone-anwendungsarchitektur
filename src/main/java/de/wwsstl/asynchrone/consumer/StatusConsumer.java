package de.wwsstl.asynchrone.consumer;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.CloudStatus;
import de.wwsstl.asynchrone.cloud.TaskStatusResult;
import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.TaskContext;
import de.wwsstl.asynchrone.files.UserFolders;
import de.wwsstl.asynchrone.pool.PollEntry;
import de.wwsstl.asynchrone.pool.StatusPool;
import de.wwsstl.asynchrone.pool.TaskId;

/**
 * Consumer-Thread einer Sandbox — der „Verbraucher-Überwachungsthread" des Diagramms (loesung_final.md 4.8).
 *
 * <p>In einer Schleife: Timeout prüfen, fällige Einträge des <b>eigenen</b> Status-Pools sammeln, sie in einem
 * Bulk-Aufruf an Cloud-API 2 übergeben und je Ergebnis routen:
 * <ul>
 *   <li>{@code SUCCESS} → Datei nach {@code donebox}, Eintrag entfernen</li>
 *   <li>{@code ERROR} → Datei nach {@code errorbox}, Fehlerzähler erhöhen, Eintrag entfernen</li>
 *   <li>{@code PENDING} → Eintrag bleibt im Pool, nächster Prüfzeitpunkt wird gesetzt</li>
 * </ul>
 * Ist der Producer fertig, alle Submit-Antworten sind eingegangen und der Pool ist leer, gilt der Task als
 * abgeschlossen. Bei einem Abbruch endet die Schleife; noch offene Dateien bleiben in der {@code inbox}. Nur bei
 * Zeitüberschreitung werden sie wie Fehler behandelt (nach {@code errorbox}).
 */
public final class StatusConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(StatusConsumer.class);

    /** Puffer, damit der Client-Timeout (der eigentliche Grenzwert) vor dem Future-Timeout greift. */
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
            log.error("[{}] Consumer von Task {} ist fehlgeschlagen", context.userId(), context.taskId(), t);
            context.cancel(CancelReason.INTERNAL_ERROR);
        } finally {
            abandonRemaining();
            log.info("[{}] Task {} beendet: {}", context.userId(), context.taskId(), context.snapshot(0));
        }
    }

    private void loop() {
        while (!context.isCancelled()) {
            if (context.checkTimeout()) {
                failPendingAfterTimeout();
                return;
            }
            sweep();
            if (context.isCancelled()) {
                return;
            }
            if (isFinished()) {
                context.complete();
                return;
            }
            context.awaitCancel(properties.sweepInterval());
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
            if (context.isCancelled()) {
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
        log.warn("[{}] Task {} hat die maximale Laufzeit von {} überschritten", context.userId(), context.taskId(),
                properties.taskTimeout());
        pool.drain().values().forEach(entry -> {
            move(entry, false);
            context.recordError();
        });
    }

    /** Beim Beenden verbliebene Einträge freigeben; die Dateien bleiben in der {@code inbox}. */
    private void abandonRemaining() {
        int abandoned = pool.drain().size();
        if (abandoned > 0) {
            context.recordAbandoned(abandoned);
            log.info("[{}] Task {}: {} Dateien blieben unentschieden in der inbox", context.userId(),
                    context.taskId(), abandoned);
        }
    }
}
