package de.wwsstl.asynchrone.producer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.SubmittedTask;
import de.wwsstl.asynchrone.cloud.TestdataItem;
import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.TaskContext;
import de.wwsstl.asynchrone.files.UserFolders;
import de.wwsstl.asynchrone.pool.StatusPool;
import de.wwsstl.asynchrone.pool.TaskId;

/**
 * Producer-Thread einer Sandbox (loesung_final.md 4.7): liest die {@code inbox} batchweise, übermittelt jeden
 * Batch nicht-blockierend an Cloud-API 1 und trägt die zurückgelieferten TaskIds in den <b>eigenen</b>
 * Status-Pool ein. Er liest sofort weiter, statt auf die Antwort oder auf die Verarbeitung früherer Batches zu
 * warten; nur die Zahl unbeantworteter Submit-Aufrufe ist begrenzt ({@code max-in-flight-batches}).
 *
 * <p>Der Producer beendet sich, wenn die {@code inbox} keine noch nicht übermittelten Dateien mehr enthält, oder
 * sobald das Abbruchsignal gesetzt ist. Erst danach meldet er {@link TaskContext#producerFinished()}.
 */
public final class InboxProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(InboxProducer.class);
    private static final Duration SLOT_POLL = Duration.ofMillis(100);

    private final TaskContext context;
    private final StatusPool pool;
    private final UserFolders folders;
    private final CloudClient cloud;
    private final PipelineProperties properties;
    private final Clock clock;
    private final Semaphore inFlightSlots;

    /** Bereits übermittelte Dateien; nur vom Producer-Thread selbst benutzt. */
    private final Set<Path> seen = new HashSet<>();

    public InboxProducer(TaskContext context, StatusPool pool, UserFolders folders, CloudClient cloud,
            PipelineProperties properties, Clock clock) {
        this.context = context;
        this.pool = pool;
        this.folders = folders;
        this.cloud = cloud;
        this.properties = properties;
        this.clock = clock;
        this.inFlightSlots = new Semaphore(properties.maxInFlightBatches());
    }

    @Override
    public void run() {
        try {
            produce();
        } catch (Throwable t) {
            log.error("[{}] Producer von Task {} ist fehlgeschlagen", context.userId(), context.taskId(), t);
            context.cancel(CancelReason.INTERNAL_ERROR);
        } finally {
            context.producerFinished();
            log.debug("[{}] Producer von Task {} beendet", context.userId(), context.taskId());
        }
    }

    private void produce() throws IOException {
        boolean foundNewFiles;
        do {
            foundNewFiles = false;
            List<Path> batch = new ArrayList<>(properties.batchSize());
            try (DirectoryStream<Path> inbox = folders.openInbox()) {
                for (Path file : inbox) {
                    if (context.isCancelled()) {
                        return;
                    }
                    if (!seen.add(file)) {
                        continue; // liegt noch in der inbox, wurde aber schon übermittelt
                    }
                    foundNewFiles = true;
                    batch.add(file);
                    if (batch.size() == properties.batchSize()) {
                        submit(batch);
                        batch = new ArrayList<>(properties.batchSize());
                    }
                }
            }
            if (!batch.isEmpty() && !context.isCancelled()) {
                submit(batch);
            }
            // Dateien, die während des Durchlaufs eintrafen, werden im nächsten Durchlauf erfasst.
        } while (foundNewFiles && !context.isCancelled());
    }

    private void submit(List<Path> files) {
        Map<String, Path> byName = new HashMap<>();
        List<TestdataItem> items = new ArrayList<>(files.size());
        for (Path file : files) {
            try {
                items.add(new TestdataItem(file.getFileName().toString(), Files.readString(file)));
                byName.put(file.getFileName().toString(), file);
            } catch (IOException | UncheckedIOException e) {
                log.warn("[{}] Datei {} nicht lesbar: {}", context.userId(), file.getFileName(), e.toString());
                fail(file);
            }
        }
        if (items.isEmpty() || !acquireSlot()) {
            context.recordAbandoned(byName.size());
            return;
        }

        context.batchStarted();
        CompletableFuture<List<SubmittedTask>> response;
        try {
            response = cloud.submit(items);
        } catch (RuntimeException e) {
            response = CompletableFuture.failedFuture(e);
        }
        // Die Antwort wird auf einem eigenen Virtual Thread verarbeitet: Dateioperationen dürfen den
        // Netzwerk-Thread des HTTP-Clients nicht blockieren, und der Producer liest bereits weiter.
        response.whenComplete((tasks, error) -> Thread.ofVirtual()
                .name("submit-result-" + context.taskId())
                .start(() -> onSubmitted(byName, tasks, error)));
    }

    /** Wartet auf einen freien In-Flight-Platz; bricht ab, sobald das Abbruchsignal gesetzt ist. */
    private boolean acquireSlot() {
        try {
            while (!inFlightSlots.tryAcquire(SLOT_POLL.toMillis(), TimeUnit.MILLISECONDS)) {
                if (context.isCancelled()) {
                    return false;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            context.cancel(CancelReason.SHUTDOWN);
            return false;
        }
        if (context.isCancelled()) {
            inFlightSlots.release();
            return false;
        }
        return true;
    }

    private void onSubmitted(Map<String, Path> byName, List<SubmittedTask> tasks, Throwable error) {
        try {
            if (error != null || tasks == null) {
                log.warn("[{}] Batch mit {} Dateien wurde von Cloud-API 1 abgelehnt: {}", context.userId(),
                        byName.size(), error);
                byName.values().forEach(this::fail);
                return;
            }
            Map<String, TaskId> ids = new HashMap<>();
            tasks.forEach(task -> ids.put(task.fileName(), task.taskId()));
            for (Map.Entry<String, Path> file : byName.entrySet()) {
                TaskId id = ids.get(file.getKey());
                if (id == null) {
                    log.warn("[{}] Cloud-API 1 lieferte keine TaskId für {}", context.userId(), file.getKey());
                    fail(file.getValue());
                } else if (context.isCancelled()) {
                    context.recordAbandoned(1);
                } else {
                    // Erst in den Pool, dann Batch als beantwortet melden (siehe StatusConsumer#isFinished).
                    pool.add(id, file.getValue(), clock.instant());
                    context.recordSubmitted();
                }
            }
        } catch (RuntimeException e) {
            log.error("[{}] Verarbeitung der Submit-Antwort von Task {} ist fehlgeschlagen", context.userId(),
                    context.taskId(), e);
            context.cancel(CancelReason.INTERNAL_ERROR);
        } finally {
            context.batchSettled();
            inFlightSlots.release();
        }
    }

    /** Datei in die {@code errorbox} legen und als Fehler zählen. */
    private void fail(Path file) {
        try {
            folders.moveToError(file);
        } catch (IOException e) {
            log.error("[{}] Datei {} konnte nicht in die errorbox verschoben werden", context.userId(),
                    file.getFileName(), e);
        }
        context.recordError();
    }
}
