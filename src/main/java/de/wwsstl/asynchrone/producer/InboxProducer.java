package de.wwsstl.asynchrone.producer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * Producer-Thread einer Sandbox (anforderungen_datenverarbeitung.md, Punkte 1 und 3): liest je Durchlauf bis zu
 * {@code batchSize} noch nicht übermittelte Dateien aus der {@code inbox}, übermittelt sie in einem Aufruf über
 * {@link CloudClient#submit(List)} an Cloud-API 1 und trägt die zurückgelieferten {@link SubmittedTask}-Objekte in
 * den <b>eigenen</b> Status-Pool ein.
 *
 * <p>Danach pausiert der Producer das Einlesen der nächsten Dateicharge, bis der {@code StatusConsumer} den
 * Status-Pool auf den konfigurierten Schwellenwert ({@code pipeline.pool-resume-threshold}) abgebaut hat. Das
 * Warten blockiert nur den (virtuellen) Producer-Thread selbst, nicht den nicht-blockierenden HTTP-Aufruf.
 *
 * <p>Der Producer beendet sich, wenn die {@code inbox} keine noch nicht übermittelten Dateien mehr enthält, oder
 * sobald das Abbruchsignal gesetzt ist. Erst danach meldet er {@link TaskContext#producerFinished()}.
 */
public final class InboxProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(InboxProducer.class);

    private final TaskContext context;
    private final StatusPool pool;
    private final UserFolders folders;
    private final CloudClient cloud;
    private final PipelineProperties properties;
    private final Clock clock;

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
        List<Path> batch;
        while (!context.isCancelled() && !(batch = nextBatch()).isEmpty()) {
            submit(batch);
            awaitResumeThreshold();
        }
    }

    /** Liest bis zu {@code batchSize} noch nicht übermittelte Dateien aus der {@code inbox}. */
    private List<Path> nextBatch() throws IOException {
        List<Path> batch = new ArrayList<>(properties.batchSize());
        try (DirectoryStream<Path> inbox = folders.openInbox()) {
            for (Path file : inbox) {
                if (context.isCancelled()) {
                    return List.of();
                }
                if (!seen.add(file)) {
                    continue; // liegt noch in der inbox, wurde aber schon übermittelt
                }
                batch.add(file);
                if (batch.size() == properties.batchSize()) {
                    break;
                }
            }
        }
        return batch;
    }

    /**
     * Pausiert das Einlesen der nächsten Dateicharge, bis die Anzahl der Dateien im Status-Pool auf
     * {@code pipeline.pool-resume-threshold} gesunken ist (anforderungen_datenverarbeitung.md, Punkte 1 und 3),
     * oder bis das Abbruchsignal gesetzt wird.
     */
    private void awaitResumeThreshold() {
        while (pool.size() > properties.poolResumeThreshold()) {
            if (context.awaitCancel(properties.sweepInterval())) {
                return;
            }
        }
    }

    /**
     * Übermittelt einen Batch in einem Aufruf an Cloud-API 1 und trägt die zurückgelieferten TaskIds in den Pool
     * ein (anforderungen_datenverarbeitung.md, Punkt 1). Der Producer-Thread wartet auf die Antwort; da er ein
     * virtueller Thread ist, blockiert das keinen Plattform-Thread.
     */
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
        if (items.isEmpty()) {
            return;
        }

        List<SubmittedTask> tasks;
        try {
            tasks = cloud.submit(items).join();
        } catch (RuntimeException e) {
            log.warn("[{}] Batch mit {} Dateien wurde von Cloud-API 1 abgelehnt: {}", context.userId(),
                    byName.size(), e.toString());
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
                pool.add(id, file.getValue(), clock.instant());
                context.recordSubmitted();
            }
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
