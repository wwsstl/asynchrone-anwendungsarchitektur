package de.wwsstl.asynchrone.producer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.CloudErrors;
import de.wwsstl.asynchrone.cloud.SubmitFailedException;
import de.wwsstl.asynchrone.cloud.SubmittedFile;
import de.wwsstl.asynchrone.cloud.TaskId;
import de.wwsstl.asynchrone.cloud.TestdataItem;
import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.files.UserFolders;
import de.wwsstl.asynchrone.pool.StatusPool;
import de.wwsstl.asynchrone.task.TaskContext;
import de.wwsstl.asynchrone.task.TaskState;

/**
 * Producer-Thread einer Sandbox (funktionsweise_sequenz.md, Abschnitt 2). Er arbeitet die {@code inbox} batchweise
 * ab:
 * <ol>
 *   <li>bis zu {@code batch-size} Dateien in die {@code pendingbox} verschieben, noch vor dem Aufruf von Cloud-API 1,
 *       damit keine Datei doppelt übermittelt wird (Cloud-API 1 ist nicht idempotent);</li>
 *   <li>ihre Daten zusammen mit der Aufgabennummer in einer Anfrage an Cloud-API 1 übermitteln;</li>
 *   <li>für erfolgreich umgewandelte Dateien die TaskId als Marker sichern und in den Status-Pool eintragen;</li>
 *   <li>nicht umgewandelte Dateien in die {@code errorbox} verschieben und als Fehler zählen;</li>
 *   <li>warten, bis der Status-Pool auf {@code pool-resume-threshold} gesunken ist (Rückstau).</li>
 * </ol>
 * Schlägt der Aufruf fehl, entscheidet die Art des Fehlers: Eindeutig nicht verarbeitete Dateien wandern zurück in die
 * {@code inbox} und werden nach einer Wartezeit erneut übermittelt. Bei unklarem Ergebnis (z. B. keine Antwort
 * innerhalb von {@code submit-timeout}) bleiben sie ohne Marker in der {@code pendingbox}, und die Aufgabe endet
 * sofort mit {@code TIMEOUT}; es wird kein weiterer Batch übermittelt.
 *
 * <p>Der Producer ist fertig, sobald die {@code inbox} leer ist oder das Abbruchsignal gesetzt wird.
 */
public final class InboxProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(InboxProducer.class);

    /** Puffer, damit der Timeout des Clients (der eigentliche Grenzwert) vor dem Timeout des Futures greift. */
    private static final Duration RESULT_GRACE = Duration.ofSeconds(5);

    private static final String SUBMIT = "Cloud-API 1 (Batch übermitteln)";

    private enum Outcome { SUBMITTED, NOT_PROCESSED, UNCLEAR }

    private final TaskContext context;
    private final StatusPool pool;
    private final UserFolders folders;
    private final CloudClient cloud;
    private final PipelineProperties properties;
    private final Clock clock;

    /** Dateien, die sich nicht in die pendingbox verschieben ließen; sie werden in dieser Aufgabe übersprungen. */
    private final Set<Path> unmovable = new HashSet<>();

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
            log.error("[{}] Producer von Aufgabe {} ist fehlgeschlagen", context.userId(), context.taskNumber(), t);
            context.finish(TaskState.ABORTED, "Producer fehlgeschlagen: " + t);
        } finally {
            context.producerFinished();
            log.info("[{}] Producer von Aufgabe {} beendet", context.userId(), context.taskNumber());
        }
    }

    private void produce() throws IOException {
        int failedAttempts = 0;
        while (!context.isStopping()) {
            List<Path> inboxFiles = folders.listInbox(properties.batchSize(), unmovable);
            if (inboxFiles.isEmpty()) {
                log.info("[{}] Aufgabe {}: inbox abgearbeitet", context.userId(), context.taskNumber());
                return;
            }
            List<Path> batch = claim(inboxFiles);
            if (batch.isEmpty()) {
                continue;
            }
            Outcome outcome = submit(batch);
            if (outcome == Outcome.UNCLEAR) {
                return;
            }
            if (outcome == Outcome.NOT_PROCESSED) {
                failedAttempts++;
                context.awaitStop(retryWait(failedAttempts));
                continue;
            }
            failedAttempts = 0;
            awaitPoolBelowThreshold();
        }
    }

    /** Verschiebt die Dateien vor dem Aufruf von Cloud-API 1 in die pendingbox. */
    private List<Path> claim(List<Path> inboxFiles) {
        List<Path> batch = new ArrayList<>(inboxFiles.size());
        for (Path inboxFile : inboxFiles) {
            try {
                batch.add(folders.moveToPending(inboxFile));
            } catch (IOException e) {
                log.warn("[{}] Datei {} lässt sich nicht in die pendingbox verschieben und wird übersprungen: {}",
                        context.userId(), inboxFile.getFileName(), e.toString());
                unmovable.add(inboxFile);
            }
        }
        return batch;
    }

    private Outcome submit(List<Path> batch) {
        Map<String, Path> filesByName = new LinkedHashMap<>();
        List<TestdataItem> items = new ArrayList<>(batch.size());
        for (Path file : batch) {
            String name = file.getFileName().toString();
            try {
                items.add(new TestdataItem(name, Files.readString(file)));
                filesByName.put(name, file);
            } catch (IOException | UncheckedIOException e) {
                log.warn("[{}] Datei {} ist nicht lesbar und kommt in die errorbox: {}", context.userId(), name,
                        e.toString());
                fail(file);
            }
        }
        if (items.isEmpty()) {
            return Outcome.SUBMITTED;
        }

        List<SubmittedFile> accepted;
        Future<List<SubmittedFile>> call = cloud.submit(context.taskNumber(), items);
        try {
            Duration wait = properties.cloud().submitTimeout().plus(RESULT_GRACE);
            accepted = call.get(wait.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SubmitFailedException failure
                    && failure.kind() == SubmitFailedException.Kind.NOT_PROCESSED) {
                returnToInbox(filesByName.values(), failure);
                return Outcome.NOT_PROCESSED;
            }
            return leaveUnclear(filesByName.size(), TaskState.TIMEOUT, unclearReason(e.getCause()));
        } catch (TimeoutException e) {
            call.cancel(true);
            return leaveUnclear(filesByName.size(), TaskState.TIMEOUT, unclearReason(e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return leaveUnclear(filesByName.size(), TaskState.ABORTED, "beim Warten auf " + SUBMIT + " unterbrochen");
        }

        Map<String, TaskId> taskIds = new HashMap<>();
        accepted.forEach(file -> taskIds.put(file.fileName(), file.taskId()));
        int rejected = 0;
        for (Map.Entry<String, Path> entry : filesByName.entrySet()) {
            TaskId taskId = taskIds.get(entry.getKey());
            if (taskId == null) {
                fail(entry.getValue());
                rejected++;
            } else {
                track(entry.getValue(), taskId);
            }
        }
        log.info("[{}] Aufgabe {}: Batch mit {} Dateien übermittelt, {} umgewandelt, {} nicht umgewandelt",
                context.userId(), context.taskNumber(), filesByName.size(), filesByName.size() - rejected, rejected);
        return Outcome.SUBMITTED;
    }

    /** Sichert die TaskId als Marker und trägt die Datei in den Status-Pool ein. */
    private void track(Path file, TaskId taskId) {
        try {
            folders.writeMarker(file, taskId);
        } catch (IOException e) {
            log.error("[{}] TaskId {} für {} konnte nicht als Marker gesichert werden", context.userId(), taskId,
                    file.getFileName(), e);
        }
        context.recordSubmitted();
        // Nach dem Abbruchsignal bleibt die Datei mit Marker in der pendingbox (funktionsweise_sequenz.md, Abschnitt 7).
        if (!context.isStopping()) {
            pool.add(taskId, file, clock.instant());
        }
    }

    /** Eindeutig nicht verarbeitet: zurück in die inbox, der Producer liest die Dateien nach einer Wartezeit erneut. */
    private void returnToInbox(Iterable<Path> files, SubmitFailedException failure) {
        int returned = 0;
        for (Path file : files) {
            try {
                folders.moveBackToInbox(file);
                returned++;
            } catch (IOException e) {
                log.error("[{}] Datei {} konnte nicht zurück in die inbox verschoben werden und bleibt ohne Marker in "
                        + "der pendingbox", context.userId(), file.getFileName(), e);
                context.recordUnclear(1);
            }
        }
        log.warn("[{}] Aufgabe {}: Cloud-API 1 hat den Batch nicht verarbeitet, {} Dateien zurück in die inbox: {}",
                context.userId(), context.taskNumber(), returned, failure.getCause().toString());
    }

    /**
     * Ergebnis unklar: Die Dateien bleiben ohne Marker in der pendingbox und werden nicht erneut übermittelt. Darüber
     * hinaus geschieht nichts mehr: Die Aufgabe endet sofort mit {@code endState} (TIMEOUT, beim Beenden der Anwendung
     * ABORTED), und es wird kein weiterer Batch übermittelt.
     */
    private Outcome leaveUnclear(int files, TaskState endState, String reason) {
        context.recordUnclear(files);
        context.finish(endState, reason);
        log.warn("[{}] Aufgabe {} endet mit {} ({}); {} Dateien bleiben ohne Marker in der pendingbox",
                context.userId(), context.taskNumber(), context.state(), context.reason(), files);
        return Outcome.UNCLEAR;
    }

    /** Grund des Endzustands nach einem unklaren Ergebnis: der Aufruf und seine Ursache. */
    private static String unclearReason(Throwable cause) {
        return SUBMIT + ": Ergebnis unklar, " + CloudErrors.describe(cause, "submit-timeout");
    }

    /** Datei in die errorbox verschieben und als Fehler zählen. */
    private void fail(Path file) {
        try {
            folders.moveToError(file);
        } catch (IOException e) {
            log.error("[{}] Datei {} konnte nicht in die errorbox verschoben werden", context.userId(),
                    file.getFileName(), e);
        }
        context.recordError();
    }

    /** Rückstau: erst weiterlesen, wenn der Status-Pool auf {@code pool-resume-threshold} gesunken ist. */
    private void awaitPoolBelowThreshold() {
        while (pool.size() > properties.poolResumeThreshold()) {
            if (context.awaitStop(properties.sweepInterval())) {
                return;
            }
        }
    }

    /** Wartezeit vor dem nächsten Versuch: beginnt bei {@code sweep-interval} und verdoppelt sich bis zur Obergrenze. */
    private Duration retryWait(int failedAttempts) {
        Duration wait = properties.sweepInterval();
        for (int i = 1; i < failedAttempts && wait.compareTo(properties.submitRetryMaxWait()) < 0; i++) {
            wait = wait.multipliedBy(2);
        }
        return wait.compareTo(properties.submitRetryMaxWait()) > 0 ? properties.submitRetryMaxWait() : wait;
    }
}
