package de.wwsstl.asynchrone.taskmanager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import de.wwsstl.asynchrone.cloud.BatchJob;
import de.wwsstl.asynchrone.cloud.BatchJobId;
import de.wwsstl.asynchrone.cloud.BatchJobStatus;
import de.wwsstl.asynchrone.cloud.CloudCallFailedException;
import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.consumer.StatusConsumer;
import de.wwsstl.asynchrone.context.CancelReason;
import de.wwsstl.asynchrone.context.Sandbox;
import de.wwsstl.asynchrone.context.TaskContext;
import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.context.TaskState;
import de.wwsstl.asynchrone.files.UserFolderResolver;
import de.wwsstl.asynchrone.files.UserFolders;
import de.wwsstl.asynchrone.pool.InMemoryStatusPool;
import de.wwsstl.asynchrone.pool.StatusPool;
import de.wwsstl.asynchrone.producer.InboxProducer;
import de.wwsstl.asynchrone.registry.Run;
import de.wwsstl.asynchrone.registry.RunRegistry;
import de.wwsstl.asynchrone.registry.TaskRegistry;
import jakarta.annotation.PreDestroy;

/**
 * Task-Scheduling- und Isolationszentrum (loesung_final.md 4.2, funktionsweise_sequenz.md).
 *
 * <p><b>Singleton über die gesamte Laufzeit:</b> Der Task Manager ist eine einzige Spring-Bean und hält keinen
 * eigenen Task-Bestand. Belegungen, Laufnummern und Endzustände stehen im {@link RunRegistry Laufregister}, die
 * lebenden Sandboxen dieser Instanz in der lokalen {@link TaskRegistry}.
 *
 * <p><b>Start in vier Schritten</b> (Abschnitt 1), Prüfen und Belegen jeweils atomar im Laufregister:
 * <ol>
 *   <li>Benutzer:in belegen — sonst {@link RejectReason#USER_TASK_RUNNING}. Das geschieht vor jedem Aufruf von
 *       Cloud-API 1, damit kein ungenutzter BatchgenAuftrag entsteht.</li>
 *   <li>BatchgenAuftrag bestimmen: einen laufenden über Cloud-API 3 fortsetzen; ohne einen solchen bei nicht leerer
 *       {@code pendingbox} {@link RejectReason#PENDINGBOX_NOT_EMPTY}; sonst über Cloud-API 1 neu anlegen.</li>
 *   <li>Aufgabennummer (= BatchgenAuftrag-Nummer) belegen und die Laufnummer vergeben — sonst
 *       {@link RejectReason#TASK_ALREADY_RUNNING}.</li>
 *   <li>Sandbox aufbauen ({@link TaskContext}, exklusiver {@link StatusPool}, Producer- und Consumer-Thread als
 *       Virtual Threads), in die lokale Registry eintragen und starten.</li>
 * </ol>
 *
 * <p><b>Abbruch</b> (Abschnitt 5) liest und schreibt keinen Zustand der Instanz: Er markiert den BatchgenAuftrag
 * über Cloud-API 3 als {@code CANCELLED}; der Consumer erkennt das im nächsten Durchlauf. Zeitüberschreitung und
 * Fehlerschwelle meldet die Sandbox als {@link de.wwsstl.asynchrone.context.AbortHandler AbortHandler} hierher; der
 * Task Manager markiert den BatchgenAuftrag dann ebenso (Abschnitt 6).
 *
 * <p><b>Lebensende eines Laufs:</b> Sobald beide Threads ausgelaufen sind, meldet der Task Manager den Lauf ab. Bei
 * {@code COMPLETED} markiert er zuerst den BatchgenAuftrag über Cloud-API 3 als abgeschlossen. Dann legt er die
 * letzte Momentaufnahme als Endzustand im Laufregister ab und entfernt die Sandbox aus der Registry. Der Endzustand
 * bleibt abfragbar, bis {@code pipeline.task-retention} abgelaufen ist; abgelaufene Einträge werden ohne eigenen
 * Scheduler bei jedem {@link #start} entfernt.
 */
@Service
public class TaskManager {

    private static final Logger log = LoggerFactory.getLogger(TaskManager.class);

    /** Puffer, damit der Client-Timeout (der eigentliche Grenzwert) vor dem Future-Timeout greift. */
    private static final Duration RESULT_GRACE = Duration.ofSeconds(5);

    private final TaskRegistry registry;
    private final RunRegistry runs;
    private final UserFolderResolver folderResolver;
    private final CloudClient cloud;
    private final PipelineProperties properties;
    private final Clock clock;

    public TaskManager(TaskRegistry registry, RunRegistry runs, UserFolderResolver folderResolver,
            CloudClient cloud, PipelineProperties properties, Clock clock) {
        this.registry = registry;
        this.runs = runs;
        this.folderResolver = folderResolver;
        this.cloud = cloud;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Startet einen neuen Lauf für die Benutzer:in — unter einem fortgesetzten oder einem neuen BatchgenAuftrag.
     *
     * @throws TaskRejectedException     mit {@code USER_TASK_RUNNING}, {@code PENDINGBOX_NOT_EMPTY} oder
     *                                   {@code TASK_ALREADY_RUNNING}
     * @throws CloudCallFailedException  wenn Cloud-API 1 oder 3 nicht erreichbar war
     */
    public TaskSnapshot start(String userId) {
        UserFolders folders = folderResolver.resolve(userId);
        evictExpired();
        Run run = runs.claimUser(userId).orElseThrow(() -> new TaskRejectedException(RejectReason.USER_TASK_RUNNING,
                "Für Benutzer '" + userId + "' läuft bereits eine Aufgabe"));
        try {
            BatchJobId jobId = batchJobFor(folders);
            if (!runs.claimTask(run, jobId)) {
                throw new TaskRejectedException(RejectReason.TASK_ALREADY_RUNNING,
                        "Unter Aufgabennummer " + jobId + " läuft bereits eine Aufgabe");
            }
        } catch (RuntimeException e) {
            runs.release(run);
            throw e;
        }
        Sandbox sandbox = launch(folders, run);
        log.info("[{}] Task {} Lauf {} gestartet ({} Tasks im Register)", userId, run.jobId(), run.number(),
                registry.size());
        return sandbox.snapshot();
    }

    /**
     * Start, Schritt 2: der BatchgenAuftrag, unter dem der Lauf arbeitet.
     *
     * @throws TaskRejectedException mit {@code PENDINGBOX_NOT_EMPTY}, wenn kein BatchgenAuftrag läuft, die
     *                               {@code pendingbox} aber Dateien eines abgebrochenen enthält
     */
    private BatchJobId batchJobFor(UserFolders folders) {
        String userId = folders.userId();
        Optional<BatchJobId> running = await(cloud.findRunningBatchJob(userId), statusWait(),
                "Cloud-API 3 (laufenden BatchgenAuftrag suchen)");
        if (running.isPresent()) {
            log.info("[{}] Laufender BatchgenAuftrag {} wird fortgesetzt", userId, running.get());
            return running.get();
        }
        if (hasPendingFiles(folders)) {
            throw new TaskRejectedException(RejectReason.PENDINGBOX_NOT_EMPTY, "Die pendingbox von Benutzer '"
                    + userId + "' enthält Dateien eines abgebrochenen BatchgenAuftrags: zuerst fortsetzen (den "
                    + "BatchgenAuftrag wieder auf RUNNING setzen) oder die pendingbox leeren");
        }
        BatchJobId created = await(cloud.createBatchJob(userId), submitWait(), "Cloud-API 1 (BatchgenAuftrag anlegen)");
        log.info("[{}] BatchgenAuftrag {} angelegt", userId, created);
        return created;
    }

    private static boolean hasPendingFiles(UserFolders folders) {
        try {
            return folders.hasPendingFiles();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Abbruch von außen: markiert den BatchgenAuftrag über Cloud-API 3 als {@code CANCELLED}, ohne Zustand dieser
     * Instanz zu lesen oder zu schreiben. Ein bereits abgebrochener BatchgenAuftrag bleibt unverändert.
     *
     * @throws TaskNotFoundException     wenn es den BatchgenAuftrag nicht gibt oder er einer anderen Benutzer:in gehört
     * @throws TaskRejectedException     mit {@code TASK_NOT_RUNNING}, wenn der BatchgenAuftrag abgeschlossen ist
     * @throws CloudCallFailedException  wenn Cloud-API 3 nicht erreichbar war
     */
    public CancelResult cancel(String userId, String taskId) {
        BatchJobId jobId = new BatchJobId(taskId);
        BatchJob job = await(cloud.batchJob(jobId), statusWait(), "Cloud-API 3 (BatchgenAuftrag lesen)")
                .filter(found -> found.userId().equals(userId))
                .orElseThrow(() -> new TaskNotFoundException(userId, taskId));
        switch (job.status()) {
            case COMPLETED -> throw new TaskRejectedException(RejectReason.TASK_NOT_RUNNING,
                    "Aufgabe " + taskId + " ist bereits abgeschlossen");
            case RUNNING -> {
                await(cloud.setBatchJobStatus(jobId, BatchJobStatus.CANCELLED), statusWait(),
                        "Cloud-API 3 (BatchgenAuftrag abbrechen)");
                log.info("[{}] BatchgenAuftrag {} als CANCELLED markiert", userId, jobId);
            }
            case CANCELLED -> log.debug("[{}] BatchgenAuftrag {} ist bereits abgebrochen", userId, jobId);
        }
        return new CancelResult(jobId.value(), TaskState.CANCELLING);
    }

    /**
     * Laufender Lauf: aktueller Zustand aus der Sandbox, {@code CANCELLING}, sobald Cloud-API 3 den BatchgenAuftrag
     * als {@code CANCELLED} meldet. Sonst der Endzustand des letzten Laufs aus dem Laufregister.
     *
     * @throws TaskNotFoundException wenn es keinen Lauf (mehr) gibt oder er einer anderen Benutzer:in gehört
     */
    public TaskSnapshot status(String userId, String taskId) {
        BatchJobId jobId = new BatchJobId(taskId);
        Optional<Sandbox> live = registry.find(jobId).filter(sandbox -> sandbox.context().userId().equals(userId));
        if (live.isPresent()) {
            TaskSnapshot snapshot = live.get().snapshot();
            return snapshot.state() == TaskState.RUNNING && cancelRequested(jobId)
                    ? snapshot.withState(TaskState.CANCELLING)
                    : snapshot;
        }
        return runs.latestFinished(jobId)
                .filter(finished -> finished.userId().equals(userId))
                .filter(finished -> finished.finishedAt().isAfter(retentionCutoff()))
                .orElseThrow(() -> new TaskNotFoundException(userId, taskId));
    }

    /** Cloud-API 3 meldet den BatchgenAuftrag als {@code CANCELLED}; ist sie nicht erreichbar, gilt: nein. */
    private boolean cancelRequested(BatchJobId jobId) {
        try {
            return await(cloud.batchJob(jobId), statusWait(), "Cloud-API 3 (BatchgenAuftrag lesen)")
                    .map(BatchJob::status).orElse(null) == BatchJobStatus.CANCELLED;
        } catch (CloudCallFailedException e) {
            log.debug("Abbruchstatus von {} nicht abrufbar: {}", jobId, e.toString());
            return false;
        }
    }

    /**
     * Start, Schritt 4: baut die Sandbox auf, trägt sie in die Registry ein und startet erst dann ihre Threads. Ab
     * dem Eintrag meldet die Sandbox den Lauf selbst ab; scheitert schon der Eintrag, wird der Lauf freigegeben.
     */
    private Sandbox launch(UserFolders folders, Run run) {
        TaskContext context = new TaskContext(folders.userId(), run.jobId(), run.number(), properties.taskTimeout(),
                properties.errorThreshold(), clock);
        StatusPool pool = new InMemoryStatusPool();

        // Der zuletzt auslaufende der beiden Threads meldet den Lauf ab.
        AtomicReference<Sandbox> self = new AtomicReference<>();
        AtomicInteger runningThreads = new AtomicInteger(2);
        Runnable onThreadExit = () -> {
            if (runningThreads.decrementAndGet() == 0) {
                deregister(self.get(), run);
            }
        };
        String name = context.jobId() + "-" + context.run();
        Thread producer = Thread.ofVirtual().name("producer-" + name).unstarted(
                andThen(new InboxProducer(context, pool, folders, cloud, properties, clock), onThreadExit));
        Thread consumer = Thread.ofVirtual().name("consumer-" + name).unstarted(andThen(
                new StatusConsumer(context, pool, folders, cloud, this::abortBatchJob, properties, clock),
                onThreadExit));
        Sandbox sandbox = new Sandbox(context, pool, producer, consumer);
        self.set(sandbox);

        // Erst registrieren: Der Lauf ist damit ab dem ersten Moment abfragbar.
        try {
            registry.register(sandbox);
        } catch (RuntimeException e) {
            runs.release(run);
            throw e;
        }
        try {
            sandbox.start();
        } catch (RuntimeException e) {
            context.cancel(CancelReason.INTERNAL_ERROR);
            // Ein nie gestarteter Thread läuft auch nie aus; für ihn wird hier abgezählt.
            for (Thread thread : new Thread[] {producer, consumer}) {
                if (thread.getState() == Thread.State.NEW && runningThreads.decrementAndGet() == 0) {
                    deregister(sandbox, run);
                }
            }
            throw e;
        }
        return sandbox;
    }

    private static Runnable andThen(Runnable task, Runnable after) {
        return () -> {
            try {
                task.run();
            } finally {
                after.run();
            }
        };
    }

    /**
     * {@link de.wwsstl.asynchrone.context.AbortHandler AbortHandler} der Sandboxen: Zeitüberschreitung oder
     * Fehlerschwelle markieren den BatchgenAuftrag über Cloud-API 3 als {@code CANCELLED}.
     */
    private boolean abortBatchJob(TaskContext context, CancelReason reason) {
        try {
            await(cloud.setBatchJobStatus(context.jobId(), BatchJobStatus.CANCELLED), statusWait(),
                    "Cloud-API 3 (BatchgenAuftrag abbrechen)");
            log.info("[{}] Task {}: {} — BatchgenAuftrag als CANCELLED markiert", context.userId(), context.jobId(),
                    reason);
            return true;
        } catch (CloudCallFailedException e) {
            log.warn("[{}] Task {}: {} — BatchgenAuftrag konnte nicht als CANCELLED markiert werden: {}",
                    context.userId(), context.jobId(), reason, e.toString());
            return false;
        }
    }

    /**
     * Meldet einen Lauf ab, dessen Threads beide ausgelaufen sind: bei {@code COMPLETED} den BatchgenAuftrag
     * abschließen, Endzustand ins Laufregister, Sandbox aus der Registry. Danach referenziert nichts mehr Threads,
     * Status-Pool oder Kontext.
     */
    private void deregister(Sandbox sandbox, Run run) {
        TaskContext context = sandbox.context();
        // Beide Threads sind beendet, also muss es auch der Lauf sein; sichert den Endzeitpunkt für die Frist.
        if (context.cancel(CancelReason.INTERNAL_ERROR)) {
            log.warn("[{}] Task {} lief nach Ende seiner Threads noch", context.userId(), context.jobId());
        }
        if (context.state() == TaskState.COMPLETED) {
            completeBatchJob(context);
        }
        // Erst ablegen, dann entfernen: Die Status-Abfrage findet den Lauf so zu jedem Zeitpunkt.
        runs.finish(run, sandbox.snapshot());
        registry.remove(sandbox);
        log.info("[{}] Task {} Lauf {} abgemeldet; Endzustand {} bleibt bis {} abfragbar ({} Tasks im Register)",
                context.userId(), context.jobId(), context.run(), context.state(),
                context.finishedAt().plus(properties.taskRetention()), registry.size());
    }

    private void completeBatchJob(TaskContext context) {
        try {
            await(cloud.setBatchJobStatus(context.jobId(), BatchJobStatus.COMPLETED), statusWait(),
                    "Cloud-API 3 (BatchgenAuftrag abschließen)");
        } catch (CloudCallFailedException e) {
            // Der BatchgenAuftrag bleibt RUNNING; der nächste Start setzt ihn fort, ohne Dateien erneut zu übermitteln.
            log.error("[{}] BatchgenAuftrag {} konnte nicht als COMPLETED markiert werden: {}", context.userId(),
                    context.jobId(), e.toString());
        }
    }

    /** Entfernt alle Endzustände, deren Aufbewahrungsfrist abgelaufen ist. */
    private void evictExpired() {
        int evicted = runs.removeFinishedUntil(retentionCutoff());
        if (evicted > 0) {
            log.info("{} Endzustände nach Ablauf der Aufbewahrungsfrist ({}) aus dem Laufregister entfernt", evicted,
                    properties.taskRetention());
        }
    }

    /** Wer spätestens zu diesem Zeitpunkt beendet wurde, hat die Aufbewahrungsfrist überschritten. */
    private Instant retentionCutoff() {
        return clock.instant().minus(properties.taskRetention());
    }

    private Duration submitWait() {
        PipelineProperties.Cloud config = properties.cloud();
        return config.submitTimeout().multipliedBy(config.submitRetries() + 1L).plus(RESULT_GRACE);
    }

    private Duration statusWait() {
        PipelineProperties.Cloud config = properties.cloud();
        return config.statusTimeout().multipliedBy(config.statusRetries() + 1L).plus(RESULT_GRACE);
    }

    private static <T> T await(CompletableFuture<T> call, Duration wait, String description) {
        try {
            return call.get(wait.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CloudCallFailedException(description, e);
        } catch (ExecutionException e) {
            throw new CloudCallFailedException(description, e.getCause());
        } catch (TimeoutException e) {
            call.cancel(true);
            throw new CloudCallFailedException(description, e);
        }
    }

    /**
     * Beim Herunterfahren laufende Tasks abbrechen. Der BatchgenAuftrag bleibt dabei {@code RUNNING}, damit der
     * nächste Start die Aufgabe fortsetzt.
     */
    @PreDestroy
    void shutdown() {
        registry.all().forEach(sandbox -> sandbox.context().cancel(CancelReason.SHUTDOWN));
    }
}
