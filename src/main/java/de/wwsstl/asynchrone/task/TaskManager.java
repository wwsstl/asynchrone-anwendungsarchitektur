package de.wwsstl.asynchrone.task;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import de.wwsstl.asynchrone.cloud.BatchJob;
import de.wwsstl.asynchrone.cloud.CloudClient;
import de.wwsstl.asynchrone.cloud.CloudUnavailableException;
import de.wwsstl.asynchrone.cloud.JobStatus;
import de.wwsstl.asynchrone.config.PipelineProperties;
import de.wwsstl.asynchrone.consumer.StatusConsumer;
import de.wwsstl.asynchrone.files.UserFolderResolver;
import de.wwsstl.asynchrone.files.UserFolders;
import de.wwsstl.asynchrone.pool.InMemoryStatusPool;
import de.wwsstl.asynchrone.pool.StatusPool;
import de.wwsstl.asynchrone.producer.InboxProducer;
import jakarta.annotation.PreDestroy;

/**
 * Zentrales Task-Management (funktionsweise_sequenz.md). Der TaskManager ist eine einzige Spring-Bean, die über die
 * gesamte Laufzeit besteht; die laufenden Sandboxen stehen in der {@link TaskRegistry}.
 *
 * <ul>
 *   <li><b>Start</b> (Abschnitt 1): {@code pendingbox} prüfen, über Cloud-API 1 einen BatchgenAuftrag anlegen, Sandbox
 *       aufbauen, eintragen und starten. Je Benutzer:in läuft höchstens eine Aufgabe gleichzeitig.</li>
 *   <li><b>Statusabfrage</b> (Abschnitt 4): aus der Cloud, während der Laufzeit zusätzlich lokal aus der Sandbox.</li>
 *   <li><b>Abbruch von außen</b> (Abschnitt 5): Abbruchsignal setzen; nach dem Ende der Threads wird {@code TERMINATED}
 *       geschrieben.</li>
 *   <li><b>Abmeldung</b> (Abschnitt 6): Sind beide Threads einer Sandbox beendet, schreibt der TaskManager den
 *       Endzustand über Cloud-API 3 und entfernt die Sandbox aus der TaskRegistry.</li>
 * </ul>
 */
@Service
public class TaskManager {

    private static final Logger log = LoggerFactory.getLogger(TaskManager.class);

    /** Puffer, damit der Timeout des Clients (der eigentliche Grenzwert) vor dem Timeout des Futures greift. */
    private static final Duration RESULT_GRACE = Duration.ofSeconds(5);

    /** So lange wartet ein Start, bis die gerade endende Aufgabe derselben Benutzer:in abgemeldet ist. */
    private static final Duration STOP_WAIT = Duration.ofSeconds(10);

    /** Belegung einer Benutzer:in, vom Start bis zur Abmeldung ihrer Sandbox. */
    private static final class UserSlot {
        private volatile Sandbox sandbox;
        private volatile Thread supervisor;
    }

    private final TaskRegistry registry;
    private final UserFolderResolver folderResolver;
    private final CloudClient cloud;
    private final PipelineProperties properties;
    private final Clock clock;

    /** Je Benutzer:in höchstens eine Aufgabe; der Eintrag besteht vom Start bis zur Abmeldung. */
    private final ConcurrentHashMap<String, UserSlot> users = new ConcurrentHashMap<>();

    public TaskManager(TaskRegistry registry, UserFolderResolver folderResolver, CloudClient cloud,
            PipelineProperties properties, Clock clock) {
        this.registry = registry;
        this.folderResolver = folderResolver;
        this.cloud = cloud;
        this.properties = properties;
        this.clock = clock;
    }

    // --- 1. Start --------------------------------------------------------------------------------------------

    /**
     * Startet eine Aufgabe für die Benutzer:in.
     *
     * @throws TaskRejectedException     mit {@code USER_TASK_RUNNING} oder {@code PENDINGBOX_NOT_EMPTY}
     * @throws CloudUnavailableException wenn Cloud-API 1 nicht erreichbar war; es wird keine Sandbox angelegt
     */
    public TaskSnapshot start(String userId) {
        UserFolders folders = folderResolver.resolve(userId);
        UserSlot slot = claimUser(userId);
        boolean launched = false;
        try {
            if (hasPendingFiles(folders)) {
                throw new TaskRejectedException(RejectReason.PENDINGBOX_NOT_EMPTY, "In der pendingbox von Benutzer '"
                        + userId + "' liegen noch Dateien einer früheren Aufgabe; bitte zuerst bearbeiten");
            }
            String taskNumber = createBatchJob(userId);
            Sandbox sandbox = launch(folders, taskNumber, slot);
            launched = true;
            log.info("[{}] Aufgabe {} gestartet ({} Aufgaben in der TaskRegistry)", userId, taskNumber,
                    registry.size());
            return sandbox.snapshot();
        } finally {
            if (!launched) {
                users.remove(userId, slot);
            }
        }
    }

    /** Belegt die Benutzer:in; eine gerade endende Aufgabe derselben Benutzer:in darf noch auslaufen. */
    private UserSlot claimUser(String userId) {
        UserSlot slot = new UserSlot();
        UserSlot existing = users.putIfAbsent(userId, slot);
        if (existing == null) {
            return slot;
        }
        awaitDeregistered(existing);
        if (users.putIfAbsent(userId, slot) == null) {
            return slot;
        }
        throw new TaskRejectedException(RejectReason.USER_TASK_RUNNING,
                "Für Benutzer '" + userId + "' läuft bereits eine Aufgabe");
    }

    /** Wartet begrenzt auf die Abmeldung einer Aufgabe, die bereits einen Endzustand hat; eine laufende nicht. */
    private void awaitDeregistered(UserSlot slot) {
        Sandbox sandbox = slot.sandbox;
        Thread supervisor = slot.supervisor;
        if (sandbox == null || supervisor == null || sandbox.context().state() == TaskState.RUNNING) {
            return;
        }
        try {
            supervisor.join(STOP_WAIT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean hasPendingFiles(UserFolders folders) {
        try {
            return folders.hasPendingFiles();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String createBatchJob(String userId) {
        String operation = "Cloud-API 1 (BatchgenAuftrag anlegen)";
        Duration wait = properties.cloud().submitTimeout().plus(RESULT_GRACE);
        String taskNumber = await(cloud.createBatchJob(userId), wait, operation);
        if (!InvalidTaskNumberException.isValid(taskNumber)) {
            throw new CloudUnavailableException(operation,
                    new IllegalStateException("ungültige Nummer des BatchgenAuftrags: '" + taskNumber + "'"));
        }
        log.info("[{}] BatchgenAuftrag {} angelegt", userId, taskNumber);
        return taskNumber;
    }

    /** Baut die Sandbox auf, trägt sie in die TaskRegistry ein und startet ihre Threads. */
    private Sandbox launch(UserFolders folders, String taskNumber, UserSlot slot) {
        TaskContext context = new TaskContext(folders.userId(), taskNumber, properties.taskTimeout(),
                properties.errorThreshold(), clock);
        StatusPool pool = new InMemoryStatusPool();
        Thread producer = Thread.ofVirtual().name("producer-" + taskNumber)
                .unstarted(new InboxProducer(context, pool, folders, cloud, properties, clock));
        Thread consumer = Thread.ofVirtual().name("consumer-" + taskNumber)
                .unstarted(new StatusConsumer(context, pool, folders, cloud, properties, clock));
        Sandbox sandbox = new Sandbox(context, pool, producer, consumer);
        Thread supervisor = Thread.ofVirtual().name("sandbox-" + taskNumber).unstarted(() -> supervise(sandbox, slot));
        slot.sandbox = sandbox;
        slot.supervisor = supervisor;

        registry.register(sandbox);
        try {
            sandbox.start();
            supervisor.start();
        } catch (RuntimeException e) {
            context.finish(TaskState.ABORTED);
            registry.remove(sandbox);
            throw e;
        }
        return sandbox;
    }

    // --- 4. Statusabfrage ------------------------------------------------------------------------------------

    /**
     * Status aus der Cloud; solange die Aufgabe läuft, zusätzlich der lokale Stand aus der Sandbox.
     *
     * @throws TaskNotFoundException     wenn weder die Cloud noch die Sandbox die Aufgabe dieser Benutzer:in kennen
     * @throws CloudUnavailableException wenn die Cloud nicht erreichbar ist und die Aufgabe nicht mehr lokal läuft
     */
    public TaskStatus status(String userId, String taskNumber) {
        InvalidTaskNumberException.requireValid(taskNumber);
        Optional<TaskSnapshot> local = findLocal(userId, taskNumber).map(Sandbox::snapshot);
        Optional<TaskStatus.Cloud> fromCloud;
        try {
            fromCloud = await(cloud.batchJob(taskNumber), statusWait(), "Cloud-API 3 (BatchgenAuftrag lesen)")
                    .filter(job -> userId.equals(job.userId()))
                    .map(TaskStatus.Cloud::of);
        } catch (CloudUnavailableException e) {
            if (local.isEmpty()) {
                throw e;
            }
            log.warn("[{}] Status von Aufgabe {} nur lokal verfügbar: {}", userId, taskNumber, e.getMessage());
            fromCloud = Optional.empty();
        }
        if (fromCloud.isEmpty() && local.isEmpty()) {
            throw new TaskNotFoundException(userId, taskNumber);
        }
        return new TaskStatus(taskNumber, userId, fromCloud.orElse(null), local.orElse(null));
    }

    // --- 5. Abbruch von außen --------------------------------------------------------------------------------

    /**
     * Setzt das Abbruchsignal; Producer und Consumer beenden sich daraufhin. Den Endzustand {@code TERMINATED}
     * schreibt die Abmeldung, sobald beide Threads beendet sind.
     *
     * @return den lokalen Stand zum Zeitpunkt des Abbruchs
     * @throws TaskNotFoundException wenn die Aufgabe nicht (mehr) läuft oder einer anderen Benutzer:in gehört
     */
    public TaskSnapshot cancel(String userId, String taskNumber) {
        InvalidTaskNumberException.requireValid(taskNumber);
        Sandbox sandbox = findLocal(userId, taskNumber)
                .orElseThrow(() -> new TaskNotFoundException(userId, taskNumber));
        if (sandbox.context().finish(TaskState.TERMINATED)) {
            log.info("[{}] Aufgabe {} wird von außen abgebrochen", userId, taskNumber);
        } else {
            log.info("[{}] Aufgabe {} ist bereits beendet ({})", userId, taskNumber, sandbox.context().state());
        }
        return sandbox.snapshot();
    }

    private Optional<Sandbox> findLocal(String userId, String taskNumber) {
        return registry.find(taskNumber).filter(sandbox -> sandbox.context().userId().equals(userId));
    }

    // --- 6. Abschluss und Abmeldung --------------------------------------------------------------------------

    /** Läuft auf einem eigenen Virtual Thread je Sandbox: wartet auf beide Threads und meldet die Sandbox dann ab. */
    private void supervise(Sandbox sandbox, UserSlot slot) {
        joinQuietly(sandbox.producer());
        joinQuietly(sandbox.consumer());
        deregister(sandbox, slot);
    }

    /** Schreibt den Endzustand über Cloud-API 3 und entfernt die Sandbox aus der TaskRegistry. */
    private void deregister(Sandbox sandbox, UserSlot slot) {
        TaskContext context = sandbox.context();
        if (context.finish(TaskState.ABORTED)) {
            log.warn("[{}] Threads von Aufgabe {} endeten ohne Endzustand", context.userId(), context.taskNumber());
        }
        JobStatus endState = context.state().jobStatus();
        if (endState == null) {
            log.warn("[{}] Aufgabe {} ({}): kein Endzustand geschrieben, der BatchgenAuftrag bleibt RUNNING und muss "
                    + "manuell korrigiert werden", context.userId(), context.taskNumber(), context.state());
        } else {
            writeEndState(context, endState);
        }
        registry.remove(sandbox);
        users.remove(context.userId(), slot);
        log.info("[{}] Aufgabe {} abgemeldet: {} ({} Aufgaben in der TaskRegistry)", context.userId(),
                context.taskNumber(), sandbox.snapshot(), registry.size());
    }

    private void writeEndState(TaskContext context, JobStatus endState) {
        try {
            await(cloud.setJobStatus(context.taskNumber(), endState), statusWait(),
                    "Cloud-API 3 (Endzustand setzen)");
            log.info("[{}] BatchgenAuftrag {} auf {} gesetzt", context.userId(), context.taskNumber(), endState);
        } catch (CloudUnavailableException e) {
            log.error("[{}] Endzustand {} für BatchgenAuftrag {} konnte nicht geschrieben werden; er bleibt RUNNING und "
                    + "muss manuell korrigiert werden: {}", context.userId(), endState, context.taskNumber(),
                    e.getMessage());
        }
    }

    private static void joinQuietly(Thread thread) {
        boolean interrupted = false;
        while (true) {
            try {
                thread.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Beim Beenden der Anwendung laufende Sandboxen stoppen. Wie bei einem Absturz wird kein Endzustand geschrieben
     * (funktionsweise_sequenz.md, Abschnitt 7); die Benutzer:in bearbeitet die Restdateien vor dem nächsten Start.
     */
    @PreDestroy
    void shutdown() {
        registry.all().forEach(sandbox -> sandbox.context().finish(TaskState.ABORTED));
        users.values().forEach(slot -> {
            Thread supervisor = slot.supervisor;
            if (supervisor != null) {
                try {
                    supervisor.join(STOP_WAIT);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    // --- Hilfsmethoden ---------------------------------------------------------------------------------------

    private Duration statusWait() {
        PipelineProperties.Cloud config = properties.cloud();
        return config.statusTimeout().multipliedBy(config.retries() + 1L).plus(RESULT_GRACE);
    }

    private static <T> T await(CompletableFuture<T> call, Duration wait, String operation) {
        try {
            return call.get(wait.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CloudUnavailableException(operation, e);
        } catch (ExecutionException e) {
            throw new CloudUnavailableException(operation, e.getCause());
        } catch (TimeoutException e) {
            call.cancel(true);
            throw new CloudUnavailableException(operation, e);
        }
    }
}
