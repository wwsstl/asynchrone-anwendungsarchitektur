package de.wwsstl.asynchrone.taskmanager;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
import de.wwsstl.asynchrone.registry.TaskHistory;
import de.wwsstl.asynchrone.registry.TaskRegistry;
import jakarta.annotation.PreDestroy;

/**
 * Task-Scheduling- und Isolationszentrum (loesung_final.md 4.2).
 *
 * <p><b>Singleton über die gesamte Laufzeit:</b> Der Task Manager ist eine einzige Spring-Bean und hält keinen
 * eigenen Task-Bestand — jeder Start legt genau einen Task samt vollständiger Sandbox an und trägt ihn in das
 * (ebenfalls einzige) {@link TaskRegistry} ein. Bei {@code n} laufenden Tasks enthält das Register {@code n} Tasks.
 *
 * <p>Je Task baut er eine eigene Sandbox: {@link TaskContext}, exklusiver {@link StatusPool}, Producer-Thread
 * und Consumer-Thread (beide Virtual Threads). Nichts davon wird zwischen Tasks geteilt.
 *
 * <p><b>Lebensende eines Tasks:</b> Sobald beide Threads ausgelaufen sind, meldet der Task Manager den Task
 * automatisch ab. Zuerst legt er die letzte Momentaufnahme in der {@link TaskHistory} ab, dann entfernt er die
 * Sandbox aus dem Register; danach hält nichts mehr Threads, Status-Pool oder Kontext. Über {@link #status} bleibt
 * der Task mit seinem Endzustand abfragbar, bis die Aufbewahrungsfrist {@code pipeline.task-retention} abgelaufen
 * ist oder der Benutzer ihn per {@link #cancel} löscht. Abgelaufene Einträge werden ohne eigenen Scheduler
 * entfernt: einzeln beim Zugriff und gesammelt bei jedem {@link #start}.
 */
@Service
public class TaskManager {

    private static final Logger log = LoggerFactory.getLogger(TaskManager.class);

    /**
     * Wie lange ein Start auf das Auslaufen der Threads eines gerade abgebrochenen Tasks desselben Benutzers wartet,
     * bevor er mit 409 abgewiesen wird.
     */
    private static final Duration STOP_WAIT = Duration.ofSeconds(10);

    private final TaskRegistry registry;
    private final TaskHistory history;
    private final UserFolderResolver folderResolver;
    private final CloudClient cloud;
    private final PipelineProperties properties;
    private final Clock clock;

    /**
     * Neuester Task je Benutzer; dient ausschließlich dazu, das gleichzeitige Starten zweier Tasks desselben
     * Benutzers atomar abzuweisen (Schlüssel = Benutzer, also höchstens ein Eintrag je Benutzer). Die laufenden
     * Tasks stehen im {@link TaskRegistry}. Der Eintrag bleibt auch nach einem Abbruch stehen, bis die Threads des
     * Tasks ausgelaufen sind (Abmeldung) oder ein neuer Task ihn ersetzt: Solange etwa noch ein Aufruf von
     * Cloud-API 1 läuft, darf kein neuer Task die {@code pendingbox} aufnehmen.
     */
    private final ConcurrentHashMap<String, Sandbox> latestByUser = new ConcurrentHashMap<>();

    public TaskManager(TaskRegistry registry, TaskHistory history, UserFolderResolver folderResolver,
            CloudClient cloud, PipelineProperties properties, Clock clock) {
        this.registry = registry;
        this.history = history;
        this.folderResolver = folderResolver;
        this.cloud = cloud;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Startet einen neuen Task für den Benutzer.
     *
     * @throws TaskAlreadyRunningException wenn für den Benutzer noch ein Task läuft oder noch nicht ausgelaufen ist
     */
    public TaskSnapshot start(String userId) {
        UserFolders folders = folderResolver.resolve(userId);
        evictExpired();
        awaitStopped(latestByUser.get(userId));
        Sandbox sandbox = latestByUser.compute(userId, (user, latest) -> {
            if (latest != null && (latest.context().state() == TaskState.RUNNING || latest.isAlive())) {
                throw new TaskAlreadyRunningException(user, latest.context().taskId());
            }
            return launch(folders);
        });
        log.info("[{}] Task {} gestartet ({} Tasks im Register)", userId, sandbox.context().taskId(),
                registry.size());
        return sandbox.snapshot();
    }

    /**
     * Externer Abbruch: beendet einen laufenden Task (Abbruchsignal an Producer und Consumer) und <b>löscht ihn</b>
     * — er wird dann auch nicht in die Historie übernommen. Ist der Task bereits beendet, wird sein Endzustand aus
     * der Historie gelöscht. Danach ist die Task-ID in jedem Fall unbekannt.
     *
     * @return den Zustand des Tasks zum Zeitpunkt des Abbruchs
     * @throws TaskNotFoundException wenn es den Task nicht (mehr) gibt oder er einem anderen Benutzer gehört
     */
    public TaskSnapshot cancel(String userId, UUID taskId) {
        Optional<Sandbox> live = findLive(userId, taskId);
        if (live.isPresent()) {
            Sandbox sandbox = live.get();
            synchronized (sandbox) {
                boolean signalled = sandbox.context().cancel(CancelReason.USER_REQUEST);
                // Wer den Eintrag löscht, hat den Abbruch "gewonnen": bei zwei gleichzeitigen Abbrüchen erhält der
                // zweite 404. Die spätere Abmeldung findet den Task dann nicht mehr und archiviert ihn nicht.
                if (registry.remove(taskId).isPresent()) {
                    log.info("[{}] Task {} {} und gelöscht ({} Tasks im Register)", userId, taskId,
                            signalled ? "abgebrochen" : "war bereits beendet", registry.size());
                    return sandbox.snapshot();
                }
            }
            // Inzwischen ausgelaufen und abgemeldet (oder parallel gelöscht): weiter mit der Historie.
        }
        TaskSnapshot finished = findFinished(userId, taskId);
        if (history.remove(taskId).isEmpty()) {
            throw new TaskNotFoundException(userId, taskId);
        }
        log.info("[{}] Beendeter Task {} aus der Historie gelöscht", userId, taskId);
        return finished;
    }

    /** Laufender Task: aktueller Zustand aus der Sandbox; beendeter Task: Endzustand aus der Historie. */
    public TaskSnapshot status(String userId, UUID taskId) {
        Optional<Sandbox> live = findLive(userId, taskId);
        return live.isPresent() ? live.get().snapshot() : findFinished(userId, taskId);
    }

    /** Baut die Sandbox auf, trägt den Task ins Register ein und startet dann erst seine Threads. */
    private Sandbox launch(UserFolders folders) {
        TaskContext context = new TaskContext(folders.userId(), properties.taskTimeout(),
                properties.errorThreshold(), clock);
        StatusPool pool = new InMemoryStatusPool();

        // Der zuletzt auslaufende der beiden Threads meldet den Task ab.
        AtomicReference<Sandbox> self = new AtomicReference<>();
        AtomicInteger runningThreads = new AtomicInteger(2);
        Runnable onThreadExit = () -> {
            if (runningThreads.decrementAndGet() == 0) {
                deregister(self.get());
            }
        };
        Thread producer = Thread.ofVirtual().name("producer-" + context.taskId()).unstarted(
                andThen(new InboxProducer(context, pool, folders, cloud, properties, clock), onThreadExit));
        Thread consumer = Thread.ofVirtual().name("consumer-" + context.taskId()).unstarted(
                andThen(new StatusConsumer(context, pool, folders, cloud, properties, clock), onThreadExit));
        Sandbox sandbox = new Sandbox(context, pool, producer, consumer);
        self.set(sandbox);

        // Erst registrieren: Der Task ist damit ab dem ersten Moment abfragbar.
        registry.register(sandbox);
        try {
            sandbox.start();
        } catch (RuntimeException e) {
            context.cancel(CancelReason.INTERNAL_ERROR);
            // Ein nie gestarteter Thread läuft auch nie aus; für ihn wird hier abgezählt. latestByUser ist wegen der
            // Ausnahme nie gesetzt worden, daher genügt das Archivieren.
            for (Thread thread : new Thread[] {producer, consumer}) {
                if (thread.getState() == Thread.State.NEW && runningThreads.decrementAndGet() == 0) {
                    archive(sandbox);
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
     * Meldet einen Task ab, dessen Threads beide ausgelaufen sind: Endzustand in die Historie, Sandbox aus dem
     * Register und aus der Startsperre. Danach referenziert nichts mehr Threads, Status-Pool oder Kontext.
     */
    private void deregister(Sandbox sandbox) {
        archive(sandbox);
        latestByUser.remove(sandbox.context().userId(), sandbox);
    }

    /** Übernimmt den Endzustand in die Historie und entfernt die Sandbox — es sei denn, der Task wurde gelöscht. */
    private void archive(Sandbox sandbox) {
        TaskContext context = sandbox.context();
        // Beide Threads sind beendet, also muss es auch der Task sein; sichert den Endzeitpunkt für die Frist.
        if (context.cancel(CancelReason.INTERNAL_ERROR)) {
            log.warn("[{}] Task {} lief nach Ende seiner Threads noch", context.userId(), context.taskId());
        }
        synchronized (sandbox) {
            if (registry.find(context.taskId()).filter(registered -> registered == sandbox).isEmpty()) {
                return; // durch den Abbruch bereits gelöscht
            }
            // Erst ablegen, dann entfernen: Die Status-Abfrage findet den Task so zu jedem Zeitpunkt.
            history.record(sandbox.snapshot());
            registry.remove(context.taskId());
        }
        log.info("[{}] Task {} abgemeldet; Endzustand {} bleibt bis {} abfragbar ({} Tasks im Register)",
                context.userId(), context.taskId(), context.state(),
                context.finishedAt().plus(properties.taskRetention()), registry.size());
    }

    /** Wartet begrenzt, bis die Threads eines bereits beendeten Tasks ausgelaufen sind; einen laufenden nicht. */
    private void awaitStopped(Sandbox previous) {
        if (previous == null || previous.context().state() == TaskState.RUNNING) {
            return;
        }
        Instant deadline = clock.instant().plus(STOP_WAIT);
        try {
            for (Thread thread : new Thread[] {previous.producerThread(), previous.consumerThread()}) {
                Duration left = Duration.between(clock.instant(), deadline);
                if (left.isNegative() || !thread.join(left)) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Optional<Sandbox> findLive(String userId, UUID taskId) {
        return registry.find(taskId).filter(sandbox -> sandbox.context().userId().equals(userId));
    }

    /** Ein abgelaufener Eintrag gilt als gelöscht, auch wenn der gesammelte Durchlauf ihn noch nicht erfasst hat. */
    private TaskSnapshot findFinished(String userId, UUID taskId) {
        TaskSnapshot snapshot = history.find(taskId)
                .filter(finished -> finished.userId().equals(userId))
                .orElseThrow(() -> new TaskNotFoundException(userId, taskId));
        if (!snapshot.finishedAt().isAfter(retentionCutoff())) {
            history.remove(taskId);
            throw new TaskNotFoundException(userId, taskId);
        }
        return snapshot;
    }

    /** Entfernt alle Einträge der Historie, deren Aufbewahrungsfrist abgelaufen ist. */
    private void evictExpired() {
        int evicted = history.removeFinishedUntil(retentionCutoff());
        if (evicted > 0) {
            log.info("{} beendete Tasks nach Ablauf der Aufbewahrungsfrist ({}) aus der Historie entfernt", evicted,
                    properties.taskRetention());
        }
    }

    /** Wer spätestens zu diesem Zeitpunkt beendet wurde, hat die Aufbewahrungsfrist überschritten. */
    private Instant retentionCutoff() {
        return clock.instant().minus(properties.taskRetention());
    }

    /** Beim Herunterfahren laufende Tasks abbrechen; das Register selbst wird dabei nicht angetastet. */
    @PreDestroy
    void shutdown() {
        registry.all().forEach(sandbox -> sandbox.context().cancel(CancelReason.SHUTDOWN));
    }
}
