package de.wwsstl.asynchrone.taskmanager;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
import de.wwsstl.asynchrone.registry.TaskRegistry;
import jakarta.annotation.PreDestroy;

/**
 * Task-Scheduling- und Isolationszentrum (loesung_final.md 4.2).
 *
 * <p><b>Singleton über die gesamte Laufzeit:</b> Der Task Manager ist eine einzige Spring-Bean und hält keinen
 * eigenen Task-Bestand — jeder Start legt genau einen Task samt vollständiger Sandbox an und trägt ihn in das
 * (ebenfalls einzige) {@link TaskRegistry} ein. Nach {@code n} Starts enthält das Register {@code n} Tasks; ein
 * Task verlässt es nur durch den externen Abbruch ({@link #cancel}).
 *
 * <p>Je Task baut er eine eigene Sandbox: {@link TaskContext}, exklusiver {@link StatusPool}, Producer-Thread
 * und Consumer-Thread (beide Virtual Threads). Nichts davon wird zwischen Tasks geteilt.
 */
@Service
public class TaskManager {

    private static final Logger log = LoggerFactory.getLogger(TaskManager.class);

    private final TaskRegistry registry;
    private final UserFolderResolver folderResolver;
    private final CloudClient cloud;
    private final PipelineProperties properties;
    private final Clock clock;

    /**
     * Wie lange ein Start auf das Auslaufen der Threads eines gerade abgebrochenen Tasks desselben Benutzers wartet,
     * bevor er mit 409 abgewiesen wird.
     */
    private static final Duration STOP_WAIT = Duration.ofSeconds(10);

    /**
     * Neuester Task je Benutzer; dient ausschließlich dazu, das gleichzeitige Starten zweier Tasks desselben
     * Benutzers atomar abzuweisen (Schlüssel = Benutzer, also höchstens ein Eintrag je Benutzer). Die Menge aller
     * Tasks steht im {@link TaskRegistry}. Der Eintrag bleibt auch nach einem Abbruch stehen, bis ein neuer Task
     * ihn ersetzt: Solange die Threads des alten Tasks noch laufen (etwa ein laufender Aufruf von Cloud-API 1),
     * darf kein neuer Task die {@code pendingbox} aufnehmen.
     */
    private final ConcurrentHashMap<String, Sandbox> latestByUser = new ConcurrentHashMap<>();

    public TaskManager(TaskRegistry registry, UserFolderResolver folderResolver, CloudClient cloud,
            PipelineProperties properties, Clock clock) {
        this.registry = registry;
        this.folderResolver = folderResolver;
        this.cloud = cloud;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Startet einen neuen Task für den Benutzer.
     *
     * @throws TaskAlreadyRunningException wenn für diesen Benutzer noch ein Task läuft oder noch nicht ausgelaufen ist
     */
    public TaskSnapshot start(String userId) {
        UserFolders folders = folderResolver.resolve(userId);
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
     * Externer Abbruch: beendet den Task (Abbruchsignal an Producer und Consumer) und <b>löscht ihn aus dem
     * Register</b>; danach ist die Task-ID unbekannt. Ist der Task bereits beendet, wird er nur noch gelöscht.
     *
     * @return den Zustand des Tasks zum Zeitpunkt des Abbruchs
     * @throws TaskNotFoundException wenn es den Task nicht (mehr) gibt oder er einem anderen Benutzer gehört
     */
    public TaskSnapshot cancel(String userId, UUID taskId) {
        Sandbox sandbox = find(userId, taskId);
        boolean signalled = sandbox.context().cancel(CancelReason.USER_REQUEST);
        // Wer den Eintrag löscht, hat den Abbruch "gewonnen": bei zwei gleichzeitigen Abbrüchen erhält der zweite 404.
        if (registry.remove(taskId).isEmpty()) {
            throw new TaskNotFoundException(userId, taskId);
        }
        log.info("[{}] Task {} {} und aus dem Register gelöscht ({} Tasks im Register)", userId, taskId,
                signalled ? "abgebrochen" : "war bereits beendet", registry.size());
        return sandbox.snapshot();
    }

    public TaskSnapshot status(String userId, UUID taskId) {
        return find(userId, taskId).snapshot();
    }

    /** Baut die Sandbox auf, trägt den Task ins Register ein und startet dann erst seine Threads. */
    private Sandbox launch(UserFolders folders) {
        TaskContext context = new TaskContext(folders.userId(), properties.taskTimeout(),
                properties.errorThreshold(), clock);
        StatusPool pool = new InMemoryStatusPool();

        Thread producer = Thread.ofVirtual().name("producer-" + context.taskId()).unstarted(
                new InboxProducer(context, pool, folders, cloud, properties, clock));
        Thread consumer = Thread.ofVirtual().name("consumer-" + context.taskId()).unstarted(
                new StatusConsumer(context, pool, folders, cloud, properties, clock));
        Sandbox sandbox = new Sandbox(context, pool, producer, consumer);

        // Erst registrieren: Der Task ist damit ab dem ersten Moment abfragbar und bleibt auch dann im Register,
        // wenn das Starten der Threads scheitert.
        registry.register(sandbox);
        try {
            sandbox.start();
        } catch (RuntimeException e) {
            context.cancel(CancelReason.INTERNAL_ERROR);
            throw e;
        }
        return sandbox;
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

    private Sandbox find(String userId, UUID taskId) {
        return registry.find(taskId)
                .filter(sandbox -> sandbox.context().userId().equals(userId))
                .orElseThrow(() -> new TaskNotFoundException(userId, taskId));
    }

    /** Beim Herunterfahren laufende Tasks abbrechen; das Register selbst wird dabei nicht angetastet. */
    @PreDestroy
    void shutdown() {
        registry.all().forEach(sandbox -> sandbox.context().cancel(CancelReason.SHUTDOWN));
    }
}
