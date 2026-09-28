package de.wwsstl.asynchrone.api;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.taskmanager.TaskManager;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * Reaktive REST-Steuerungsebene (Spring WebFlux) mit genau drei Operationen: Start, Abbruch, Status
 * (loesung_final.md 4.1). Der Controller enthält keine Geschäftslogik, sondern ruft ausschließlich den
 * {@link TaskManager} auf.
 *
 * <p>Der {@link TaskManager} arbeitet blockierend (Dateisystem, Sperren, Warten auf auslaufende Threads). Jeder
 * Aufruf wird daher auf den {@code taskManagerScheduler} (Virtual Threads) verlagert und nie auf dem
 * Netty-Event-Loop ausgeführt. Fehler erreichen als {@code Mono.error} den {@link ApiExceptionHandler}.
 */
@RestController
@RequestMapping("/api/users/{userId}/tasks")
public class TaskController {

    private final TaskManager taskManager;
    private final Scheduler taskManagerScheduler;

    public TaskController(TaskManager taskManager, Scheduler taskManagerScheduler) {
        this.taskManager = taskManager;
        this.taskManagerScheduler = taskManagerScheduler;
    }

    @PostMapping
    public Mono<ResponseEntity<TaskSnapshot>> start(@PathVariable String userId) {
        return offload(() -> taskManager.start(userId)).map(snapshot -> {
            URI location = URI.create("/api/users/" + userId + "/tasks/" + snapshot.taskId());
            return ResponseEntity.accepted().location(location).body(snapshot);
        });
    }

    /**
     * Externer Abbruch: beendet einen laufenden Task bzw. löscht einen beendeten aus der Historie; danach liefert
     * die taskId 404.
     */
    @PostMapping("/{taskId}/cancel")
    public Mono<TaskSnapshot> cancel(@PathVariable String userId, @PathVariable UUID taskId) {
        return offload(() -> taskManager.cancel(userId, taskId));
    }

    /** Aktueller Zustand eines laufenden bzw. Endzustand eines beendeten Tasks (bis {@code task-retention}). */
    @GetMapping("/{taskId}")
    public Mono<TaskSnapshot> status(@PathVariable String userId, @PathVariable UUID taskId) {
        return offload(() -> taskManager.status(userId, taskId));
    }

    private <T> Mono<T> offload(Callable<T> call) {
        return Mono.fromCallable(call).subscribeOn(taskManagerScheduler);
    }
}
