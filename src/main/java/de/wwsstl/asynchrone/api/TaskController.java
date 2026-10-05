package de.wwsstl.asynchrone.api;

import java.net.URI;
import java.util.concurrent.Callable;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.taskmanager.CancelResult;
import de.wwsstl.asynchrone.taskmanager.TaskManager;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * Reaktive REST-Steuerungsebene (Spring WebFlux) mit genau drei Operationen: Start, Abbruch, Status
 * (loesung_final.md 4.1). Der Controller enthält keine Geschäftslogik, sondern ruft ausschließlich den
 * {@link TaskManager} auf. Die {@code taskId} ist die Aufgabennummer, also die Nummer des BatchgenAuftrags.
 *
 * <p>Der {@link TaskManager} arbeitet blockierend (Dateisystem, Aufrufe der Cloud-Dienste). Jeder Aufruf wird
 * daher auf den {@code taskManagerScheduler} (Virtual Threads) verlagert und nie auf dem Netty-Event-Loop
 * ausgeführt. Fehler erreichen als {@code Mono.error} den {@link ApiExceptionHandler}.
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
     * Abbruch von außen: markiert den BatchgenAuftrag über Cloud-API 3 als {@code CANCELLED} und antwortet mit
     * {@code 202} und {@code CANCELLING}. Der Task endet spätestens nach einem {@code sweep-interval}.
     */
    @PostMapping("/{taskId}/cancel")
    public Mono<ResponseEntity<CancelResult>> cancel(@PathVariable String userId, @PathVariable String taskId) {
        return offload(() -> taskManager.cancel(userId, taskId)).map(result -> ResponseEntity.accepted().body(result));
    }

    /** Aktueller Zustand des laufenden bzw. Endzustand des letzten Laufs (bis {@code task-retention}). */
    @GetMapping("/{taskId}")
    public Mono<TaskSnapshot> status(@PathVariable String userId, @PathVariable String taskId) {
        return offload(() -> taskManager.status(userId, taskId));
    }

    private <T> Mono<T> offload(Callable<T> call) {
        return Mono.fromCallable(call).subscribeOn(taskManagerScheduler);
    }
}
