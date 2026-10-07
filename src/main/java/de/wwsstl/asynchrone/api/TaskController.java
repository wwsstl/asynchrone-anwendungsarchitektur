package de.wwsstl.asynchrone.api;

import java.net.URI;
import java.util.concurrent.Callable;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import de.wwsstl.asynchrone.task.TaskManager;
import de.wwsstl.asynchrone.task.TaskSnapshot;
import de.wwsstl.asynchrone.task.TaskStatus;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * REST-API (Spring WebFlux) mit drei Operationen: Start, Status, Abbruch. Sie enthält keine Geschäftslogik, sondern
 * ruft nur den {@link TaskManager} auf. Weil dieser blockierend arbeitet, läuft jeder Aufruf auf einem eigenen Virtual
 * Thread ({@code taskManagerScheduler}) und nie auf dem Event-Loop.
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

    /** Startet eine Aufgabe: {@code 202 Accepted} mit dem lokalen Stand und der Adresse für die Statusabfrage. */
    @PostMapping
    public Mono<ResponseEntity<TaskSnapshot>> start(@PathVariable String userId) {
        return offload(() -> taskManager.start(userId)).map(snapshot -> ResponseEntity
                .accepted()
                .location(URI.create("/api/users/" + userId + "/tasks/" + snapshot.taskNumber()))
                .body(snapshot));
    }

    /** Status aus der Cloud, während der Laufzeit zusätzlich der lokale Stand. */
    @GetMapping("/{taskNumber}")
    public Mono<TaskStatus> status(@PathVariable String userId, @PathVariable String taskNumber) {
        return offload(() -> taskManager.status(userId, taskNumber));
    }

    /** Abbruch von außen: {@code 202 Accepted}; die Aufgabe endet, sobald Producer und Consumer beendet sind. */
    @PostMapping("/{taskNumber}/cancel")
    public Mono<ResponseEntity<TaskSnapshot>> cancel(@PathVariable String userId, @PathVariable String taskNumber) {
        return offload(() -> taskManager.cancel(userId, taskNumber))
                .map(snapshot -> ResponseEntity.accepted().body(snapshot));
    }

    private <T> Mono<T> offload(Callable<T> call) {
        return Mono.fromCallable(call).subscribeOn(taskManagerScheduler);
    }
}
