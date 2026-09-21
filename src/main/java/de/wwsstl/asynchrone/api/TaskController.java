package de.wwsstl.asynchrone.api;

import java.net.URI;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import de.wwsstl.asynchrone.context.TaskSnapshot;
import de.wwsstl.asynchrone.taskmanager.TaskManager;

/**
 * REST-Steuerungsebene mit genau drei Operationen: Start, Abbruch, Status (loesung_final.md 4.1). Der Controller
 * enthält keine Geschäftslogik, sondern ruft ausschließlich den {@link TaskManager} auf.
 */
@RestController
@RequestMapping("/api/users/{userId}/tasks")
public class TaskController {

    private final TaskManager taskManager;

    public TaskController(TaskManager taskManager) {
        this.taskManager = taskManager;
    }

    @PostMapping
    public ResponseEntity<TaskSnapshot> start(@PathVariable String userId) {
        TaskSnapshot snapshot = taskManager.start(userId);
        URI location = URI.create("/api/users/" + userId + "/tasks/" + snapshot.taskId());
        return ResponseEntity.accepted().location(location).body(snapshot);
    }

    /** Externer Abbruch: beendet den Task und löscht ihn aus der TaskRegistry; danach liefert die taskId 404. */
    @PostMapping("/{taskId}/cancel")
    public ResponseEntity<TaskSnapshot> cancel(@PathVariable String userId, @PathVariable UUID taskId) {
        return ResponseEntity.ok(taskManager.cancel(userId, taskId));
    }

    @GetMapping("/{taskId}")
    public TaskSnapshot status(@PathVariable String userId, @PathVariable UUID taskId) {
        return taskManager.status(userId, taskId);
    }
}
