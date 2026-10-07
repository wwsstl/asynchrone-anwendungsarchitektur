package de.wwsstl.asynchrone.task;

import de.wwsstl.asynchrone.pool.StatusPool;

/**
 * Die isolierte Laufzeitumgebung einer Aufgabe: eigener {@link TaskContext}, eigener {@link StatusPool}, eigener
 * Producer- und Consumer-Thread (beide Virtual Threads). Nichts davon wird zwischen Aufgaben geteilt.
 */
public record Sandbox(TaskContext context, StatusPool pool, Thread producer, Thread consumer) {

    void start() {
        producer.start();
        consumer.start();
    }

    /** {@code true}, solange Producer- oder Consumer-Thread noch laufen. */
    public boolean isAlive() {
        return producer.isAlive() || consumer.isAlive();
    }

    public TaskSnapshot snapshot() {
        return context.snapshot(pool.size());
    }
}
