package de.wwsstl.asynchrone.context;

import de.wwsstl.asynchrone.pool.StatusPool;

/**
 * Die vollständig isolierte Laufzeitumgebung eines Tasks: eigener {@link TaskContext}, eigener
 * {@link StatusPool}, eigener Producer- und eigener Consumer-Thread (loesung_final.md 4.2).
 *
 * <p>Sobald beide Threads ausgelaufen sind, meldet der {@code TaskManager} die Sandbox ab: Ihre letzte
 * {@link #snapshot()} wandert in die {@code TaskHistory}, die Sandbox selbst verlässt die {@code TaskRegistry}.
 */
public record Sandbox(TaskContext context, StatusPool pool, Thread producerThread, Thread consumerThread) {

    public void start() {
        producerThread.start();
        consumerThread.start();
    }

    /** {@code true}, solange Producer- oder Consumer-Thread noch laufen (auch nach einem Abbruchsignal). */
    public boolean isAlive() {
        return producerThread.isAlive() || consumerThread.isAlive();
    }

    public TaskSnapshot snapshot() {
        return context.snapshot(pool.size());
    }
}
