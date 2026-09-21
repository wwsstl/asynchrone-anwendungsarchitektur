package de.wwsstl.asynchrone.context;

import de.wwsstl.asynchrone.pool.StatusPool;

/**
 * Die vollständig isolierte Laufzeitumgebung eines Tasks: eigener {@link TaskContext}, eigener
 * {@link StatusPool}, eigener Producer- und eigener Consumer-Thread (loesung_final.md 4.2).
 *
 * <p>Mit {@link #snapshot()} bleibt der Zustand auch nach Ende der Threads abfragbar; die Sandbox selbst
 * verbleibt als Eintrag des Tasks in der {@code TaskRegistry}.
 */
public record Sandbox(TaskContext context, StatusPool pool, Thread producerThread, Thread consumerThread) {

    public void start() {
        producerThread.start();
        consumerThread.start();
    }

    public TaskSnapshot snapshot() {
        return context.snapshot(pool.size());
    }
}
