package de.wwsstl.asynchrone.context;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Zustand genau einer Sandbox (kein Spring-Singleton, loesung_final.md 4.4).
 *
 * <p>Producer, Consumer und REST-API greifen nebenläufig darauf zu; alle Felder sind daher atomar. Der Wechsel
 * aus {@link TaskState#RUNNING} in einen Endzustand erfolgt genau einmal — wer zuerst kommt (Abschluss oder
 * Abbruch), gewinnt.
 */
public final class TaskContext {

    private final String userId;
    private final UUID taskId;
    private final Instant startedAt;
    private final Duration timeout;
    private final int errorThreshold;
    private final Clock clock;

    private final Object transitionLock = new Object();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicReference<CancelReason> cancelReason = new AtomicReference<>();
    private final AtomicReference<TaskState> state = new AtomicReference<>(TaskState.RUNNING);
    private final CountDownLatch cancelSignal = new CountDownLatch(1);

    private final AtomicInteger errorCount = new AtomicInteger();
    private final AtomicInteger succeededCount = new AtomicInteger();
    private final AtomicInteger submittedCount = new AtomicInteger();
    private final AtomicInteger abandonedCount = new AtomicInteger();
    private final AtomicBoolean producerFinished = new AtomicBoolean();

    public TaskContext(String userId, Duration timeout, int errorThreshold, Clock clock) {
        this.userId = userId;
        this.taskId = UUID.randomUUID();
        this.timeout = timeout;
        this.errorThreshold = errorThreshold;
        this.clock = clock;
        this.startedAt = clock.instant();
    }

    public String userId() {
        return userId;
    }

    public UUID taskId() {
        return taskId;
    }

    // --- Abbruch / Endzustände -------------------------------------------------------------------------------

    /**
     * Setzt das Abbruchsignal. Der erste Grund gewinnt; ein bereits beendeter Task bleibt unverändert.
     *
     * @return {@code true}, wenn dieser Aufruf den Task abgebrochen hat
     */
    public boolean cancel(CancelReason reason) {
        synchronized (transitionLock) {
            if (state.get() != TaskState.RUNNING) {
                return false;
            }
            cancelReason.set(reason);
            state.set(TaskState.CANCELLED);
            cancelled.set(true);
        }
        cancelSignal.countDown();
        return true;
    }

    /** Markiert den Task als vollständig abgearbeitet, sofern er nicht schon abgebrochen wurde. */
    public boolean complete() {
        synchronized (transitionLock) {
            return state.compareAndSet(TaskState.RUNNING, TaskState.COMPLETED);
        }
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    public CancelReason cancelReason() {
        return cancelReason.get();
    }

    public TaskState state() {
        return state.get();
    }

    /**
     * Wartet bis zu {@code maxWait} oder bis der Task abgebrochen wird — was zuerst eintritt. So reagieren
     * schlafende Threads sofort auf ein Abbruchsignal, ohne dass Interrupts nötig sind.
     *
     * @return {@code true}, wenn der Task abgebrochen wurde
     */
    public boolean awaitCancel(Duration maxWait) {
        try {
            return cancelSignal.await(maxWait.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return cancelled.get();
        }
    }

    /**
     * Prüft die maximale Laufzeit (wird im Zyklus des Consumers aufgerufen, loesung_final.md 4.4).
     *
     * @return {@code true}, wenn dieser Aufruf den Task wegen Zeitüberschreitung abgebrochen hat
     */
    public boolean checkTimeout() {
        if (state.get() != TaskState.RUNNING) {
            return false;
        }
        if (clock.instant().isBefore(startedAt.plus(timeout))) {
            return false;
        }
        return cancel(CancelReason.TIMEOUT);
    }

    // --- Zähler ----------------------------------------------------------------------------------------------

    /**
     * Zählt eine fehlerhafte Datei und bricht den Task ab, sobald der Schwellenwert erreicht ist.
     *
     * @return der neue Stand des Fehlerzählers
     */
    public int recordError() {
        int errors = errorCount.incrementAndGet();
        if (errors >= errorThreshold) {
            cancel(CancelReason.ERROR_THRESHOLD);
        }
        return errors;
    }

    public int errorCount() {
        return errorCount.get();
    }

    public void recordSucceeded() {
        succeededCount.incrementAndGet();
    }

    public void recordSubmitted() {
        submittedCount.incrementAndGet();
    }

    public void recordAbandoned(int files) {
        abandonedCount.addAndGet(files);
    }

    public void producerFinished() {
        producerFinished.set(true);
    }

    public boolean isProducerFinished() {
        return producerFinished.get();
    }

    public TaskSnapshot snapshot(int pending) {
        return new TaskSnapshot(userId, taskId, state.get(), cancelReason.get(), startedAt,
                producerFinished.get(), submittedCount.get(), succeededCount.get(), errorCount.get(), pending,
                abandonedCount.get());
    }
}
