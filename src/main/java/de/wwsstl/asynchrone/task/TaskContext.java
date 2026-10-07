package de.wwsstl.asynchrone.task;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Zustand genau einer Sandbox. Producer, Consumer und REST-API greifen nebenläufig darauf zu; alle Felder sind daher
 * atomar. Der Wechsel aus {@link TaskState#RUNNING} in einen Endzustand geschieht genau einmal: Wer zuerst kommt,
 * gewinnt. Mit dem Endzustand wird zugleich das Abbruchsignal gesetzt, auf das Producer und Consumer achten.
 */
public final class TaskContext {

    private final String userId;
    private final String taskNumber;
    private final Instant startedAt;
    private final Duration timeout;
    private final int errorThreshold;
    private final Clock clock;

    private final Object transitionLock = new Object();
    private final AtomicReference<TaskState> state = new AtomicReference<>(TaskState.RUNNING);
    private final AtomicReference<Instant> finishedAt = new AtomicReference<>();
    /** Abbruchsignal für Producer und Consumer. */
    private final AtomicBoolean stopRequested = new AtomicBoolean();
    /** Weckt wartende Threads, sobald das Abbruchsignal gesetzt ist. */
    private final CountDownLatch stopSignal = new CountDownLatch(1);

    private final AtomicInteger errorCount = new AtomicInteger();
    private final AtomicInteger submittedCount = new AtomicInteger();
    private final AtomicInteger succeededCount = new AtomicInteger();
    private final AtomicInteger unclearCount = new AtomicInteger();
    private final AtomicBoolean producerFinished = new AtomicBoolean();

    public TaskContext(String userId, String taskNumber, Duration timeout, int errorThreshold, Clock clock) {
        this.userId = userId;
        this.taskNumber = taskNumber;
        this.timeout = timeout;
        this.errorThreshold = errorThreshold;
        this.clock = clock;
        this.startedAt = clock.instant();
    }

    public String userId() {
        return userId;
    }

    public String taskNumber() {
        return taskNumber;
    }

    // --- Endzustand und Abbruchsignal ------------------------------------------------------------------------

    /**
     * Setzt den Endzustand und das Abbruchsignal, sofern die Aufgabe noch läuft.
     *
     * @return {@code true}, wenn dieser Aufruf die Aufgabe beendet hat
     */
    public boolean finish(TaskState endState) {
        if (endState == TaskState.RUNNING) {
            throw new IllegalArgumentException("RUNNING ist kein Endzustand");
        }
        synchronized (transitionLock) {
            if (state.get() != TaskState.RUNNING) {
                return false;
            }
            finishedAt.set(clock.instant());
            state.set(endState);
            stopRequested.set(true);
        }
        stopSignal.countDown();
        return true;
    }

    public TaskState state() {
        return state.get();
    }

    public Instant finishedAt() {
        return finishedAt.get();
    }

    /** {@code true}, sobald das Abbruchsignal gesetzt ist. */
    public boolean isStopping() {
        return stopRequested.get();
    }

    /**
     * Wartet bis zu {@code maxWait} oder bis das Abbruchsignal gesetzt wird, was zuerst eintritt.
     *
     * @return {@code true}, wenn das Abbruchsignal gesetzt ist
     */
    public boolean awaitStop(Duration maxWait) {
        try {
            stopSignal.await(maxWait.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finish(TaskState.ABORTED);
        }
        return isStopping();
    }

    /** {@code true}, sobald die maximale Laufzeit erreicht ist. */
    public boolean isTimedOut() {
        return !clock.instant().isBefore(startedAt.plus(timeout));
    }

    // --- Zähler ----------------------------------------------------------------------------------------------

    /** Zählt eine fehlerhafte Datei (nicht umgewandelt, Status {@code ERROR} oder unlesbar). */
    public int recordError() {
        return errorCount.incrementAndGet();
    }

    public boolean errorThresholdReached() {
        return errorCount.get() >= errorThreshold;
    }

    public void recordSubmitted() {
        submittedCount.incrementAndGet();
    }

    public void recordSucceeded() {
        succeededCount.incrementAndGet();
    }

    public void recordUnclear(int files) {
        unclearCount.addAndGet(files);
    }

    public void producerFinished() {
        producerFinished.set(true);
    }

    public boolean isProducerFinished() {
        return producerFinished.get();
    }

    public TaskSnapshot snapshot(int pending) {
        return new TaskSnapshot(taskNumber, userId, state.get(), startedAt, finishedAt.get(), submittedCount.get(),
                succeededCount.get(), errorCount.get(), pending, unclearCount.get());
    }
}
