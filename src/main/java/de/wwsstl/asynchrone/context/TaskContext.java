package de.wwsstl.asynchrone.context;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import de.wwsstl.asynchrone.cloud.BatchJobId;

/**
 * Zustand genau einer Sandbox, also eines Laufs (kein Spring-Singleton, loesung_final.md 4.4).
 *
 * <p>Producer, Consumer und REST-API greifen nebenläufig darauf zu; alle Felder sind daher atomar. Der Wechsel
 * aus {@link TaskState#RUNNING} in einen Endzustand erfolgt genau einmal — wer zuerst kommt (Abschluss oder
 * Abbruch), gewinnt.
 *
 * <p>Zeitüberschreitung und Fehlerschwelle brechen den Lauf nicht selbst ab. Der Consumer stellt sie fest und
 * meldet sie über {@link #requestAbort}; ab dann {@link #isStopping() hält die Sandbox an}, und der eigentliche
 * Abbruch folgt über Cloud-API 3 (funktionsweise_sequenz.md, Abschnitt 6).
 */
public final class TaskContext {

    private final String userId;
    private final BatchJobId jobId;
    private final int run;
    private final Instant startedAt;
    private final Duration timeout;
    private final int errorThreshold;
    private final Clock clock;

    private final Object transitionLock = new Object();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicReference<CancelReason> cancelReason = new AtomicReference<>();
    private final AtomicReference<CancelReason> abortRequest = new AtomicReference<>();
    private final AtomicReference<TaskState> state = new AtomicReference<>(TaskState.RUNNING);
    /** Zeitpunkt des Wechsels in einen Endzustand; wird vor {@link #state} gesetzt, also nie später sichtbar. */
    private final AtomicReference<Instant> finishedAt = new AtomicReference<>();
    /** Weckt wartende Threads bei einer Abbruchmeldung oder einem Abbruch. */
    private final CountDownLatch stopSignal = new CountDownLatch(1);

    private final AtomicInteger errorCount = new AtomicInteger();
    private final AtomicInteger succeededCount = new AtomicInteger();
    private final AtomicInteger submittedCount = new AtomicInteger();
    private final AtomicInteger resumedCount = new AtomicInteger();
    private final AtomicInteger abandonedCount = new AtomicInteger();
    private final AtomicBoolean producerFinished = new AtomicBoolean();

    /**
     * @param jobId Aufgabennummer, zugleich die Nummer des BatchgenAuftrags
     * @param run   Laufnummer unter dieser Aufgabennummer
     */
    public TaskContext(String userId, BatchJobId jobId, int run, Duration timeout, int errorThreshold, Clock clock) {
        this.userId = userId;
        this.jobId = jobId;
        this.run = run;
        this.timeout = timeout;
        this.errorThreshold = errorThreshold;
        this.clock = clock;
        this.startedAt = clock.instant();
    }

    public String userId() {
        return userId;
    }

    public BatchJobId jobId() {
        return jobId;
    }

    public int run() {
        return run;
    }

    // --- Abbruch / Endzustände -------------------------------------------------------------------------------

    /**
     * Hält fest, dass der Lauf wegen {@code reason} abgebrochen werden soll, und weckt wartende Threads. Producer
     * und Consumer nehmen ab dann keine neue Arbeit mehr an; der Endzustand folgt erst mit {@link #cancel}.
     *
     * @return {@code true}, wenn dies die erste Abbruchmeldung eines laufenden Tasks war
     */
    public boolean requestAbort(CancelReason reason) {
        if (state.get() != TaskState.RUNNING || !abortRequest.compareAndSet(null, reason)) {
            return false;
        }
        stopSignal.countDown();
        return true;
    }

    /** Der Grund der ersten Abbruchmeldung; leer, wenn es keine gab. */
    public Optional<CancelReason> abortRequest() {
        return Optional.ofNullable(abortRequest.get());
    }

    /**
     * Setzt den Endzustand {@code CANCELLED}. Der erste Grund gewinnt; ein bereits beendeter Task bleibt unverändert.
     *
     * @return {@code true}, wenn dieser Aufruf den Task abgebrochen hat
     */
    public boolean cancel(CancelReason reason) {
        synchronized (transitionLock) {
            if (state.get() != TaskState.RUNNING) {
                return false;
            }
            cancelReason.set(reason);
            finishedAt.set(clock.instant());
            state.set(TaskState.CANCELLED);
            cancelled.set(true);
        }
        stopSignal.countDown();
        return true;
    }

    /** Markiert den Task als vollständig abgearbeitet, sofern er nicht schon abgebrochen wurde. */
    public boolean complete() {
        synchronized (transitionLock) {
            if (state.get() != TaskState.RUNNING) {
                return false;
            }
            finishedAt.set(clock.instant());
            state.set(TaskState.COMPLETED);
            return true;
        }
    }

    /** Zeitpunkt, zu dem der Task seinen Endzustand erreicht hat; {@code null}, solange er läuft. */
    public Instant finishedAt() {
        return finishedAt.get();
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    /** {@code true}, sobald der Task abgebrochen ist oder sein Abbruch gemeldet wurde. */
    public boolean isStopping() {
        return cancelled.get() || abortRequest.get() != null;
    }

    public CancelReason cancelReason() {
        return cancelReason.get();
    }

    public TaskState state() {
        return state.get();
    }

    /**
     * Wartet bis zu {@code maxWait} oder bis der Task {@linkplain #isStopping() anhält} — was zuerst eintritt. So
     * reagieren schlafende Threads sofort, ohne dass Interrupts nötig sind.
     *
     * @return {@code true}, wenn der Task anhält
     */
    public boolean awaitStop(Duration maxWait) {
        try {
            stopSignal.await(maxWait.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return isStopping();
    }

    /** {@code true}, sobald die maximale Laufzeit erreicht ist (geprüft im Zyklus des Consumers). */
    public boolean isTimedOut() {
        return !clock.instant().isBefore(startedAt.plus(timeout));
    }

    /** {@code true}, sobald so viele Dateien fehlerhaft waren, wie {@code error-threshold} erlaubt. */
    public boolean errorThresholdReached() {
        return errorCount.get() >= errorThreshold;
    }

    // --- Zähler ----------------------------------------------------------------------------------------------

    /**
     * Zählt eine fehlerhafte Datei. Ob damit die Fehlerschwelle erreicht ist, prüft der Consumer.
     *
     * @return der neue Stand des Fehlerzählers
     */
    public int recordError() {
        return errorCount.incrementAndGet();
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

    /** Eine bereits früher übermittelte Datei, deren Statusabfrage dieser Task wieder aufnimmt. */
    public void recordResumed() {
        resumedCount.incrementAndGet();
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
        return new TaskSnapshot(userId, jobId.value(), run, state.get(), cancelReason.get(), startedAt,
                finishedAt.get(), producerFinished.get(), submittedCount.get(), resumedCount.get(),
                succeededCount.get(), errorCount.get(), pending, abandonedCount.get());
    }
}
