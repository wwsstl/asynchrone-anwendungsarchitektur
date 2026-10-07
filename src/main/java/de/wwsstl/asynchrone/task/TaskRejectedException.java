package de.wwsstl.asynchrone.task;

/** Ein Start wurde abgewiesen; {@link #reason()} nennt die Ursache. */
public class TaskRejectedException extends RuntimeException {

    private final RejectReason reason;

    public TaskRejectedException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
