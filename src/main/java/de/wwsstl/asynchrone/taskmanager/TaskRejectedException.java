package de.wwsstl.asynchrone.taskmanager;

/** Eine Anfrage steht im Konflikt mit dem aktuellen Zustand; der {@link RejectReason} nennt die Ursache. */
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
