package de.wwsstl.asynchrone.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import de.wwsstl.asynchrone.cloud.CloudUnavailableException;
import de.wwsstl.asynchrone.files.InvalidUserIdException;
import de.wwsstl.asynchrone.task.InvalidTaskNumberException;
import de.wwsstl.asynchrone.task.TaskNotFoundException;
import de.wwsstl.asynchrone.task.TaskRejectedException;

/**
 * Fehlerantworten als {@link ProblemDetail}. Die Eigenschaft {@value #CODE} enthält einen Ursachencode, damit die
 * Oberfläche je Ursache einen passenden Hinweis anzeigen kann.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    static final String CODE = "code";

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(InvalidUserIdException.class)
    ProblemDetail invalidUserId(InvalidUserIdException e) {
        return problem(HttpStatus.BAD_REQUEST, "INVALID_USER_ID", e.getMessage());
    }

    @ExceptionHandler(InvalidTaskNumberException.class)
    ProblemDetail invalidTaskNumber(InvalidTaskNumberException e) {
        return problem(HttpStatus.BAD_REQUEST, "INVALID_TASK_NUMBER", e.getMessage());
    }

    @ExceptionHandler(TaskNotFoundException.class)
    ProblemDetail notFound(TaskNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, "TASK_NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(TaskRejectedException.class)
    ProblemDetail rejected(TaskRejectedException e) {
        return problem(HttpStatus.CONFLICT, e.reason().name(), e.getMessage());
    }

    @ExceptionHandler(CloudUnavailableException.class)
    ProblemDetail cloudUnavailable(CloudUnavailableException e) {
        log.warn("Anfrage abgewiesen: {}", e.getMessage());
        return problem(HttpStatus.BAD_GATEWAY, "CLOUD_UNAVAILABLE", e.getMessage());
    }

    private static ProblemDetail problem(HttpStatus status, String code, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setProperty(CODE, code);
        return problem;
    }
}
