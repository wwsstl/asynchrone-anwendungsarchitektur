package de.wwsstl.asynchrone.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import de.wwsstl.asynchrone.cloud.CloudCallFailedException;
import de.wwsstl.asynchrone.cloud.InvalidBatchJobIdException;
import de.wwsstl.asynchrone.files.InvalidUserIdException;
import de.wwsstl.asynchrone.taskmanager.TaskNotFoundException;
import de.wwsstl.asynchrone.taskmanager.TaskRejectedException;

/**
 * Fehlerantworten als {@link ProblemDetail}. Abgewiesene Anfragen ({@code 409}) und nicht erreichbare Cloud-Dienste
 * ({@code 502}) tragen zusätzlich die Eigenschaft {@value #CODE} mit einem Ursachencode, damit die Oberfläche je
 * Ursache einen passenden Hinweis anzeigen kann.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    static final String CODE = "code";
    static final String CLOUD_UNAVAILABLE = "CLOUD_UNAVAILABLE";

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler({InvalidUserIdException.class, InvalidBatchJobIdException.class})
    ProblemDetail badRequest(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(TaskNotFoundException.class)
    ProblemDetail notFound(TaskNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(TaskRejectedException.class)
    ProblemDetail conflict(TaskRejectedException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
        problem.setProperty(CODE, e.reason().name());
        return problem;
    }

    @ExceptionHandler(CloudCallFailedException.class)
    ProblemDetail cloudUnavailable(CloudCallFailedException e) {
        log.warn("Anfrage abgebrochen: {}", e.getMessage());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, e.getMessage());
        problem.setProperty(CODE, CLOUD_UNAVAILABLE);
        return problem;
    }
}
