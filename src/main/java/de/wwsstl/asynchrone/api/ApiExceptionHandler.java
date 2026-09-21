package de.wwsstl.asynchrone.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import de.wwsstl.asynchrone.files.InvalidUserIdException;
import de.wwsstl.asynchrone.taskmanager.TaskAlreadyRunningException;
import de.wwsstl.asynchrone.taskmanager.TaskNotFoundException;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(InvalidUserIdException.class)
    ProblemDetail invalidUser(InvalidUserIdException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(TaskNotFoundException.class)
    ProblemDetail notFound(TaskNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(TaskAlreadyRunningException.class)
    ProblemDetail conflict(TaskAlreadyRunningException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }
}
