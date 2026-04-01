package main.java.com.logmind.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Centralised error handling.
 *
 * Uses Spring 6's ProblemDetail (RFC 7807) for structured JSON error bodies.
 *
 * Every error response looks like:
 * {
 *   "type": "https://logmind.dev/errors/validation-failed",
 *   "title": "Validation failed",
 *   "status": 400,
 *   "detail": "3 field(s) failed validation",
 *   "timestamp": "2025-01-15T10:30:00Z",
 *   "errors": { "service": "must not be blank", "level": "must not be null" }
 * }
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    // Bean Validation failures (e.g. missing required fields, wrong format)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> errors = ex.getBindingResult().getFieldErrors()
                .stream()
                .collect(Collectors.toMap(
                        FieldError::getField,
                        fe -> fe.getDefaultMessage() != null ? fe.getDefaultMessage() : "invalid value",
                        (a, b) -> a   // keep first message if a field has multiple violations
                ));

        ProblemDetail detail = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST,
                errors.size() + " field(s) failed validation"
        );
        detail.setType(URI.create("https://logmind.dev/errors/validation-failed"));
        detail.setTitle("Validation failed");
        detail.setProperty("timestamp", Instant.now());
        detail.setProperty("errors", errors);
        return detail;
    }

    // Business rule violations (batch size, unknown service, etc.)
    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgument(IllegalArgumentException ex) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        detail.setType(URI.create("https://logmind.dev/errors/bad-request"));
        detail.setTitle("Bad request");
        detail.setProperty("timestamp", Instant.now());
        return detail;
    }

    // Catch-all — log the stack trace but don't leak internals to the caller
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("Unhandled exception", ex);
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred. Check the server logs."
        );
        detail.setType(URI.create("https://logmind.dev/errors/internal-error"));
        detail.setTitle("Internal server error");
        detail.setProperty("timestamp", Instant.now());
        return detail;
    }
}