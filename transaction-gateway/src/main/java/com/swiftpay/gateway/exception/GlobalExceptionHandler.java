package com.swiftpay.gateway.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(DuplicateTransactionException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, Object> handleDuplicate(
            DuplicateTransactionException ex
    ) {

        return Map.of(
                "timestamp", LocalDateTime.now(),
                "errorCode", "DUPLICATE_TRANSACTION",
                "message", ex.getMessage()
        );
    }
}