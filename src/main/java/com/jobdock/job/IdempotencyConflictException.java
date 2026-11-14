package com.jobdock.job;

/** The idempotency key was already used for a request with a different type or payload. */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String key) {
        super("Idempotency-Key '" + key + "' was already used with a different type or payload");
    }
}
