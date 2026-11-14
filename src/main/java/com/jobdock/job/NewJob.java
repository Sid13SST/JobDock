package com.jobdock.job;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Objects;

/**
 * A request to enqueue a job.
 *
 * @param idempotencyKey optional; {@code null} means no deduplication
 * @param runAt optional; {@code null} means "now" according to the database clock
 * @param maxAttempts total attempts allowed (first run included) before the job is DEAD
 */
public record NewJob(String type, JsonNode payload, String idempotencyKey, Instant runAt, int maxAttempts) {

    public static final int DEFAULT_MAX_ATTEMPTS = 5;
    public static final int MAX_MAX_ATTEMPTS = 100;

    public NewJob {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
        if (maxAttempts < 1 || maxAttempts > MAX_MAX_ATTEMPTS) {
            throw new IllegalArgumentException("maxAttempts must be in [1, " + MAX_MAX_ATTEMPTS + "]");
        }
    }

    /** With {@link #DEFAULT_MAX_ATTEMPTS}. */
    public NewJob(String type, JsonNode payload, String idempotencyKey, Instant runAt) {
        this(type, payload, idempotencyKey, runAt, DEFAULT_MAX_ATTEMPTS);
    }
}
