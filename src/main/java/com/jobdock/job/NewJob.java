package com.jobdock.job;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Objects;

/**
 * A request to enqueue a job.
 *
 * @param idempotencyKey optional; {@code null} means no deduplication
 * @param runAt optional; {@code null} means "now" according to the database clock
 */
public record NewJob(String type, JsonNode payload, String idempotencyKey, Instant runAt) {

    public NewJob {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
    }
}
