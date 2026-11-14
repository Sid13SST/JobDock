package com.jobdock.job;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/** A row of the {@code jobs} table. All timestamps come from the database clock. */
public record Job(
        UUID id,
        String type,
        JsonNode payload,
        JobStatus status,
        int attempts,
        int maxAttempts,
        String idempotencyKey,
        Instant runAt,
        String lockedBy,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant finishedAt) {}
