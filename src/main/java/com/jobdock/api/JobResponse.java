package com.jobdock.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.jobdock.job.Job;
import com.jobdock.job.JobStatus;
import java.time.Instant;
import java.util.UUID;

public record JobResponse(
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
        Instant finishedAt) {

    static JobResponse from(Job job) {
        return new JobResponse(job.id(), job.type(), job.payload(), job.status(), job.attempts(), job.maxAttempts(),
                job.idempotencyKey(), job.runAt(), job.lockedBy(), job.lastError(), job.createdAt(),
                job.updatedAt(), job.startedAt(), job.finishedAt());
    }
}
