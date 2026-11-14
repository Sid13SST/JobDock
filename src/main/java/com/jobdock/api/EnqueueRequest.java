package com.jobdock.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.jobdock.job.NewJob;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.time.Instant;

/**
 * Body of {@code POST /jobs}.
 *
 * @param runAt optional earliest execution time (ISO-8601); defaults to now
 * @param maxAttempts optional total attempts before the job is DEAD; defaults to
 *     {@link com.jobdock.job.NewJob#DEFAULT_MAX_ATTEMPTS}
 */
public record EnqueueRequest(
        @NotBlank @Size(max = 100) String type,
        JsonNode payload,
        Instant runAt,
        @Min(1) @Max(NewJob.MAX_MAX_ATTEMPTS) Integer maxAttempts) {

    @JsonIgnore
    @AssertTrue(message = "payload must be a JSON object")
    public boolean isPayloadObject() {
        return payload != null && payload.isObject();
    }
}
