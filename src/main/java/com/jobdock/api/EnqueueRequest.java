package com.jobdock.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.time.Instant;

/**
 * Body of {@code POST /jobs}.
 *
 * @param runAt optional earliest execution time (ISO-8601); defaults to now
 */
public record EnqueueRequest(
        @NotBlank @Size(max = 100) String type,
        JsonNode payload,
        Instant runAt) {

    @JsonIgnore
    @AssertTrue(message = "payload must be a JSON object")
    public boolean isPayloadObject() {
        return payload != null && payload.isObject();
    }
}
