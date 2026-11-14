package com.jobdock.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Branching logic of enqueue; database behaviour itself is covered by JobRepositoryIT. */
class JobServiceTest {

    private final ObjectMapper om = new ObjectMapper();
    private final JobRepository repository = mock(JobRepository.class);
    private final JobService service = new JobService(repository);

    @Test
    void newJobIsCreated() {
        Job job = job("email", om.createObjectNode().put("a", 1), "k");
        when(repository.insertIfAbsent(any(), any())).thenReturn(Optional.of(job));

        EnqueueResult result = service.enqueue(new NewJob("email", job.payload(), "k", null));

        assertThat(result).isEqualTo(new EnqueueResult.Created(job));
    }

    @Test
    void sameKeySameRequestIsReplayed() {
        Job existing = job("email", om.createObjectNode().put("a", 1).put("b", 2), "k");
        when(repository.insertIfAbsent(any(), any())).thenReturn(Optional.empty());
        when(repository.findByIdempotencyKey("k")).thenReturn(Optional.of(existing));

        // Same object, different field order.
        JsonNode payload = om.createObjectNode().put("b", 2).put("a", 1);
        EnqueueResult result = service.enqueue(new NewJob("email", payload, "k", null));

        assertThat(result).isEqualTo(new EnqueueResult.Replayed(existing));
    }

    @Test
    void sameKeyDifferentPayloadIsRejected() {
        when(repository.insertIfAbsent(any(), any())).thenReturn(Optional.empty());
        when(repository.findByIdempotencyKey("k"))
                .thenReturn(Optional.of(job("email", om.createObjectNode().put("a", 1), "k")));

        assertThatThrownBy(() -> service.enqueue(new NewJob("email", om.createObjectNode().put("a", 2), "k", null)))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void sameKeyDifferentTypeIsRejected() {
        JsonNode payload = om.createObjectNode().put("a", 1);
        when(repository.insertIfAbsent(any(), any())).thenReturn(Optional.empty());
        when(repository.findByIdempotencyKey("k")).thenReturn(Optional.of(job("email", payload, "k")));

        assertThatThrownBy(() -> service.enqueue(new NewJob("sms", payload, "k", null)))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    private static Job job(String type, JsonNode payload, String key) {
        Instant now = Instant.now();
        return new Job(UUID.randomUUID(), type, payload, JobStatus.QUEUED, 0, key, now, null, null, now, now,
                null, null);
    }
}
