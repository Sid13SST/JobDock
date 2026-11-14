package com.jobdock.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.JobStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Worker control flow with a stubbed repository; real claim semantics are covered by ITs. */
class WorkerTest {

    private final JobRepository repository = mock(JobRepository.class);
    private final List<UUID> handled = new ArrayList<>();

    @Test
    void claimsOnlyRegisteredTypesWithConfiguredBatchSize() {
        Worker worker = new Worker("w1", repository, registry(recording("email")), 7);
        when(repository.claim(any(), any(), anyInt())).thenReturn(List.of());

        assertThat(worker.pollOnce()).isZero();
        verify(repository).claim("w1", Set.of("email"), 7);
    }

    @Test
    void successfulJobsAreMarkedSucceeded() {
        Job a = running("email");
        Job b = running("email");
        Worker worker = new Worker("w1", repository, registry(recording("email")), 10);
        when(repository.claim(any(), any(), anyInt())).thenReturn(List.of(a, b));
        when(repository.markSucceeded(any(), eq("w1"))).thenReturn(true);

        assertThat(worker.pollOnce()).isEqualTo(2);

        assertThat(handled).containsExactly(a.id(), b.id());
        verify(repository).markSucceeded(a.id(), "w1");
        verify(repository).markSucceeded(b.id(), "w1");
    }

    @Test
    void failingHandlerIsNotMarkedSucceededAndDoesNotStopTheBatch() {
        Job bad = running("boom");
        Job good = running("email");
        JobHandler failing = new JobHandler() {
            @Override
            public String type() {
                return "boom";
            }

            @Override
            public void handle(Job job) {
                throw new IllegalStateException("handler failed");
            }
        };
        Worker worker = new Worker("w1", repository, registry(failing, recording("email")), 10);
        when(repository.claim(any(), any(), anyInt())).thenReturn(List.of(bad, good));
        when(repository.markSucceeded(any(), any())).thenReturn(true);

        assertThat(worker.pollOnce()).isEqualTo(2);

        verify(repository, never()).markSucceeded(eq(bad.id()), any());
        verify(repository).markSucceeded(good.id(), "w1");
    }

    @Test
    void lostOwnershipOnCompletionIsTolerated() {
        Job job = running("email");
        Worker worker = new Worker("w1", repository, registry(recording("email")), 10);
        when(repository.claim(any(), any(), anyInt())).thenReturn(List.of(job));
        when(repository.markSucceeded(job.id(), "w1")).thenReturn(false);

        assertThat(worker.pollOnce()).isEqualTo(1);
        assertThat(handled).containsExactly(job.id());
    }

    private JobHandler recording(String type) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(Job job) {
                handled.add(job.id());
            }
        };
    }

    private static HandlerRegistry registry(JobHandler... handlers) {
        return new HandlerRegistry(List.of(handlers));
    }

    private static Job running(String type) {
        Instant now = Instant.now();
        return new Job(UUID.randomUUID(), type, JsonNodeFactory.instance.objectNode(), JobStatus.RUNNING, 1, null,
                now, "w1", null, now, now, now, null);
    }
}
