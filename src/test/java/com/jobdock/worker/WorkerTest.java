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
import com.jobdock.job.FailureOutcome;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.JobStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

/** Worker control flow with a stubbed repository; real claim/transition semantics are covered by ITs. */
class WorkerTest {

    /** r = 0: delay(n) = 0.5 * 1s * 2^(n-1). */
    private static final BackoffPolicy BACKOFF =
            new BackoffPolicy(Duration.ofSeconds(1), Duration.ofMinutes(5), () -> 0.0);

    private final JobRepository repository = mock(JobRepository.class);
    private final List<UUID> handled = new ArrayList<>();

    @Test
    void claimsOnlyRegisteredTypesWithConfiguredBatchSize() {
        Worker worker = worker(7, recording("email"));
        when(repository.claim(any(), any(), anyInt(), any())).thenReturn(List.of());

        assertThat(worker.pollOnce()).isZero();
        verify(repository).claim("w1", Set.of("email"), 7, Duration.ofSeconds(30));
    }

    @Test
    void successfulJobsAreMarkedSucceeded() {
        Job a = running("email", 1);
        Job b = running("email", 1);
        Worker worker = worker(10, recording("email"));
        when(repository.claim(any(), any(), anyInt(), any())).thenReturn(List.of(a, b));
        when(repository.markSucceeded(any(), eq("w1"), anyInt())).thenReturn(true);

        assertThat(worker.pollOnce()).isEqualTo(2);

        assertThat(handled).containsExactly(a.id(), b.id());
        verify(repository).markSucceeded(a.id(), "w1", 1);
        verify(repository).markSucceeded(b.id(), "w1", 1);
        verify(repository, never()).markFailed(any(), any(), anyInt(), any(), any());
    }

    @Test
    void failingHandlerRecordsFailureWithBackoffForItsAttemptAndBatchContinues() {
        Job bad = running("boom", 3);
        Job good = running("email", 1);
        Worker worker = worker(10, failing("boom", new IllegalStateException("disk full")), recording("email"));
        when(repository.claim(any(), any(), anyInt(), any())).thenReturn(List.of(bad, good));
        when(repository.markSucceeded(any(), any(), anyInt())).thenReturn(true);
        when(repository.markFailed(any(), any(), anyInt(), any(), any()))
                .thenReturn(Optional.of(new FailureOutcome(JobStatus.FAILED_RETRY, Instant.now())));

        assertThat(worker.pollOnce()).isEqualTo(2);

        // attempt 3 -> 0.5 * 1s * 2^2 = 2s
        verify(repository).markFailed(bad.id(), "w1", 3, "java.lang.IllegalStateException: disk full",
                Duration.ofSeconds(2));
        verify(repository, never()).markSucceeded(eq(bad.id()), any(), anyInt());
        verify(repository).markSucceeded(good.id(), "w1", 1);
    }

    @Test
    void deadOutcomeAndLostOwnershipOnFailureAreTolerated() {
        Job a = running("boom", 5);
        Job b = running("boom", 1);
        Worker worker = worker(10, failing("boom", new RuntimeException()));
        when(repository.claim(any(), any(), anyInt(), any())).thenReturn(List.of(a, b));
        when(repository.markFailed(eq(a.id()), any(), anyInt(), any(), any()))
                .thenReturn(Optional.of(new FailureOutcome(JobStatus.DEAD, Instant.now())));
        when(repository.markFailed(eq(b.id()), any(), anyInt(), any(), any())).thenReturn(Optional.empty());

        assertThat(worker.pollOnce()).isEqualTo(2);

        verify(repository).markFailed(a.id(), "w1", 5, "java.lang.RuntimeException", Duration.ofSeconds(8));
        verify(repository).markFailed(b.id(), "w1", 1, "java.lang.RuntimeException", Duration.ofMillis(500));
    }

    @Test
    void jobWithoutHandlerIsRecordedAsFailedNotStranded() {
        Job orphan = running("unknown", 1);
        Worker worker = worker(10, recording("email"));
        when(repository.claim(any(), any(), anyInt(), any())).thenReturn(List.of(orphan));
        when(repository.markFailed(any(), any(), anyInt(), any(), any())).thenReturn(Optional.empty());

        worker.pollOnce();

        verify(repository).markFailed(orphan.id(), "w1", 1, "No handler registered for type 'unknown'",
                Duration.ofMillis(500));
    }

    @Test
    void heartbeatExtendsAllHeldClaimsAndJobLostBeforeStartIsSkipped() {
        Job first = running("email", 1);
        Job second = running("email", 1);
        Worker[] self = new Worker[1];
        // While the first job runs, a heartbeat finds that the second (not yet started) claim was lost.
        JobHandler heartbeatingHandler = new JobHandler() {
            @Override
            public String type() {
                return "email";
            }

            @Override
            public void handle(Job job) {
                handled.add(job.id());
                if (job.id().equals(first.id())) {
                    assertThat(self[0].heldClaims()).containsOnlyKeys(first.id(), second.id());
                    self[0].heartbeat();
                }
            }
        };
        self[0] = worker(10, heartbeatingHandler);
        when(repository.claim(any(), any(), anyInt(), any())).thenReturn(List.of(first, second));
        when(repository.extendLeases(eq("w1"), any(), eq(Duration.ofSeconds(30)))).thenReturn(Set.of(first.id()));
        when(repository.markSucceeded(any(), any(), anyInt())).thenReturn(true);

        assertThat(self[0].pollOnce()).isEqualTo(2);

        verify(repository).extendLeases("w1", java.util.Map.of(first.id(), 1, second.id(), 1), Duration.ofSeconds(30));
        assertThat(handled).containsExactly(first.id());
        verify(repository, never()).markSucceeded(eq(second.id()), any(), anyInt());
        verify(repository, never()).markFailed(eq(second.id()), any(), anyInt(), any(), any());
        assertThat(self[0].heldClaims()).isEmpty();
    }

    @Test
    void heartbeatWithNothingHeldDoesNotTouchTheDatabase() {
        worker(10, recording("email")).heartbeat();
        verify(repository, never()).extendLeases(any(), any(), any());
    }

    @Test
    void errorsThrownByHandlersAreRecordedAsFailures() {
        Job overflow = running("overflow", 1);
        Job assertion = running("assert", 2);
        Worker worker = worker(10, throwing("overflow", new StackOverflowError()),
                throwing("assert", new AssertionError("invariant broken")));
        when(repository.claim(any(), any(), anyInt(), any())).thenReturn(List.of(overflow, assertion));
        when(repository.markFailed(any(), any(), anyInt(), any(), any()))
                .thenReturn(Optional.of(new FailureOutcome(JobStatus.FAILED_RETRY, Instant.now())));

        assertThat(worker.pollOnce()).isEqualTo(2);

        verify(repository).markFailed(overflow.id(), "w1", 1, "java.lang.StackOverflowError", Duration.ofMillis(500));
        verify(repository).markFailed(assertion.id(), "w1", 2, "java.lang.AssertionError: invariant broken",
                Duration.ofSeconds(1));
    }

    @Test
    void failedResultWriteDoesNotStrandTheRestOfTheBatch() {
        Job a = running("email", 1);
        Job b = running("boom", 1);
        Job c = running("email", 1);
        Worker worker = worker(10, recording("email"), throwing("boom", new IllegalStateException("x")));
        when(repository.claim(any(), any(), anyInt(), any())).thenReturn(List.of(a, b, c));
        when(repository.markSucceeded(eq(a.id()), any(), anyInt()))
                .thenThrow(new DataAccessResourceFailureException("connection lost"));
        when(repository.markFailed(eq(b.id()), any(), anyInt(), any(), any()))
                .thenThrow(new DataAccessResourceFailureException("connection lost"));
        when(repository.markSucceeded(eq(c.id()), any(), anyInt())).thenReturn(true);

        assertThat(worker.pollOnce()).isEqualTo(3);

        assertThat(handled).containsExactly(a.id(), c.id());
        verify(repository).markSucceeded(c.id(), "w1", 1);
        assertThat(worker.heldClaims()).as("nothing left heartbeated; leases of a and b will expire").isEmpty();
    }

    @Test
    void interruptedWorkerDoesNotStartTheRestOfTheBatch() {
        Job first = running("email", 1);
        Job second = running("email", 1);
        JobHandler interrupting = new JobHandler() {
            @Override
            public String type() {
                return "email";
            }

            @Override
            public void handle(Job job) {
                handled.add(job.id());
                Thread.currentThread().interrupt(); // as WorkerPool.stop() does after its timeout
            }
        };
        Worker worker = worker(10, interrupting);
        when(repository.claim(any(), any(), anyInt(), any())).thenReturn(List.of(first, second));
        when(repository.markSucceeded(any(), any(), anyInt())).thenReturn(true);
        try {
            worker.pollOnce();
        } finally {
            Thread.interrupted(); // clear the flag for the next test
        }

        assertThat(handled).containsExactly(first.id());
        verify(repository, never()).markSucceeded(eq(second.id()), any(), anyInt());
        assertThat(worker.heldClaims()).isEmpty();
    }

    @Test
    void lostOwnershipOnCompletionIsTolerated() {
        Job job = running("email", 1);
        Worker worker = worker(10, recording("email"));
        when(repository.claim(any(), any(), anyInt(), any())).thenReturn(List.of(job));
        when(repository.markSucceeded(job.id(), "w1", 1)).thenReturn(false);

        assertThat(worker.pollOnce()).isEqualTo(1);
        assertThat(handled).containsExactly(job.id());
    }

    private Worker worker(int batchSize, JobHandler... handlers) {
        return new Worker("w1", repository, new HandlerRegistry(List.of(handlers)), BACKOFF, batchSize, Duration.ofSeconds(30));
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

    private static JobHandler throwing(String type, Error e) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(Job job) {
                throw e;
            }
        };
    }

    private static JobHandler throwing(String type, RuntimeException e) {
        return failing(type, e);
    }

    private static JobHandler failing(String type, RuntimeException e) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(Job job) {
                throw e;
            }
        };
    }

    private static Job running(String type, int attempts) {
        Instant now = Instant.now();
        return new Job(UUID.randomUUID(), type, JsonNodeFactory.instance.objectNode(), JobStatus.RUNNING, attempts,
                5, null, now, "w1", now.plusSeconds(30), null, now, now, now, null);
    }
}
