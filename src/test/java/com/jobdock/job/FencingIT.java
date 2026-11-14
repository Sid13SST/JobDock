package com.jobdock.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.PostgresIntegrationTest;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Results are fenced by (owner, attempt): after a job was taken away from a claim and claimed
 * again, the old claim can no longer record success or failure.
 */
class FencingIT extends PostgresIntegrationTest {

    private static final Set<String> NOOP = Set.of("noop");
    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    JobRepository repository;

    @Test
    void staleClaimOfAnotherWorkerCannotRecordAResult() {
        Job first = claimNew("w1");
        requeueAsIfLeaseExpired(first.id());
        Job second = repository.claim("w2", NOOP, 10, LEASE).getFirst();

        assertThat(repository.markSucceeded(first.id(), "w1", first.attempts())).isFalse();
        assertThat(repository.markFailed(first.id(), "w1", first.attempts(), "late", Duration.ZERO)).isEmpty();

        Job current = load(first.id());
        assertThat(current.status()).isEqualTo(JobStatus.RUNNING);
        assertThat(current.lockedBy()).isEqualTo("w2");
        assertThat(current.attempts()).isEqualTo(2);

        assertThat(repository.markSucceeded(second.id(), "w2", second.attempts())).isTrue();
    }

    @Test
    void staleClaimOfTheSameWorkerIdIsRejectedByAttempt() {
        // Owner alone would not fence this: the same worker id holds the job again, for attempt 2.
        Job first = claimNew("w1");
        requeueAsIfLeaseExpired(first.id());
        Job second = repository.claim("w1", NOOP, 10, LEASE).getFirst();
        assertThat(second.attempts()).isEqualTo(2);

        assertThat(repository.markSucceeded(first.id(), "w1", 1)).isFalse();
        assertThat(repository.markFailed(first.id(), "w1", 1, "late", Duration.ZERO)).isEmpty();
        assertThat(load(first.id()).status()).isEqualTo(JobStatus.RUNNING);

        assertThat(repository.markFailed(second.id(), "w1", 2, "real failure", Duration.ZERO))
                .map(FailureOutcome::status).contains(JobStatus.FAILED_RETRY);
    }

    private Job claimNew(String worker) {
        repository.insertIfAbsent(UUID.randomUUID(), new NewJob("noop", JsonNodeFactory.instance.objectNode(), null, null));
        return repository.claim(worker, NOOP, 1, LEASE).getFirst();
    }

    /** What the lease reaper does to an expired claim (Batch 3.4), written directly here. */
    private void requeueAsIfLeaseExpired(UUID id) {
        jdbc.sql("""
                UPDATE jobs SET status = 'FAILED_RETRY', locked_by = NULL, locked_until = NULL,
                                run_at = now() - interval '1 second'
                WHERE id = ?""").param(id).update();
    }

    private Job load(UUID id) {
        return repository.findById(id).orElseThrow();
    }
}
