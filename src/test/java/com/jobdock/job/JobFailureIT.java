package com.jobdock.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.PostgresIntegrationTest;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Failure transitions and retry eligibility, against real Postgres. */
class JobFailureIT extends PostgresIntegrationTest {

    private static final Set<String> NOOP = Set.of("noop");

    @Autowired
    JobRepository repository;

    @Test
    void failureBelowMaxAttemptsSchedulesRetryAtDatabaseNowPlusDelay() {
        Job job = claimNew(3);

        Optional<FailureOutcome> outcome = repository.markFailed(job.id(), "w1", "boom", Duration.ofMillis(2_500));

        assertThat(outcome).map(FailureOutcome::status).contains(JobStatus.FAILED_RETRY);
        Job failed = reload(job);
        assertThat(failed.status()).isEqualTo(JobStatus.FAILED_RETRY);
        assertThat(failed.lockedBy()).isNull();
        assertThat(failed.lastError()).isEqualTo("boom");
        assertThat(failed.finishedAt()).isNull();
        assertThat(failed.attempts()).isEqualTo(1);
        // run_at and updated_at are both computed from the same statement's now().
        assertThat(Duration.between(failed.updatedAt(), failed.runAt())).isEqualTo(Duration.ofMillis(2_500));
        assertThat(outcome.get().runAt()).isEqualTo(failed.runAt());
    }

    @Test
    void failureOnLastAttemptDeadLettersAndKeepsLastError() {
        Job job = claimNew(1);

        Optional<FailureOutcome> outcome = repository.markFailed(job.id(), "w1", "fatal", Duration.ofSeconds(10));

        assertThat(outcome).map(FailureOutcome::status).contains(JobStatus.DEAD);
        Job dead = reload(job);
        assertThat(dead.status()).isEqualTo(JobStatus.DEAD);
        assertThat(dead.lastError()).isEqualTo("fatal");
        assertThat(dead.lockedBy()).isNull();
        assertThat(dead.finishedAt()).isNotNull();
        assertThat(dead.runAt()).as("no retry scheduled").isEqualTo(job.runAt());
    }

    @Test
    void failureByNonOwnerOrOnNonRunningJobChangesNothing() {
        Job job = claimNew(3);

        assertThat(repository.markFailed(job.id(), "someone-else", "x", Duration.ZERO)).isEmpty();
        assertThat(reload(job).status()).isEqualTo(JobStatus.RUNNING);

        assertThat(repository.markSucceeded(job.id(), "w1")).isTrue();
        assertThat(repository.markFailed(job.id(), "w1", "x", Duration.ZERO)).isEmpty();
        assertThat(reload(job).status()).isEqualTo(JobStatus.SUCCEEDED);
    }

    @Test
    void retryIsNotClaimedBeforeRunAtButIsAfterwards() {
        Job job = claimNew(3);
        repository.markFailed(job.id(), "w1", "boom", Duration.ofHours(1));

        assertThat(repository.claim("w2", NOOP, 10)).isEmpty();

        makeDue(job);
        List<Job> retried = repository.claim("w2", NOOP, 10);
        assertThat(retried).singleElement().satisfies(j -> {
            assertThat(j.id()).isEqualTo(job.id());
            assertThat(j.status()).isEqualTo(JobStatus.RUNNING);
            assertThat(j.lockedBy()).isEqualTo("w2");
            assertThat(j.attempts()).isEqualTo(2);
            assertThat(j.lastError()).as("previous error kept until overwritten").isEqualTo("boom");
        });
    }

    @Test
    void deadJobIsNeverClaimedAgain() {
        Job job = claimNew(1);
        repository.markFailed(job.id(), "w1", "fatal", Duration.ZERO);
        makeDue(job);

        assertThat(repository.claim("w2", NOOP, 10)).isEmpty();
    }

    @Test
    void longErrorIsTruncated() {
        Job job = claimNew(3);

        repository.markFailed(job.id(), "w1", "e".repeat(JobRepository.MAX_ERROR_LENGTH + 500), Duration.ZERO);

        assertThat(reload(job).lastError()).hasSize(JobRepository.MAX_ERROR_LENGTH);
    }

    private Job claimNew(int maxAttempts) {
        repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob("noop", JsonNodeFactory.instance.objectNode(), null, null, maxAttempts)).orElseThrow();
        return repository.claim("w1", NOOP, 1).getFirst();
    }

    /** Test-only time travel: make a scheduled retry due now instead of waiting for it. */
    private void makeDue(Job job) {
        jdbc.sql("UPDATE jobs SET run_at = now() - interval '1 second' WHERE id = ?").param(job.id()).update();
    }

    private Job reload(Job job) {
        return repository.findById(job.id()).orElseThrow();
    }
}
