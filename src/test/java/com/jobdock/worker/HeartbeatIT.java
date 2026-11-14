package com.jobdock.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.PostgresIntegrationTest;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.JobStatus;
import com.jobdock.job.NewJob;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The pool's heartbeat thread keeps a long-running job's lease alive while the pool's own reaper
 * runs every 50ms, against real Postgres: the job must not be reaped or run twice.
 */
class HeartbeatIT extends PostgresIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(1);

    @Autowired
    JobRepository repository;

    private final java.util.concurrent.atomic.AtomicInteger executions = new java.util.concurrent.atomic.AtomicInteger();

    @Test
    void heartbeatKeepsLeaseAliveForAJobRunningLongerThanSeveralLeasePeriods() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        JobHandler slow = new JobHandler() {
            @Override
            public String type() {
                return "slow";
            }

            @Override
            public void handle(Job job) throws InterruptedException {
                executions.incrementAndGet();
                started.countDown();
                release.await(60, TimeUnit.SECONDS);
            }
        };
        UUID id = repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob("slow", JsonNodeFactory.instance.objectNode(), null, null)).orElseThrow().id();
        WorkerPool pool = new WorkerPool(
                new WorkerProperties(true, 1, 1, Duration.ofMillis(20), Duration.ofSeconds(5)).withLease(LEASE)
                        .withReaperInterval(Duration.ofMillis(50)),
                repository, new HandlerRegistry(List.of(slow)),
                BackoffPolicy.withRandomJitter(Duration.ofSeconds(1), Duration.ofSeconds(1)));

        pool.start();
        try {
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            Instant firstExpiry = repository.findById(id).orElseThrow().startedAt().plus(LEASE);

            // Wait (by the database clock) until three lease periods past the original expiry.
            await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
                    .until(() -> dbNow().isAfter(firstExpiry.plus(LEASE.multipliedBy(3))));

            Job running = repository.findById(id).orElseThrow();
            assertThat(running.status()).isEqualTo(JobStatus.RUNNING);
            assertThat(running.attempts()).isEqualTo(1);
            assertThat(running.lockedUntil()).as("lease was extended past the original expiry").isAfter(dbNow());

            release.countDown();
            await().atMost(Duration.ofSeconds(10)).until(() -> repository.findById(id).orElseThrow().status()
                    == JobStatus.SUCCEEDED);
        } finally {
            release.countDown();
            pool.stop();
        }
        Job done = repository.findById(id).orElseThrow();
        assertThat(done.attempts()).as("never reclaimed").isEqualTo(1);
        assertThat(done.lastError()).as("never reaped").isNull();
        assertThat(executions.get()).isEqualTo(1);
    }

    private Instant dbNow() {
        return jdbc.sql("SELECT now()").query(java.sql.Timestamp.class).single().toInstant();
    }
}
