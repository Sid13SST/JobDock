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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * A "zombie" is a worker that stalled without heartbeating (a plain Worker, no pool), so its
 * lease expired and the job moved on, and that then wakes up and tries to record a result.
 * Fencing must reject it. The handler runs twice in these scenarios: that is the at-least-once
 * behaviour the design states, not a bug.
 */
class ZombieWorkerIT extends PostgresIntegrationTest {

    private static final Duration SHORT_LEASE = Duration.ofMillis(300);
    private static final BackoffPolicy BACKOFF = new BackoffPolicy(Duration.ofSeconds(1), Duration.ofSeconds(1), () -> 0.0);

    @Autowired
    JobRepository repository;

    private final AtomicInteger executions = new AtomicInteger();

    @Test
    void zombieSuccessCannotOverwriteTheNewOwnersClaimOrResult() throws Exception {
        Gate zombieGate = new Gate();
        Gate ownerGate = new Gate();
        UUID id = enqueue();
        Worker zombie = new Worker("zombie", repository, registry(zombieGate, false), BACKOFF, 1, SHORT_LEASE);
        Worker owner = new Worker("owner", repository, registry(ownerGate, false), BACKOFF, 1, Duration.ofSeconds(30));

        CompletableFuture<Integer> zombieRun = CompletableFuture.supplyAsync(zombie::pollOnce);
        zombieGate.awaitEntered();
        reapAfterLeaseExpiry(id);

        CompletableFuture<Integer> ownerRun = CompletableFuture.supplyAsync(owner::pollOnce);
        ownerGate.awaitEntered();
        assertThat(load(id).lockedBy()).isEqualTo("owner");

        zombieGate.release(); // zombie's handler returns normally -> its success must be rejected
        zombieRun.get(10, TimeUnit.SECONDS);
        Job whileOwnerRuns = load(id);
        assertThat(whileOwnerRuns.status()).isEqualTo(JobStatus.RUNNING);
        assertThat(whileOwnerRuns.lockedBy()).isEqualTo("owner");
        assertThat(whileOwnerRuns.attempts()).isEqualTo(2);

        ownerGate.release();
        ownerRun.get(10, TimeUnit.SECONDS);
        Job done = load(id);
        assertThat(done.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(2);
        assertThat(executions.get()).as("at-least-once: the handler ran in both workers").isEqualTo(2);
    }

    @Test
    void zombieFailureAfterTheNewOwnerSucceededDoesNotReopenTheJob() throws Exception {
        Gate zombieGate = new Gate();
        UUID id = enqueue();
        Worker zombie = new Worker("zombie", repository, registry(zombieGate, true), BACKOFF, 1, SHORT_LEASE);

        CompletableFuture<Integer> zombieRun = CompletableFuture.supplyAsync(zombie::pollOnce);
        zombieGate.awaitEntered();
        reapAfterLeaseExpiry(id);

        Worker owner = new Worker("owner", repository, registry(null, false), BACKOFF, 1, Duration.ofSeconds(30));
        assertThat(owner.pollOnce()).isEqualTo(1);
        assertThat(load(id).status()).isEqualTo(JobStatus.SUCCEEDED);

        zombieGate.release(); // zombie's handler now throws -> its failure must be rejected
        zombieRun.get(10, TimeUnit.SECONDS);
        Job done = load(id);
        assertThat(done.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(done.lastError()).startsWith("Lease expired: worker zombie");
        assertThat(executions.get()).isEqualTo(2);
    }

    private void reapAfterLeaseExpiry(UUID id) {
        LeaseReaper reaper = new LeaseReaper(repository);
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50)).until(() -> reaper.reapOnce() == 1);
        assertThat(load(id).status()).isEqualTo(JobStatus.FAILED_RETRY);
    }

    /** Handler that (optionally) blocks on a gate, then succeeds or throws. */
    private HandlerRegistry registry(Gate gate, boolean throwAfterGate) {
        return new HandlerRegistry(List.of(new JobHandler() {
            @Override
            public String type() {
                return "work";
            }

            @Override
            public void handle(Job job) throws InterruptedException {
                executions.incrementAndGet();
                if (gate != null) {
                    gate.enter();
                }
                if (throwAfterGate) {
                    throw new IllegalStateException("zombie failed late");
                }
            }
        }));
    }

    private UUID enqueue() {
        return repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob("work", JsonNodeFactory.instance.objectNode(), null, null)).orElseThrow().id();
    }

    private Job load(UUID id) {
        return repository.findById(id).orElseThrow();
    }

    private static final class Gate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void enter() throws InterruptedException {
            entered.countDown();
            released.await(30, TimeUnit.SECONDS);
        }

        void awaitEntered() throws InterruptedException {
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
        }

        void release() {
            released.countDown();
        }
    }
}
