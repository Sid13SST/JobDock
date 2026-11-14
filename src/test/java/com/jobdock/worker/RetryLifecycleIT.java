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
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** End-to-end retry -> dead-letter path through the Worker, against real Postgres. */
class RetryLifecycleIT extends PostgresIntegrationTest {

    @Autowired
    JobRepository repository;

    /** Attempt numbers seen by the handler, in order. */
    private final List<Integer> attemptsSeen = new CopyOnWriteArrayList<>();
    /** System.nanoTime() at each handler invocation. */
    private final List<Long> invokedAtNanos = new CopyOnWriteArrayList<>();

    @Test
    void failingJobIsRetriedWithBackoffThenDeadLetteredWithLastError() {
        // r = 0 -> delays 5s, 10s for attempts 1, 2 (base 10s, halved by the minimum jitter factor).
        BackoffPolicy backoff = new BackoffPolicy(Duration.ofSeconds(10), Duration.ofMinutes(1), () -> 0.0);
        Worker worker = new Worker("w1", repository, registry(Integer.MAX_VALUE), backoff, 10);
        UUID id = enqueue(3);

        assertThat(worker.pollOnce()).isEqualTo(1);
        Job afterFirst = load(id);
        assertThat(afterFirst.status()).isEqualTo(JobStatus.FAILED_RETRY);
        assertThat(afterFirst.attempts()).isEqualTo(1);
        assertThat(afterFirst.lastError()).isEqualTo("java.lang.IllegalStateException: failure on attempt 1");
        assertThat(retryDelay(afterFirst)).isEqualTo(Duration.ofSeconds(5));

        assertThat(worker.pollOnce()).as("retry not due yet").isZero();

        makeDue(id);
        assertThat(worker.pollOnce()).isEqualTo(1);
        Job afterSecond = load(id);
        assertThat(afterSecond.status()).isEqualTo(JobStatus.FAILED_RETRY);
        assertThat(afterSecond.attempts()).isEqualTo(2);
        assertThat(retryDelay(afterSecond)).isEqualTo(Duration.ofSeconds(10));

        makeDue(id);
        assertThat(worker.pollOnce()).isEqualTo(1);
        Job dead = load(id);
        assertThat(dead.status()).isEqualTo(JobStatus.DEAD);
        assertThat(dead.attempts()).isEqualTo(3);
        assertThat(dead.lastError()).isEqualTo("java.lang.IllegalStateException: failure on attempt 3");
        assertThat(dead.finishedAt()).isNotNull();
        assertThat(dead.lockedBy()).isNull();

        makeDue(id);
        assertThat(worker.pollOnce()).as("DEAD is terminal").isZero();
        assertThat(attemptsSeen).containsExactly(1, 2, 3);
    }

    @Test
    void jobThatFailsTwiceThenSucceedsEndsSucceeded() {
        BackoffPolicy backoff = new BackoffPolicy(Duration.ofSeconds(10), Duration.ofMinutes(1), () -> 0.0);
        Worker worker = new Worker("w1", repository, registry(2), backoff, 10);
        UUID id = enqueue(5);

        worker.pollOnce();
        makeDue(id);
        worker.pollOnce();
        makeDue(id);
        worker.pollOnce();

        Job done = load(id);
        assertThat(done.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(done.attempts()).isEqualTo(3);
        assertThat(attemptsSeen).containsExactly(1, 2, 3);
    }

    @Test
    void workerPoolRetriesOnRealBackoffTimingUntilDead() {
        // No time travel: the pool must wait for run_at itself. r = 0 -> delays 50ms, 100ms.
        BackoffPolicy backoff = new BackoffPolicy(Duration.ofMillis(100), Duration.ofSeconds(1), () -> 0.0);
        WorkerPool pool = new WorkerPool(new WorkerProperties(true, 2, 5, Duration.ofMillis(10), Duration.ofSeconds(5)),
                repository, registry(Integer.MAX_VALUE), backoff);
        UUID id = enqueue(3);

        pool.start();
        try {
            await().atMost(Duration.ofSeconds(30)).until(() -> load(id).status() == JobStatus.DEAD);
        } finally {
            pool.stop();
        }

        assertThat(attemptsSeen).containsExactly(1, 2, 3);
        assertThat(load(id).attempts()).isEqualTo(3);
        // Each retry started no earlier than its backoff after the previous attempt started.
        assertThat(Duration.ofNanos(invokedAtNanos.get(1) - invokedAtNanos.get(0))).isGreaterThanOrEqualTo(Duration.ofMillis(50));
        assertThat(Duration.ofNanos(invokedAtNanos.get(2) - invokedAtNanos.get(1))).isGreaterThanOrEqualTo(Duration.ofMillis(100));
    }

    /** Handler that throws on attempts 1..failures and succeeds afterwards. */
    private HandlerRegistry registry(int failures) {
        return new HandlerRegistry(List.of(new JobHandler() {
            @Override
            public String type() {
                return "flaky";
            }

            @Override
            public void handle(Job job) {
                invokedAtNanos.add(System.nanoTime());
                attemptsSeen.add(job.attempts());
                if (job.attempts() <= failures) {
                    throw new IllegalStateException("failure on attempt " + job.attempts());
                }
            }
        }));
    }

    private UUID enqueue(int maxAttempts) {
        return repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob("flaky", JsonNodeFactory.instance.objectNode(), null, null, maxAttempts)).orElseThrow().id();
    }

    /** run_at and updated_at come from the same statement's now(), so this is the exact scheduled delay. */
    private static Duration retryDelay(Job job) {
        return Duration.between(job.updatedAt(), job.runAt());
    }

    /** Test-only time travel: make a scheduled retry due now instead of waiting for it. */
    private void makeDue(UUID id) {
        jdbc.sql("UPDATE jobs SET run_at = now() - interval '1 second' WHERE id = ? AND status = 'FAILED_RETRY'")
                .param(id).update();
    }

    private Job load(UUID id) {
        return repository.findById(id).orElseThrow();
    }
}
