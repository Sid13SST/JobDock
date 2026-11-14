package com.jobdock.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.PostgresIntegrationTest;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.NewJob;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Retries under contention, against real Postgres: while jobs bounce between RUNNING and
 * FAILED_RETRY across many workers, each job is held by at most one worker at any moment, every
 * attempt number runs exactly once, and every job ends in the expected terminal state.
 */
class RetryConcurrencyIT extends PostgresIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RetryConcurrencyIT.class);

    @Autowired
    JobRepository repository;

    private final Map<UUID, AtomicInteger> inFlight = new ConcurrentHashMap<>();
    private final AtomicInteger overlaps = new AtomicInteger();
    /** attempt numbers executed per job, in execution order */
    private final Map<UUID, List<Integer>> attemptsByJob = new ConcurrentHashMap<>();
    /** executing thread per job, in execution order */
    private final Map<UUID, List<String>> threadsByJob = new ConcurrentHashMap<>();

    @Test
    void retryingJobsAreHeldByOneWorkerAtATimeAndEachAttemptRunsOnce() {
        int flakyJobs = 300; // fail attempts 1 and 2, succeed on 3 (maxAttempts 5)
        int doomedJobs = 100; // always fail, maxAttempts 3 -> DEAD
        Set<UUID> flaky = new HashSet<>();
        Set<UUID> doomed = new HashSet<>();
        for (int i = 0; i < flakyJobs; i++) {
            flaky.add(enqueue("flaky", 5));
        }
        for (int i = 0; i < doomedJobs; i++) {
            doomed.add(enqueue("doomed", 3));
        }
        HandlerRegistry registry = new HandlerRegistry(List.of(handler("flaky", 2), handler("doomed", Integer.MAX_VALUE)));
        BackoffPolicy backoff = BackoffPolicy.withRandomJitter(Duration.ofMillis(5), Duration.ofMillis(40));
        WorkerProperties props = new WorkerProperties(true, 4, 5, Duration.ofMillis(5), Duration.ofSeconds(10));
        WorkerPool poolA = new WorkerPool(props, repository, registry, backoff);
        WorkerPool poolB = new WorkerPool(props, repository, registry, backoff);

        poolA.start();
        poolB.start();
        try {
            await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofMillis(100))
                    .until(() -> count("status IN ('SUCCEEDED', 'DEAD')") == flakyJobs + doomedJobs);
        } finally {
            poolA.stop();
            poolB.stop();
        }

        long movedAcrossThreads = threadsByJob.values().stream()
                .filter(ts -> new HashSet<>(ts).size() > 1).count();
        log.info("Jobs whose attempts ran on more than one worker thread: {} of {}", movedAcrossThreads,
                threadsByJob.size());

        assertThat(overlaps.get()).as("a job was executing on two workers at once").isZero();
        assertThat(attemptsByJob.keySet()).isEqualTo(union(flaky, doomed));
        assertThat(attemptsByJob.values()).as("each attempt number executed exactly once, in order")
                .allSatisfy(attempts -> assertThat(attempts).containsExactly(1, 2, 3));

        assertThat(count("type = 'flaky' AND status = 'SUCCEEDED' AND attempts = 3")).isEqualTo(flakyJobs);
        assertThat(count("type = 'doomed' AND status = 'DEAD' AND attempts = 3"
                + " AND last_error = 'java.lang.IllegalStateException: failure on attempt 3'")).isEqualTo(doomedJobs);
        assertThat(count("locked_by IS NOT NULL")).isZero();
        assertThat(countJobs()).isEqualTo(flakyJobs + doomedJobs);
    }

    /** Throws on attempts 1..failures, succeeds afterwards; detects concurrent execution of a job. */
    private JobHandler handler(String type, int failures) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(Job job) throws InterruptedException {
                AtomicInteger running = inFlight.computeIfAbsent(job.id(), id -> new AtomicInteger());
                if (running.incrementAndGet() != 1) {
                    overlaps.incrementAndGet();
                }
                try {
                    attemptsByJob.computeIfAbsent(job.id(), id -> Collections.synchronizedList(new ArrayList<>()))
                            .add(job.attempts());
                    threadsByJob.computeIfAbsent(job.id(), id -> Collections.synchronizedList(new ArrayList<>()))
                            .add(Thread.currentThread().getName());
                    // Widen the window in which a second holder would be detected.
                    Thread.sleep(ThreadLocalRandom.current().nextInt(3));
                    if (job.attempts() <= failures) {
                        throw new IllegalStateException("failure on attempt " + job.attempts());
                    }
                } finally {
                    running.decrementAndGet();
                }
            }
        };
    }

    private UUID enqueue(String type, int maxAttempts) {
        return repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob(type, JsonNodeFactory.instance.objectNode(), null, null, maxAttempts)).orElseThrow().id();
    }

    private long count(String where) {
        return jdbc.sql("SELECT count(*) FROM jobs WHERE " + where).query(Long.class).single();
    }

    private static Set<UUID> union(Set<UUID> a, Set<UUID> b) {
        Set<UUID> all = new HashSet<>(a);
        all.addAll(b);
        return all;
    }
}
