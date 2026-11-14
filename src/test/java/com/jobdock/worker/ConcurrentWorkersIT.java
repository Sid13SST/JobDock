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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Success-path guarantee under contention, against real Postgres: with N concurrent workers and
 * M jobs, every job's handler runs exactly once and every job ends SUCCEEDED (none lost, none
 * duplicated). This does not cover crashes or failures, where delivery is at-least-once.
 */
class ConcurrentWorkersIT extends PostgresIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentWorkersIT.class);

    @Autowired
    JobRepository repository;

    /** handler executions per job id */
    private final Map<UUID, AtomicInteger> executions = new ConcurrentHashMap<>();
    /** job ids per worker thread, for the distribution log line */
    private final Map<String, AtomicInteger> perThread = new ConcurrentHashMap<>();

    @Test
    void nWorkersByMJobsEachJobRunsExactlyOnce() throws Exception {
        int workers = 8;
        int jobs = 1000;
        Set<UUID> enqueued = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < jobs; i++) {
            enqueued.add(enqueue());
        }
        HandlerRegistry registry = new HandlerRegistry(List.of(countingHandler()));

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        try {
            List<Future<Integer>> claimedPerWorker = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                Worker worker = new Worker("worker-" + w, repository, registry, 10);
                claimedPerWorker.add(pool.submit(() -> {
                    start.await();
                    int total = 0;
                    int n;
                    while ((n = worker.pollOnce()) > 0) {
                        total += n;
                    }
                    return total;
                }));
            }
            start.countDown();
            int totalClaimed = 0;
            for (Future<Integer> f : claimedPerWorker) {
                totalClaimed += f.get(120, TimeUnit.SECONDS);
            }
            assertThat(totalClaimed).isEqualTo(jobs);
        } finally {
            pool.shutdownNow();
        }

        assertExactlyOnce(enqueued);
    }

    @Test
    void twoWorkerPoolsWithConcurrentProducersLoseNothingAndDuplicateNothing() throws Exception {
        int producers = 4;
        int jobsPerProducer = 250;
        int total = producers * jobsPerProducer;
        HandlerRegistry registry = new HandlerRegistry(List.of(countingHandler()));
        WorkerProperties props = new WorkerProperties(true, 4, 5, Duration.ofMillis(20), Duration.ofSeconds(10));
        // Two pools = two independent "processes" with distinct instance ids sharing one database.
        WorkerPool poolA = new WorkerPool(props, repository, registry);
        WorkerPool poolB = new WorkerPool(props, repository, registry);
        Set<UUID> enqueued = ConcurrentHashMap.newKeySet();

        poolA.start();
        poolB.start();
        ExecutorService producerPool = Executors.newFixedThreadPool(producers);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                futures.add(producerPool.submit(() -> {
                    for (int i = 0; i < jobsPerProducer; i++) {
                        enqueued.add(enqueue());
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get(120, TimeUnit.SECONDS);
            }
            await().atMost(Duration.ofSeconds(120)).until(() -> countSucceeded() == total);
        } finally {
            producerPool.shutdownNow();
            poolA.stop();
            poolB.stop();
        }

        assertThat(enqueued).hasSize(total);
        assertExactlyOnce(enqueued);
    }

    private void assertExactlyOnce(Set<UUID> enqueued) {
        log.info("Jobs handled per worker thread: {}", perThread);
        assertThat(executions.keySet()).as("every enqueued job was executed, nothing else was").isEqualTo(enqueued);
        assertThat(executions.values()).as("no job executed more than once").allSatisfy(c -> assertThat(c.get()).isEqualTo(1));
        assertThat(countSucceeded()).isEqualTo(enqueued.size());
        assertThat(countJobs()).isEqualTo(enqueued.size());
        assertThat(jdbc.sql("SELECT count(*) FROM jobs WHERE attempts <> 1 OR locked_by IS NOT NULL")
                .query(Long.class).single()).as("each job claimed exactly once and released").isZero();
    }

    private UUID enqueue() {
        return repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob("count", JsonNodeFactory.instance.objectNode(), null, null)).orElseThrow().id();
    }

    private long countSucceeded() {
        return jdbc.sql("SELECT count(*) FROM jobs WHERE status = 'SUCCEEDED'").query(Long.class).single();
    }

    private JobHandler countingHandler() {
        return new JobHandler() {
            @Override
            public String type() {
                return "count";
            }

            @Override
            public void handle(Job job) throws InterruptedException {
                executions.computeIfAbsent(job.id(), id -> new AtomicInteger()).incrementAndGet();
                perThread.computeIfAbsent(Thread.currentThread().getName(), n -> new AtomicInteger()).incrementAndGet();
                // Small random work time so claims and completions interleave across workers.
                if (ThreadLocalRandom.current().nextInt(4) == 0) {
                    Thread.sleep(1);
                }
            }
        };
    }
}
