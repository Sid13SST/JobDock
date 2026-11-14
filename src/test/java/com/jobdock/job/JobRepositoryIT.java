package com.jobdock.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jobdock.PostgresIntegrationTest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class JobRepositoryIT extends PostgresIntegrationTest {

    private static final Set<String> NOOP = Set.of("noop");
    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    JobRepository repository;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    DataSource dataSource;

    @Test
    void insertCreatesQueuedJob() {
        ObjectNode payload = objectMapper.createObjectNode().put("to", "a@example.com");
        Job job = repository.insertIfAbsent(UUID.randomUUID(), new NewJob("email", payload, null, null)).orElseThrow();

        assertThat(job.status()).isEqualTo(JobStatus.QUEUED);
        assertThat(job.attempts()).isZero();
        assertThat(job.payload()).isEqualTo(payload);
        assertThat(job.lockedBy()).isNull();
        assertThat(job.runAt()).isNotNull();
        assertThat(repository.findById(job.id())).contains(job);
    }

    @Test
    void insertStoresMaxAttempts() {
        Job custom = repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob("noop", objectMapper.createObjectNode(), null, null, 3)).orElseThrow();
        Job defaulted = insertDue(null);

        assertThat(custom.maxAttempts()).isEqualTo(3);
        assertThat(defaulted.maxAttempts()).isEqualTo(NewJob.DEFAULT_MAX_ATTEMPTS);
    }

    @Test
    void insertWithUsedIdempotencyKeyInsertsNothing() {
        Job first = repository.insertIfAbsent(UUID.randomUUID(), newJob("k1")).orElseThrow();

        Optional<Job> second = repository.insertIfAbsent(UUID.randomUUID(), newJob("k1"));

        assertThat(second).isEmpty();
        assertThat(countJobs()).isEqualTo(1);
        assertThat(repository.findByIdempotencyKey("k1")).map(Job::id).contains(first.id());
    }

    @Test
    void concurrentInsertsWithSameKeyCreateExactlyOneJob() throws Exception {
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Optional<Job>>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                start.await();
                return repository.insertIfAbsent(UUID.randomUUID(), newJob("same-key"));
            });
        }
        List<Optional<Job>> results = runConcurrently(tasks, start);

        assertThat(results.stream().filter(Optional::isPresent).count()).isEqualTo(1);
        assertThat(countJobs()).isEqualTo(1);
    }

    @Test
    void claimMarksOldestDueJobsRunningForWorker() {
        Job older = insertDue(Instant.now().minusSeconds(20));
        Job newer = insertDue(Instant.now().minusSeconds(10));
        insertDue(Instant.now().minusSeconds(5));

        List<Job> claimed = repository.claim("worker-1", NOOP, 2, LEASE);

        assertThat(claimed).extracting(Job::id).containsExactlyInAnyOrder(older.id(), newer.id());
        assertThat(claimed).allSatisfy(j -> {
            assertThat(j.status()).isEqualTo(JobStatus.RUNNING);
            assertThat(j.lockedBy()).isEqualTo("worker-1");
            assertThat(j.attempts()).isEqualTo(1);
            assertThat(j.startedAt()).isNotNull();
            // locked_until and started_at come from the same statement's now().
            assertThat(Duration.between(j.startedAt(), j.lockedUntil())).isEqualTo(LEASE);
        });
    }

    @Test
    void claimRejectsNonPositiveLease() {
        assertThatThrownBy(() -> repository.claim("worker-1", NOOP, 1, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void claimIgnoresJobsScheduledInTheFuture() {
        insertDue(Instant.now().plus(Duration.ofHours(1)));

        assertThat(repository.claim("worker-1", NOOP, 10, LEASE)).isEmpty();
    }

    @Test
    void claimedJobIsNotClaimedAgain() {
        insertDue(null);
        assertThat(repository.claim("worker-1", NOOP, 10, LEASE)).hasSize(1);

        assertThat(repository.claim("worker-2", NOOP, 10, LEASE)).isEmpty();
    }

    @Test
    void claimOnlyTakesRequestedTypes() {
        insertDue(null);
        Job other = repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob("unhandled", objectMapper.createObjectNode(), null, null)).orElseThrow();

        assertThat(repository.claim("worker-1", Set.of("noop", "email"), 10, LEASE)).extracting(Job::type).containsExactly("noop");
        assertThat(repository.claim("worker-1", Set.of(), 10, LEASE)).isEmpty();
        assertThat(repository.findById(other.id()).orElseThrow().status()).isEqualTo(JobStatus.QUEUED);
    }

    @Test
    void claimRejectsNonPositiveLimit() {
        assertThatThrownBy(() -> repository.claim("worker-1", NOOP, 0, LEASE)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void claimSkipsRowsLockedByAnotherTransactionWithoutBlocking() throws Exception {
        Job locked = insertDue(Instant.now().minusSeconds(10));
        Job free = insertDue(Instant.now().minusSeconds(5));

        try (Connection other = dataSource.getConnection()) {
            other.setAutoCommit(false);
            try (PreparedStatement ps = other.prepareStatement("SELECT id FROM jobs WHERE id = ? FOR UPDATE")) {
                ps.setObject(1, locked.id());
                ps.executeQuery();
            }

            List<Job> claimed = assertTimeoutPreemptively(Duration.ofSeconds(5), () -> repository.claim("worker-1", NOOP, 10, LEASE));

            assertThat(claimed).extracting(Job::id).containsExactly(free.id());
            other.rollback();
        }
        assertThat(repository.findById(locked.id()).orElseThrow().status()).isEqualTo(JobStatus.QUEUED);
    }

    @Test
    void concurrentClaimersReceiveDisjointJobsAndNoneAreMissed() throws Exception {
        int jobs = 300;
        for (int i = 0; i < jobs; i++) {
            insertDue(null);
        }
        int workers = 8;
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<List<UUID>>> tasks = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            String workerId = "worker-" + w;
            tasks.add(() -> {
                start.await();
                List<UUID> mine = new ArrayList<>();
                List<Job> batch;
                while (!(batch = repository.claim(workerId, NOOP, 5, LEASE)).isEmpty()) {
                    batch.forEach(j -> mine.add(j.id()));
                }
                return mine;
            });
        }
        List<List<UUID>> perWorker = runConcurrently(tasks, start);

        List<UUID> all = perWorker.stream().flatMap(List::stream).toList();
        Set<UUID> distinct = new HashSet<>(all);
        assertThat(all).hasSize(jobs);
        assertThat(distinct).hasSize(jobs);
        assertThat(jdbc.sql("SELECT count(*) FROM jobs WHERE status = 'RUNNING' AND attempts = 1")
                .query(Long.class).single()).isEqualTo(jobs);
    }

    @Test
    void markSucceededOnlyByOwnerWhileRunning() {
        insertDue(null);
        Job job = repository.claim("worker-1", NOOP, 1, LEASE).getFirst();

        assertThat(repository.markSucceeded(job.id(), "worker-2")).isFalse();
        assertThat(repository.findById(job.id()).orElseThrow().status()).isEqualTo(JobStatus.RUNNING);

        assertThat(repository.markSucceeded(job.id(), "worker-1")).isTrue();
        Job done = repository.findById(job.id()).orElseThrow();
        assertThat(done.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(done.lockedBy()).isNull();
        assertThat(done.lockedUntil()).isNull();
        assertThat(done.finishedAt()).isNotNull();

        assertThat(repository.markSucceeded(job.id(), "worker-1")).isFalse();
    }

    @Test
    void markSucceededOnQueuedJobDoesNothing() {
        Job job = insertDue(null);

        assertThat(repository.markSucceeded(job.id(), "worker-1")).isFalse();
        assertThat(repository.findById(job.id()).orElseThrow().status()).isEqualTo(JobStatus.QUEUED);
    }

    private Job insertDue(Instant runAt) {
        return repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob("noop", objectMapper.createObjectNode(), null, runAt)).orElseThrow();
    }

    private NewJob newJob(String key) {
        return new NewJob("noop", objectMapper.createObjectNode().put("n", 1), key, null);
    }

    private static <T> List<T> runConcurrently(List<Callable<T>> tasks, CountDownLatch start) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(tasks.size());
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (Callable<T> task : tasks) {
                futures.add(pool.submit(task));
            }
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> f : futures) {
                results.add(f.get(60, java.util.concurrent.TimeUnit.SECONDS));
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }
}
