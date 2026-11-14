package com.jobdock.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.PostgresIntegrationTest;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.JobStatus;
import com.jobdock.job.NewJob;
import com.jobdock.job.ReapedJob;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Lease-expiry recovery, against real Postgres. */
class LeaseReaperIT extends PostgresIntegrationTest {

    private static final Set<String> NOOP = Set.of("noop");
    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    JobRepository repository;

    @Test
    void expiredLeaseWithAttemptsLeftIsRequeuedImmediately() {
        Job job = claimNew("w1", 3);
        expireLease(job.id());

        List<ReapedJob> reaped = repository.reapExpiredLeases(100);

        assertThat(reaped).containsExactly(new ReapedJob(job.id(), JobStatus.FAILED_RETRY, "w1", 1));
        Job requeued = load(job.id());
        assertThat(requeued.status()).isEqualTo(JobStatus.FAILED_RETRY);
        assertThat(requeued.lockedBy()).isNull();
        assertThat(requeued.lockedUntil()).isNull();
        assertThat(requeued.attempts()).as("the abandoned attempt counts").isEqualTo(1);
        assertThat(requeued.lastError()).isEqualTo("Lease expired: worker w1 stopped heartbeating during attempt 1");

        Job retried = repository.claim("w2", NOOP, 10, LEASE).getFirst();
        assertThat(retried.id()).isEqualTo(job.id());
        assertThat(retried.attempts()).isEqualTo(2);
    }

    @Test
    void expiredLeaseOnLastAttemptIsDeadLettered() {
        Job job = claimNew("w1", 1);
        expireLease(job.id());

        assertThat(repository.reapExpiredLeases(100)).extracting(ReapedJob::status).containsExactly(JobStatus.DEAD);

        Job dead = load(job.id());
        assertThat(dead.status()).isEqualTo(JobStatus.DEAD);
        assertThat(dead.finishedAt()).isNotNull();
        assertThat(dead.lastError()).startsWith("Lease expired: worker w1");
        assertThat(repository.claim("w2", NOOP, 10, LEASE)).isEmpty();
    }

    @Test
    void liveLeasesAndNonRunningJobsAreNotTouched() {
        Job live = claimNew("w1", 3);
        repository.insertIfAbsent(UUID.randomUUID(), new NewJob("noop", JsonNodeFactory.instance.objectNode(), null, null));

        assertThat(repository.reapExpiredLeases(100)).isEmpty();
        assertThat(load(live.id()).status()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void concurrentReapersRequeueEachExpiredJobExactlyOnce() throws Exception {
        int jobs = 300;
        for (int i = 0; i < jobs; i++) {
            repository.insertIfAbsent(UUID.randomUUID(), new NewJob("noop", JsonNodeFactory.instance.objectNode(), null, null));
        }
        assertThat(repository.claim("w1", NOOP, jobs, LEASE)).hasSize(jobs);
        jdbc.sql("UPDATE jobs SET locked_until = now() - interval '1 second'").update();

        int reapers = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(reapers);
        List<UUID> reapedIds = new ArrayList<>();
        try {
            List<Future<List<UUID>>> futures = new ArrayList<>();
            for (int r = 0; r < reapers; r++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    List<UUID> mine = new ArrayList<>();
                    List<ReapedJob> batch;
                    // Small batches so reapers interleave on the same rows.
                    while (!(batch = repository.reapExpiredLeases(7)).isEmpty()) {
                        batch.forEach(j -> mine.add(j.id()));
                    }
                    return mine;
                }));
            }
            start.countDown();
            for (Future<List<UUID>> f : futures) {
                reapedIds.addAll(f.get(60, TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(reapedIds).as("reaped in total").hasSize(jobs);
        assertThat(new HashSet<>(reapedIds)).as("distinct").hasSize(jobs);
        assertThat(jdbc.sql("SELECT count(*) FROM jobs WHERE status = 'FAILED_RETRY' AND attempts = 1 AND locked_by IS NULL")
                .query(Long.class).single()).isEqualTo(jobs);
    }

    @Test
    void leaseReaperDrainsMoreThanOneBatch() {
        int jobs = LeaseReaper.BATCH + 30;
        for (int i = 0; i < jobs; i++) {
            repository.insertIfAbsent(UUID.randomUUID(), new NewJob("noop", JsonNodeFactory.instance.objectNode(), null, null));
        }
        repository.claim("w1", NOOP, jobs, LEASE);
        jdbc.sql("UPDATE jobs SET locked_until = now() - interval '1 second'").update();

        assertThat(new LeaseReaper(repository).reapOnce()).isEqualTo(jobs);
    }

    private Job claimNew(String worker, int maxAttempts) {
        repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob("noop", JsonNodeFactory.instance.objectNode(), null, null, maxAttempts));
        return repository.claim(worker, NOOP, 1, LEASE).getFirst();
    }

    /** Test-only time travel instead of waiting 30s for the lease to run out. */
    private void expireLease(UUID id) {
        jdbc.sql("UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id = ?").param(id).update();
    }

    private Job load(UUID id) {
        return repository.findById(id).orElseThrow();
    }
}
