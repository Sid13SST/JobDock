package com.jobdock.worker;

import com.jobdock.job.FailureOutcome;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.JobStatus;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One claim-execute-complete loop iteration. {@link #pollOnce()} is called by one thread only
 * (each thread owns its Worker and a unique worker id); {@link #heartbeat()} may be called
 * concurrently from a heartbeat thread. Handlers run with no database transaction open.
 */
public class Worker {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private final String id;
    private final JobRepository repository;
    private final HandlerRegistry handlers;
    private final BackoffPolicy backoff;
    private final int batchSize;
    private final Duration lease;
    /** Claims this worker holds and has not finished: job id -> attempt. Heartbeats cover all of them. */
    private final Map<UUID, Integer> held = new ConcurrentHashMap<>();
    /** Held claims the heartbeat found were taken away (lease expired and reclaimed or reaped). */
    private final Set<UUID> lost = ConcurrentHashMap.newKeySet();

    public Worker(String id, JobRepository repository, HandlerRegistry handlers, BackoffPolicy backoff,
            int batchSize, Duration lease) {
        this.id = id;
        this.repository = repository;
        this.handlers = handlers;
        this.backoff = backoff;
        this.batchSize = batchSize;
        this.lease = lease;
    }

    public String id() {
        return id;
    }

    /**
     * Claims up to {@code batchSize} jobs and runs each one.
     *
     * @return the number of jobs claimed (0 means the queue had nothing due for us)
     */
    public int pollOnce() {
        List<Job> claimed = repository.claim(id, handlers.types(), batchSize, lease);
        claimed.forEach(job -> held.put(job.id(), job.attempts()));
        try {
            for (Job job : claimed) {
                if (Thread.currentThread().isInterrupted()) {
                    // Shutdown timed out. Unstarted jobs stop being heartbeated (finally below) and the
                    // reaper requeues them once their lease expires.
                    log.warn("Worker {} interrupted; not starting the remaining jobs of its batch", id);
                    break;
                }
                if (lost.remove(job.id())) {
                    log.warn("Lease on job {} (attempt {}) was lost before it started; skipping it", job.id(),
                            job.attempts());
                    held.remove(job.id());
                    continue;
                }
                try {
                    execute(job);
                } catch (RuntimeException e) {
                    // Recording the result failed (e.g. database unavailable). Do not strand the rest of
                    // the batch: this job is dropped from the heartbeat set below, so its lease expires
                    // and the reaper requeues it; the handler will then run again (at-least-once).
                    log.error("Worker {} could not record the result of job {} (attempt {}); it will be "
                            + "recovered after its lease expires", id, job.id(), job.attempts(), e);
                } finally {
                    held.remove(job.id());
                    lost.remove(job.id());
                }
            }
        } finally {
            claimed.forEach(job -> held.remove(job.id()));
        }
        return claimed.size();
    }

    /**
     * Extends the lease of every claim this worker holds. A claim that is no longer ours is
     * marked lost: if its job has not started it will be skipped; if it is running, its result
     * will be rejected by fencing when it finishes.
     */
    public void heartbeat() {
        if (held.isEmpty()) {
            return;
        }
        Map<UUID, Integer> snapshot = Map.copyOf(held);
        Set<UUID> extended = repository.extendLeases(id, snapshot, lease);
        snapshot.forEach((jobId, attempt) -> {
            if (!extended.contains(jobId) && attempt.equals(held.get(jobId))) {
                lost.add(jobId);
                log.warn("Worker {} lost the lease on job {} (attempt {})", id, jobId, attempt);
            }
        });
    }

    /** Claims currently held (for tests and diagnostics). */
    Map<UUID, Integer> heldClaims() {
        return Map.copyOf(held);
    }

    private void execute(Job job) {
        JobHandler handler = handlers.find(job.type()).orElse(null);
        if (handler == null) {
            // Cannot normally happen: we only claim registered types. Record it rather than strand the job.
            fail(job, "No handler registered for type '" + job.type() + "'", null);
            return;
        }
        try {
            handler.handle(job);
        } catch (Throwable t) {
            // Any Throwable counts as a failed attempt, including Errors such as StackOverflowError or
            // AssertionError. After an OutOfMemoryError the JVM may be unusable; if recording fails,
            // lease expiry recovers the job.
            fail(job, describe(t), t);
            return;
        }
        if (!repository.markSucceeded(job.id(), id, job.attempts())) {
            log.warn("Job {} finished but worker {} no longer owns it; success not recorded", job.id(), id);
        }
    }

    private void fail(Job job, String error, Throwable cause) {
        Duration delay = backoff.delay(job.attempts());
        Optional<FailureOutcome> outcome = repository.markFailed(job.id(), id, job.attempts(), error, delay);
        if (outcome.isEmpty()) {
            log.warn("Job {} failed but worker {} no longer owns it; failure not recorded", job.id(), id);
        } else if (outcome.get().status() == JobStatus.DEAD) {
            log.error("Job {} (type '{}') failed attempt {}/{} and is DEAD: {}", job.id(), job.type(),
                    job.attempts(), job.maxAttempts(), error, cause);
        } else {
            log.warn("Job {} (type '{}') failed attempt {}/{}, retry at {}: {}", job.id(), job.type(),
                    job.attempts(), job.maxAttempts(), outcome.get().runAt(), error);
        }
    }

    static String describe(Throwable e) {
        String message = e.getMessage();
        return message == null ? e.getClass().getName() : e.getClass().getName() + ": " + message;
    }
}
