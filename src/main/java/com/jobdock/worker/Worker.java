package com.jobdock.worker;

import com.jobdock.job.FailureOutcome;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.JobStatus;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One claim-execute-complete loop iteration. Not thread-safe; each thread owns its Worker and
 * a unique worker id. Handlers run with no database transaction open.
 */
public class Worker {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private final String id;
    private final JobRepository repository;
    private final HandlerRegistry handlers;
    private final BackoffPolicy backoff;
    private final int batchSize;
    private final Duration lease;

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
        for (Job job : claimed) {
            execute(job);
        }
        return claimed.size();
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
        } catch (Exception e) {
            fail(job, describe(e), e);
            return;
        }
        if (!repository.markSucceeded(job.id(), id)) {
            log.warn("Job {} finished but worker {} no longer owns it; success not recorded", job.id(), id);
        }
    }

    private void fail(Job job, String error, Exception cause) {
        Duration delay = backoff.delay(job.attempts());
        Optional<FailureOutcome> outcome = repository.markFailed(job.id(), id, error, delay);
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
