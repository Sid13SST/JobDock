package com.jobdock.worker;

import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import java.util.List;
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
    private final int batchSize;

    public Worker(String id, JobRepository repository, HandlerRegistry handlers, int batchSize) {
        this.id = id;
        this.repository = repository;
        this.handlers = handlers;
        this.batchSize = batchSize;
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
        List<Job> claimed = repository.claim(id, handlers.types(), batchSize);
        for (Job job : claimed) {
            execute(job);
        }
        return claimed.size();
    }

    private void execute(Job job) {
        JobHandler handler = handlers.find(job.type()).orElse(null);
        if (handler == null) {
            // Cannot normally happen: we only claim registered types.
            log.error("No handler for job {} of type '{}'; leaving it RUNNING", job.id(), job.type());
            return;
        }
        try {
            handler.handle(job);
        } catch (Exception e) {
            // Batch 1: failure path not implemented yet. The job stays RUNNING; retry/backoff and
            // lease-expiry recovery are planned (docs/TASKS.md, batches 2 and 3).
            log.error("Job {} (type '{}', attempt {}) failed; failure handling not implemented yet",
                    job.id(), job.type(), job.attempts(), e);
            return;
        }
        if (!repository.markSucceeded(job.id(), id)) {
            log.warn("Job {} finished but worker {} no longer owns it; success not recorded", job.id(), id);
        }
    }
}
