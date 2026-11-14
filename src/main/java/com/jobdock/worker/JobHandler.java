package com.jobdock.worker;

import com.jobdock.job.Job;

/**
 * Executes jobs of one {@link #type()}. Register an implementation as a Spring bean.
 *
 * <p>Delivery is at-least-once: the same job may be handled more than once (for example if the
 * worker dies after the handler's side effects but before completion is recorded), so
 * implementations must be idempotent, e.g. by using {@link Job#id()} as a deduplication key.
 */
public interface JobHandler {

    String type();

    /** Returning normally marks the job SUCCEEDED. Throwing means the attempt failed. */
    void handle(Job job) throws Exception;
}
