package com.jobdock.worker;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param enabled start worker threads in this process
 * @param threads number of worker threads, each claiming independently
 * @param batchSize max jobs claimed per poll
 * @param idleDelay sleep between polls when nothing was claimed (or a poll failed)
 * @param shutdownTimeout how long stop() waits for in-flight jobs before interrupting threads
 */
@ConfigurationProperties("jobdock.worker")
public record WorkerProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("4") int threads,
        @DefaultValue("10") int batchSize,
        @DefaultValue("500ms") Duration idleDelay,
        @DefaultValue("30s") Duration shutdownTimeout) {

    public WorkerProperties {
        if (threads < 1) {
            throw new IllegalArgumentException("jobdock.worker.threads must be >= 1");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("jobdock.worker.batch-size must be >= 1");
        }
    }
}
