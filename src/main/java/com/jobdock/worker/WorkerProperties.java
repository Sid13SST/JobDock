package com.jobdock.worker;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param enabled start worker threads in this process
 * @param threads number of worker threads, each claiming independently
 * @param batchSize max jobs claimed per poll
 * @param idleDelay sleep between polls when nothing was claimed (or a poll failed)
 * @param shutdownTimeout how long stop() waits for in-flight jobs before interrupting threads
 * @param leaseDuration how long a claim is valid without a heartbeat (visibility timeout)
 */
@ConfigurationProperties("jobdock.worker")
public record WorkerProperties(
        boolean enabled,
        int threads,
        int batchSize,
        Duration idleDelay,
        Duration shutdownTimeout,
        Duration leaseDuration) {

    public static final Duration DEFAULT_LEASE = Duration.ofSeconds(30);

    @ConstructorBinding
    public WorkerProperties(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("4") int threads,
            @DefaultValue("10") int batchSize,
            @DefaultValue("500ms") Duration idleDelay,
            @DefaultValue("30s") Duration shutdownTimeout,
            @DefaultValue("30s") Duration leaseDuration) {
        if (threads < 1) {
            throw new IllegalArgumentException("jobdock.worker.threads must be >= 1");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("jobdock.worker.batch-size must be >= 1");
        }
        if (leaseDuration.isNegative() || leaseDuration.isZero()) {
            throw new IllegalArgumentException("jobdock.worker.lease-duration must be positive");
        }
        this.enabled = enabled;
        this.threads = threads;
        this.batchSize = batchSize;
        this.idleDelay = idleDelay;
        this.shutdownTimeout = shutdownTimeout;
        this.leaseDuration = leaseDuration;
    }

    /** With the default lease (convenient for tests). */
    public WorkerProperties(boolean enabled, int threads, int batchSize, Duration idleDelay, Duration shutdownTimeout) {
        this(enabled, threads, batchSize, idleDelay, shutdownTimeout, DEFAULT_LEASE);
    }

    public WorkerProperties withLease(Duration lease) {
        return new WorkerProperties(enabled, threads, batchSize, idleDelay, shutdownTimeout, lease);
    }
}
