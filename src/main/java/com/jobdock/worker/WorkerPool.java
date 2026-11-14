package com.jobdock.worker;

import com.jobdock.job.JobRepository;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Runs {@code threads} independent poll loops. Each thread has its own worker id, so the
 * database (not this class) is what keeps two threads — or two processes — from taking the
 * same job.
 */
@Component
@ConditionalOnProperty(name = "jobdock.worker.enabled", havingValue = "true", matchIfMissing = true)
public class WorkerPool implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    private final WorkerProperties props;
    private final JobRepository repository;
    private final HandlerRegistry handlers;
    private final BackoffPolicy backoff;
    private final String instanceId;
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running;

    public WorkerPool(WorkerProperties props, JobRepository repository, HandlerRegistry handlers,
            BackoffPolicy backoff) {
        this.props = props;
        this.repository = repository;
        this.handlers = handlers;
        this.backoff = backoff;
        this.instanceId = hostname() + "-" + ProcessHandle.current().pid() + "-"
                + UUID.randomUUID().toString().substring(0, 8);
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        for (int i = 0; i < props.threads(); i++) {
            Worker worker = new Worker(instanceId + "-" + i, repository, handlers, backoff, props.batchSize());
            Thread t = new Thread(() -> loop(worker), threadNamePrefix() + i);
            threads.add(t);
            t.start();
        }
        log.info("Started {} worker threads (instance {}, types {})", props.threads(), instanceId, handlers.types());
    }

    private void loop(Worker worker) {
        while (running) {
            int claimed;
            try {
                claimed = worker.pollOnce();
            } catch (RuntimeException e) {
                log.error("Worker {} poll failed; retrying after {}", worker.id(), props.idleDelay(), e);
                claimed = 0;
            }
            if (claimed == 0 && !sleep()) {
                return;
            }
        }
    }

    private boolean sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(props.idleDelay().toMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** Stops claiming new jobs and waits up to shutdownTimeout for in-flight jobs to finish. */
    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        long deadline = System.nanoTime() + props.shutdownTimeout().toNanos();
        for (Thread t : threads) {
            try {
                t.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (t.isAlive()) {
                log.warn("Worker thread {} did not finish within {}; interrupting", t.getName(), props.shutdownTimeout());
                t.interrupt();
            }
        }
        threads.clear();
        log.info("Worker pool stopped");
    }

    /** Worker threads of this pool are named with this prefix followed by their index. */
    String threadNamePrefix() {
        return "jobdock-worker-" + instanceId + "-";
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }
}
