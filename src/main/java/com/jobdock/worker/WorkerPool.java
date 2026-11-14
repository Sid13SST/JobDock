package com.jobdock.worker;

import com.jobdock.job.JobRepository;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Runs {@code threads} independent poll loops, one heartbeat thread (extends the leases of all
 * held claims) and one lease-reaper thread. Each poll thread has its own worker id, so the
 * database (not this class) is what keeps two threads — or two processes — from taking the
 * same job, and what keeps concurrent reapers in different processes from double-requeuing.
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
    private final List<Worker> workers = new ArrayList<>();
    private final LeaseReaper reaper;
    private ScheduledExecutorService heartbeats;
    private ScheduledExecutorService reaperExecutor;
    private volatile boolean running;

    public WorkerPool(WorkerProperties props, JobRepository repository, HandlerRegistry handlers,
            BackoffPolicy backoff) {
        this.props = props;
        this.repository = repository;
        this.handlers = handlers;
        this.backoff = backoff;
        this.reaper = new LeaseReaper(repository);
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
            Worker worker = new Worker(instanceId + "-" + i, repository, handlers, backoff, props.batchSize(), props.leaseDuration());
            workers.add(worker);
            Thread t = new Thread(() -> loop(worker), threadNamePrefix() + i);
            threads.add(t);
        }
        long interval = props.heartbeatInterval().toMillis();
        heartbeats = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, threadNamePrefix() + "heartbeat"));
        heartbeats.scheduleWithFixedDelay(this::heartbeatAll, interval, interval, TimeUnit.MILLISECONDS);
        // Separate thread, so a slow reap never delays heartbeats.
        long reapEvery = props.reaperInterval().toMillis();
        reaperExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, threadNamePrefix() + "reaper"));
        reaperExecutor.scheduleWithFixedDelay(this::reap, reapEvery, reapEvery, TimeUnit.MILLISECONDS);
        threads.forEach(Thread::start);
        log.info("Started {} worker threads (instance {}, types {}, lease {}, heartbeat every {})", props.threads(),
                instanceId, handlers.types(), props.leaseDuration(), props.heartbeatInterval());
    }

    /** Runs on the reaper thread; must never throw, or the scheduler would stop running it. */
    private void reap() {
        try {
            reaper.reapOnce();
        } catch (RuntimeException e) {
            log.error("Lease reaper run failed; will retry in {}", props.reaperInterval(), e);
        }
    }

    /** Runs on the heartbeat thread; must never throw, or the scheduler would stop running it. */
    private void heartbeatAll() {
        for (Worker worker : workers) {
            try {
                worker.heartbeat();
            } catch (RuntimeException e) {
                log.error("Heartbeat for worker {} failed; its leases may expire", worker.id(), e);
            }
        }
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
        // Heartbeats keep running while in-flight jobs finish above, then stop.
        heartbeats.shutdownNow();
        reaperExecutor.shutdownNow();
        workers.clear();
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
