package com.jobdock.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.PostgresIntegrationTest;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.JobStatus;
import com.jobdock.job.NewJob;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Real crash: a worker runs in a separate JVM ({@link CrashableWorkerMain}), claims a job and is
 * killed with {@code kill -9} while its handler runs. Nothing in the dead process gets to clean
 * up. The job must be recovered through lease expiry and completed by a worker in this JVM.
 */
class CrashRecoveryIT extends PostgresIntegrationTest {

    private static final Duration CHILD_LEASE = Duration.ofMillis(1500);

    @Autowired
    JobRepository repository;

    @Autowired
    PostgreSQLContainer<?> postgres;

    @Test
    void jobOfAWorkerKilledWithSigkillMidJobIsRecoveredAndCompletedByAnotherWorker() throws Exception {
        UUID id = repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob(CrashableWorkerMain.TYPE, JsonNodeFactory.instance.objectNode(), null, null, 3))
                .orElseThrow().id();

        Process child = startChildWorker();
        try {
            String started = awaitLine(child, "STARTED ", Duration.ofSeconds(60));
            assertThat(started).isEqualTo("STARTED " + id + " 1");

            Job running = load(id);
            assertThat(running.status()).isEqualTo(JobStatus.RUNNING);
            assertThat(running.lockedBy()).as("claimed by the child process").contains("-" + child.pid() + "-");
            String childWorker = running.lockedBy();

            // The child heartbeats: after more than one lease period its lease is still ahead of now().
            Instant firstExpiry = running.startedAt().plus(CHILD_LEASE);
            await().atMost(Duration.ofSeconds(15)).until(() -> dbNow().isAfter(firstExpiry.plusMillis(500)));
            assertThat(load(id).lockedUntil()).as("child heartbeat extended the lease").isAfter(dbNow());

            Process kill = new ProcessBuilder("kill", "-9", String.valueOf(child.pid())).inheritIO().start();
            assertThat(kill.waitFor(10, TimeUnit.SECONDS)).isTrue();
            assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
            assertThat(child.exitValue()).as("killed by SIGKILL (128 + 9)").isEqualTo(137);

            // Recovery: a normal pool in this JVM, with its reaper, picks the job up after the lease runs out.
            AtomicInteger executionsHere = new AtomicInteger();
            WorkerPool survivor = new WorkerPool(
                    new WorkerProperties(true, 1, 1, Duration.ofMillis(20), Duration.ofSeconds(5))
                            .withReaperInterval(Duration.ofMillis(100)),
                    repository, new HandlerRegistry(List.of(succeeding(executionsHere))),
                    BackoffPolicy.withRandomJitter(Duration.ofSeconds(1), Duration.ofSeconds(1)));
            survivor.start();
            try {
                await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                        .until(() -> load(id).status() == JobStatus.SUCCEEDED);
            } finally {
                survivor.stop();
            }

            Job done = load(id);
            assertThat(done.attempts()).as("attempt 1 died with the child, attempt 2 succeeded").isEqualTo(2);
            assertThat(done.lastError())
                    .isEqualTo("Lease expired: worker " + childWorker + " stopped heartbeating during attempt 1");
            assertThat(executionsHere.get()).isEqualTo(1);
        } finally {
            child.destroyForcibly();
        }
    }

    private Process startChildWorker() throws Exception {
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Path log = Path.of("target", "crash-child.log");
        ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", System.getProperty("java.class.path"),
                CrashableWorkerMain.class.getName(), postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword(), String.valueOf(CHILD_LEASE.toMillis()))
                .redirectErrorStream(true);
        Process child = pb.start();
        Files.deleteIfExists(log);
        BlockingQueue<String> lines = new LinkedBlockingQueue<>();
        Thread reader = new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    lines.add(line);
                    Files.writeString(log, line + System.lineSeparator(), StandardCharsets.UTF_8,
                            java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
                }
            } catch (Exception ignored) {
                // child killed
            }
        }, "crash-child-reader");
        reader.setDaemon(true);
        reader.start();
        childLines = lines;
        return child;
    }

    private BlockingQueue<String> childLines;

    private String awaitLine(Process child, String prefix, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            String line = childLines.poll(200, TimeUnit.MILLISECONDS);
            if (line != null && line.startsWith(prefix)) {
                return line;
            }
            if (line == null && !child.isAlive()) {
                throw new AssertionError("child worker exited early; see target/crash-child.log");
            }
        }
        throw new AssertionError("no '" + prefix + "' line from child within " + timeout + "; see target/crash-child.log");
    }

    private static JobHandler succeeding(AtomicInteger executions) {
        return new JobHandler() {
            @Override
            public String type() {
                return CrashableWorkerMain.TYPE;
            }

            @Override
            public void handle(Job job) {
                executions.incrementAndGet();
            }
        };
    }

    private Instant dbNow() {
        return jdbc.sql("SELECT now()").query(java.sql.Timestamp.class).single().toInstant();
    }

    private Job load(UUID id) {
        return repository.findById(id).orElseThrow();
    }
}
