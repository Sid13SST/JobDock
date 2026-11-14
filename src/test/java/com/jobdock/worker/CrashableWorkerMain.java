package com.jobdock.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Test helper run in a separate JVM by {@link CrashRecoveryIT}: a real WorkerPool (with
 * heartbeat) whose handler for type {@value #TYPE} announces the job on stdout and then hangs,
 * so the test can kill this process with SIGKILL mid-job. Not a test itself.
 *
 * <p>Args: jdbcUrl user password leaseMillis
 */
public final class CrashableWorkerMain {

    static final String TYPE = "crashable";

    private CrashableWorkerMain() {}

    public static void main(String[] args) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(args[0]);
        config.setUsername(args[1]);
        config.setPassword(args[2]);
        config.setMaximumPoolSize(4);
        HikariDataSource dataSource = new HikariDataSource(config);
        JobRepository repository = new JobRepository(JdbcClient.create(dataSource), new ObjectMapper());
        Duration lease = Duration.ofMillis(Long.parseLong(args[3]));

        JobHandler hang = new JobHandler() {
            @Override
            public String type() {
                return TYPE;
            }

            @Override
            public void handle(Job job) throws InterruptedException {
                System.out.println("STARTED " + job.id() + " " + job.attempts());
                System.out.flush();
                Thread.sleep(Long.MAX_VALUE);
            }
        };
        WorkerPool pool = new WorkerPool(
                new WorkerProperties(true, 1, 1, Duration.ofMillis(50), Duration.ofSeconds(1)).withLease(lease),
                repository, new HandlerRegistry(List.of(hang)),
                BackoffPolicy.withRandomJitter(Duration.ofSeconds(1), Duration.ofSeconds(1)));
        pool.start();
        System.out.println("READY pid=" + ProcessHandle.current().pid());
        System.out.flush();
    }
}
