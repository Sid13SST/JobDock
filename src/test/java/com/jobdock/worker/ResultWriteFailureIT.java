package com.jobdock.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.PostgresIntegrationTest;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.NewJob;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessResourceFailureException;

/**
 * If writing a job's result fails, the rest of the claimed batch still runs, and the job whose
 * write failed is recovered through lease expiry and run again. Real Postgres; the write failure
 * is injected once for one job.
 */
class ResultWriteFailureIT extends PostgresIntegrationTest {

    @Autowired
    JobRepository repository;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void batchContinuesAndTheUnrecordedJobIsRecoveredViaLeaseExpiry() {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ids.add(repository.insertIfAbsent(UUID.randomUUID(),
                    new NewJob("work", JsonNodeFactory.instance.objectNode(), null, null)).orElseThrow().id());
        }
        UUID unlucky = ids.get(1);
        AtomicBoolean injected = new AtomicBoolean();
        JobRepository flaky = new JobRepository(jdbc, objectMapper) {
            @Override
            public boolean markSucceeded(UUID id, String workerId, int attempt) {
                if (id.equals(unlucky) && injected.compareAndSet(false, true)) {
                    throw new DataAccessResourceFailureException("injected: connection lost while recording");
                }
                return super.markSucceeded(id, workerId, attempt);
            }
        };
        Map<UUID, AtomicInteger> executions = new ConcurrentHashMap<>();
        JobHandler handler = new JobHandler() {
            @Override
            public String type() {
                return "work";
            }

            @Override
            public void handle(Job job) {
                executions.computeIfAbsent(job.id(), id -> new AtomicInteger()).incrementAndGet();
            }
        };
        WorkerPool pool = new WorkerPool(
                new WorkerProperties(true, 1, 5, Duration.ofMillis(20), Duration.ofSeconds(5))
                        .withLease(Duration.ofMillis(600)).withReaperInterval(Duration.ofMillis(50)),
                flaky, new HandlerRegistry(List.of(handler)),
                new BackoffPolicy(Duration.ofMillis(10), Duration.ofMillis(10), () -> 0.0));

        pool.start();
        try {
            await().atMost(Duration.ofSeconds(30)).until(() -> count("status = 'SUCCEEDED'") == 5);
        } finally {
            pool.stop();
        }

        assertThat(injected).isTrue();
        assertThat(count("status = 'SUCCEEDED' AND attempts = 1")).as("rest of the batch ran once").isEqualTo(4);
        Job recovered = repository.findById(unlucky).orElseThrow();
        assertThat(recovered.attempts()).as("recovered and run again").isEqualTo(2);
        assertThat(recovered.lastError()).startsWith("Lease expired: worker ");
        assertThat(executions.get(unlucky).get()).isEqualTo(2);
        ids.stream().filter(id -> !id.equals(unlucky))
                .forEach(id -> assertThat(executions.get(id).get()).isEqualTo(1));
    }

    private long count(String where) {
        return jdbc.sql("SELECT count(*) FROM jobs WHERE " + where).query(Long.class).single();
    }
}
