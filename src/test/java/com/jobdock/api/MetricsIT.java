package com.jobdock.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.PostgresIntegrationTest;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.NewJob;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** {@code GET /metrics} over HTTP against real Postgres, with jobs driven into every state. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class MetricsIT extends PostgresIntegrationTest {

    private static final Set<String> NOOP = Set.of("noop");
    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    TestRestTemplate http;

    @Autowired
    JobRepository repository;

    @Test
    void emptyQueue() {
        JsonNode m = metrics();

        for (String status : new String[] {"QUEUED", "RUNNING", "SUCCEEDED", "FAILED_RETRY", "DEAD"}) {
            assertThat(m.get("countsByStatus").get(status).asLong()).as(status).isZero();
        }
        assertThat(m.get("oldestDueAgeSeconds").isNull()).isTrue();
        assertThat(m.get("retriesTotal").asLong()).isZero();
        assertThat(m.get("deadLettered").asLong()).isZero();
        assertThat(m.get("leaseExpirationsTotal").asLong()).isZero();
    }

    @Test
    void countsAgeRetriesDeadLettersAndLeaseExpirations() {
        // 2 QUEUED, the older one due for ~90s; 1 QUEUED scheduled in the future (not "due").
        UUID oldQueued = enqueue(5);
        jdbc.sql("UPDATE jobs SET run_at = now() - interval '90 seconds' WHERE id = ?").param(oldQueued).update();
        enqueue(5);
        repository.insertIfAbsent(UUID.randomUUID(), new NewJob("noop", JsonNodeFactory.instance.objectNode(), null,
                Instant.now().plus(Duration.ofHours(1))));
        // These three are claimed in one go below, while every job above is still QUEUED.
        // They go to SUCCEEDED, then via a failure to FAILED_RETRY, then via 2 lease expiries to DEAD.
        jdbc.sql("UPDATE jobs SET run_at = run_at + interval '2 hours'").update(); // park the jobs above
        UUID succeeded = enqueue(5);
        UUID retrying = enqueue(5);
        UUID dead = enqueue(2);
        for (Job j : repository.claim("w1", NOOP, 3, LEASE)) {
            if (j.id().equals(succeeded)) {
                repository.markSucceeded(j.id(), "w1", j.attempts());
            } else if (j.id().equals(retrying)) {
                repository.markFailed(j.id(), "w1", j.attempts(), "boom", Duration.ofHours(1));
            }
        }
        // dead: attempt 1 lease expires -> FAILED_RETRY; attempt 2 lease expires -> DEAD.
        expireAndReap(dead);
        jdbc.sql("UPDATE jobs SET run_at = now() WHERE id = ?").param(dead).update();
        assertThat(repository.claim("w2", NOOP, 1, LEASE)).extracting(Job::id).containsExactly(dead);
        expireAndReap(dead);
        // 1 RUNNING
        UUID running = enqueue(5);
        repository.claim("w3", NOOP, 1, LEASE);
        // Unpark the original QUEUED jobs.
        jdbc.sql("UPDATE jobs SET run_at = run_at - interval '2 hours' WHERE status = 'QUEUED'").update();

        JsonNode m = metrics();

        JsonNode counts = m.get("countsByStatus");
        assertThat(counts.get("QUEUED").asLong()).isEqualTo(3);
        assertThat(counts.get("RUNNING").asLong()).isEqualTo(1);
        assertThat(counts.get("SUCCEEDED").asLong()).isEqualTo(1);
        assertThat(counts.get("FAILED_RETRY").asLong()).isEqualTo(1);
        assertThat(counts.get("DEAD").asLong()).isEqualTo(1);
        assertThat(m.get("deadLettered").asLong()).isEqualTo(1);
        assertThat(m.get("retriesTotal").asLong()).as("only the dead job had a second attempt").isEqualTo(1);
        assertThat(m.get("leaseExpirationsTotal").asLong()).isEqualTo(2);
        // The oldest due job has been due for ~90s (the retry and the future job are not due yet).
        assertThat(m.get("oldestDueAgeSeconds").asDouble()).isBetween(90.0, 120.0);
        assertThat(running).isNotNull();
    }

    private void expireAndReap(UUID id) {
        jdbc.sql("UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id = ?").param(id).update();
        assertThat(repository.reapExpiredLeases(100)).hasSize(1);
    }

    private UUID enqueue(int maxAttempts) {
        return repository.insertIfAbsent(UUID.randomUUID(),
                new NewJob("noop", JsonNodeFactory.instance.objectNode(), null, null, maxAttempts)).orElseThrow().id();
    }

    private JsonNode metrics() {
        ResponseEntity<JsonNode> r = http.getForEntity("/metrics", JsonNode.class);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.OK);
        return r.getBody();
    }
}
