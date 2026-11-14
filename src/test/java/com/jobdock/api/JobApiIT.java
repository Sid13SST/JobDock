package com.jobdock.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.jobdock.PostgresIntegrationTest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/** Full HTTP stack against real Postgres. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class JobApiIT extends PostgresIntegrationTest {

    @Autowired
    TestRestTemplate http;

    @Test
    void enqueueThenGet() {
        ResponseEntity<JsonNode> created = post(null, "{\"type\":\"email\",\"payload\":{\"to\":\"a@example.com\"}}");

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String id = created.getBody().get("id").asText();
        assertThat(created.getHeaders().getLocation()).hasToString("/jobs/" + id);

        ResponseEntity<JsonNode> fetched = http.getForEntity("/jobs/" + id, JsonNode.class);
        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(fetched.getBody().get("status").asText()).isEqualTo("QUEUED");
        assertThat(fetched.getBody().get("payload").get("to").asText()).isEqualTo("a@example.com");
        assertThat(fetched.getBody().get("attempts").asInt()).isZero();
    }

    @Test
    void runAtIsStored() {
        Instant runAt = Instant.now().plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS);
        ResponseEntity<JsonNode> created =
                post(null, "{\"type\":\"email\",\"payload\":{},\"runAt\":\"" + runAt + "\"}");

        assertThat(Instant.parse(created.getBody().get("runAt").asText())).isEqualTo(runAt);
    }

    @Test
    void replayWithSameKeyReturnsOriginalJob() {
        ResponseEntity<JsonNode> first = post("order-42", "{\"type\":\"email\",\"payload\":{\"a\":1,\"b\":2}}");
        ResponseEntity<JsonNode> second = post("order-42", "{\"type\":\"email\",\"payload\":{\"b\":2,\"a\":1}}");

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(second.getBody().get("id")).isEqualTo(first.getBody().get("id"));
        assertThat(countJobs()).isEqualTo(1);
    }

    @Test
    void sameKeyDifferentPayloadReturns409AndCreatesNothing() {
        post("order-43", "{\"type\":\"email\",\"payload\":{\"a\":1}}");
        ResponseEntity<JsonNode> conflict = post("order-43", "{\"type\":\"email\",\"payload\":{\"a\":2}}");

        assertThat(conflict.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(countJobs()).isEqualTo(1);
    }

    @Test
    void requestsWithoutKeyAreNotDeduplicated() {
        post(null, "{\"type\":\"email\",\"payload\":{}}");
        post(null, "{\"type\":\"email\",\"payload\":{}}");

        assertThat(countJobs()).isEqualTo(2);
    }

    @Test
    void concurrentRequestsWithSameKeyCreateOneJob() throws Exception {
        int clients = 12;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try {
            List<Future<ResponseEntity<JsonNode>>> futures = new ArrayList<>();
            for (int i = 0; i < clients; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return post("race-key", "{\"type\":\"email\",\"payload\":{\"x\":1}}");
                }));
            }
            start.countDown();
            List<ResponseEntity<JsonNode>> responses = new ArrayList<>();
            for (Future<ResponseEntity<JsonNode>> f : futures) {
                responses.add(f.get(30, TimeUnit.SECONDS));
            }

            assertThat(responses).filteredOn(r -> r.getStatusCode() == HttpStatus.CREATED).hasSize(1);
            assertThat(responses).filteredOn(r -> r.getStatusCode() == HttpStatus.OK).hasSize(clients - 1);
            assertThat(responses).extracting(r -> r.getBody().get("id").asText()).containsOnly(
                    responses.getFirst().getBody().get("id").asText());
            assertThat(countJobs()).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void unknownJobReturns404() {
        ResponseEntity<String> r = http.getForEntity("/jobs/00000000-0000-0000-0000-000000000000", String.class);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private ResponseEntity<JsonNode> post(String key, String json) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (key != null) {
            headers.set("Idempotency-Key", key);
        }
        return http.postForEntity("/jobs", new HttpEntity<>(json, headers), JsonNode.class);
    }
}
