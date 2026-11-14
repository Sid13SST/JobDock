package com.jobdock.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.PostgresIntegrationTest;
import com.jobdock.job.Job;
import com.jobdock.job.JobRepository;
import com.jobdock.job.NewJob;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class WorkerPoolIT extends PostgresIntegrationTest {

    @Autowired
    JobRepository repository;

    @Test
    void poolRunsQueuedJobsToSuccessAndStopsCleanly() {
        Set<UUID> handled = ConcurrentHashMap.newKeySet();
        JobHandler handler = new JobHandler() {
            @Override
            public String type() {
                return "noop";
            }

            @Override
            public void handle(Job job) {
                handled.add(job.id());
            }
        };
        for (int i = 0; i < 50; i++) {
            repository.insertIfAbsent(UUID.randomUUID(), new NewJob("noop", JsonNodeFactory.instance.objectNode(), null, null));
        }
        WorkerPool pool = new WorkerPool(
                new WorkerProperties(true, 3, 5, Duration.ofMillis(50), Duration.ofSeconds(5)),
                repository, new HandlerRegistry(List.of(handler)),
                BackoffPolicy.withRandomJitter(Duration.ofMillis(10), Duration.ofMillis(100)));

        pool.start();
        try {
            await().atMost(Duration.ofSeconds(30)).until(() -> succeeded() == 50);
        } finally {
            pool.stop();
        }

        assertThat(pool.isRunning()).isFalse();
        assertThat(handled).hasSize(50);
        assertThat(Thread.getAllStackTraces().keySet())
                .noneMatch(t -> t.getName().startsWith(pool.threadNamePrefix()) && t.isAlive());
    }

    private long succeeded() {
        return jdbc.sql("SELECT count(*) FROM jobs WHERE status = 'SUCCEEDED'").query(Long.class).single();
    }
}
