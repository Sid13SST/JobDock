package com.jobdock.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobdock.job.Job;
import com.jobdock.job.JobStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SleepJobHandlerTest {

    private final ObjectMapper om = new ObjectMapper();
    private final List<Duration> slept = new ArrayList<>();
    private final SleepJobHandler handler = new SleepJobHandler(slept::add);

    @Test
    void sleepsForPayloadSeconds() throws Exception {
        handler.handle(job("{\"seconds\":20}"));
        handler.handle(job("{}"));
        handler.handle(job("{\"seconds\":0}"));

        assertThat(slept).containsExactly(Duration.ofSeconds(20), Duration.ofSeconds(5), Duration.ZERO);
        assertThat(handler.type()).isEqualTo("demo.sleep");
    }

    @Test
    void rejectsInvalidSeconds() {
        for (String payload : new String[] {"{\"seconds\":-1}", "{\"seconds\":301}", "{\"seconds\":1.5}",
                "{\"seconds\":\"10\"}"}) {
            assertThatThrownBy(() -> handler.handle(job(payload))).as(payload)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(slept).isEmpty();
    }

    private Job job(String payload) throws Exception {
        Instant now = Instant.now();
        return new Job(UUID.randomUUID(), "demo.sleep", om.readTree(payload), JobStatus.RUNNING, 1, 5, null, now,
                "w1", now.plusSeconds(30), null, now, now, now, null);
    }
}
