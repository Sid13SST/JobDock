package com.jobdock.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.jobdock.job.Job;
import com.jobdock.worker.JobHandler;
import java.net.InetAddress;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Demo handler for type {@value #TYPE}: sleeps {@code payload.seconds} (0..300, default 5) and
 * logs start and end with the host name, so the crash demo can show which container ran which
 * attempt. Registered only with {@code jobdock.demo.enabled=true}.
 *
 * <p>Idempotent by construction (no side effects besides logging).
 */
@Component
@ConditionalOnProperty(name = "jobdock.demo.enabled", havingValue = "true")
public class SleepJobHandler implements JobHandler {

    public static final String TYPE = "demo.sleep";
    static final int MAX_SECONDS = 300;

    private static final Logger log = LoggerFactory.getLogger(SleepJobHandler.class);

    interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    private final Sleeper sleeper;

    public SleepJobHandler() {
        this(d -> Thread.sleep(d.toMillis()));
    }

    SleepJobHandler(Sleeper sleeper) {
        this.sleeper = sleeper;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public void handle(Job job) throws InterruptedException {
        Duration duration = Duration.ofSeconds(seconds(job.payload()));
        log.info("demo.sleep job {} attempt {} started on host {}, sleeping {}", job.id(), job.attempts(), host(),
                duration);
        sleeper.sleep(duration);
        log.info("demo.sleep job {} attempt {} finished on host {}", job.id(), job.attempts(), host());
    }

    static int seconds(JsonNode payload) {
        JsonNode value = payload.get("seconds");
        if (value == null || value.isNull()) {
            return 5;
        }
        if (!value.canConvertToInt() || !value.isIntegralNumber() || value.asInt() < 0 || value.asInt() > MAX_SECONDS) {
            throw new IllegalArgumentException("payload.seconds must be an integer in [0, " + MAX_SECONDS + "]");
        }
        return value.asInt();
    }

    private static String host() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }
}
