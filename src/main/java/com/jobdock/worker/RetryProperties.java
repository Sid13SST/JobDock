package com.jobdock.worker;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param baseDelay backoff after the first failed attempt (before jitter)
 * @param maxDelay cap on the backoff (before jitter)
 */
@ConfigurationProperties("jobdock.retry")
public record RetryProperties(
        @DefaultValue("1s") Duration baseDelay,
        @DefaultValue("5m") Duration maxDelay) {}
