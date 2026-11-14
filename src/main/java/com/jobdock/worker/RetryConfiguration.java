package com.jobdock.worker;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class RetryConfiguration {

    @Bean
    BackoffPolicy backoffPolicy(RetryProperties props) {
        return BackoffPolicy.withRandomJitter(props.baseDelay(), props.maxDelay());
    }
}
