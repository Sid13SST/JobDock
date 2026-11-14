package com.jobdock.api;

import com.jobdock.job.JobRepository;
import com.jobdock.job.QueueStats;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code GET /metrics}: queue statistics as JSON. See {@link QueueStats} for the definitions. */
@RestController
public class MetricsController {

    private final JobRepository repository;

    public MetricsController(JobRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/metrics")
    public QueueStats metrics() {
        return repository.stats();
    }
}
