package com.jobdock.worker;

import com.jobdock.job.JobRepository;
import com.jobdock.job.JobStatus;
import com.jobdock.job.ReapedJob;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Requeues or dead-letters RUNNING jobs whose lease expired. Any number may run at once. */
public class LeaseReaper {

    static final int BATCH = 100;
    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final JobRepository repository;

    public LeaseReaper(JobRepository repository) {
        this.repository = repository;
    }

    /** Reaps until no expired lease is left (that this reaper can lock). @return jobs reaped */
    public int reapOnce() {
        int total = 0;
        List<ReapedJob> batch;
        do {
            batch = repository.reapExpiredLeases(BATCH);
            for (ReapedJob job : batch) {
                if (job.status() == JobStatus.DEAD) {
                    log.error("Lease of worker {} on job {} expired on its last attempt {}; job is DEAD",
                            job.previousOwner(), job.id(), job.attempt());
                } else {
                    log.warn("Lease of worker {} on job {} expired during attempt {}; requeued for retry",
                            job.previousOwner(), job.id(), job.attempt());
                }
            }
            total += batch.size();
        } while (batch.size() == BATCH);
        return total;
    }
}
