package com.jobdock.job;

import java.util.Map;

/**
 * Point-in-time queue statistics, computed from the jobs table (so they are the same whichever
 * instance serves them and survive restarts).
 *
 * @param countsByStatus number of jobs in each state (every state present, 0 if none)
 * @param oldestDueAgeSeconds how long the longest-waiting claimable job (QUEUED or FAILED_RETRY
 *     with {@code run_at <= now()}) has been due, in seconds; {@code null} if none is due
 * @param retriesTotal attempts started beyond each job's first ({@code sum(attempts - 1)}), i.e.
 *     retries after failures plus re-runs after lease expiry
 * @param deadLettered jobs in DEAD
 * @param leaseExpirationsTotal times the reaper took a job back from a worker that stopped
 *     heartbeating
 */
public record QueueStats(
        Map<JobStatus, Long> countsByStatus,
        Double oldestDueAgeSeconds,
        long retriesTotal,
        long deadLettered,
        long leaseExpirationsTotal) {}
