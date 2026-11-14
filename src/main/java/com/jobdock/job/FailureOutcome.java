package com.jobdock.job;

import java.time.Instant;

/**
 * Result of recording a failed attempt.
 *
 * @param status {@link JobStatus#FAILED_RETRY} or {@link JobStatus#DEAD}
 * @param runAt when the job becomes eligible again (unchanged for DEAD)
 */
public record FailureOutcome(JobStatus status, Instant runAt) {}
