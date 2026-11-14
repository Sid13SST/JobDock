package com.jobdock.job;

import java.util.UUID;

/**
 * A RUNNING job whose lease expired and was taken back by the reaper.
 *
 * @param status {@link JobStatus#FAILED_RETRY} or {@link JobStatus#DEAD}
 * @param previousOwner the worker that stopped heartbeating
 * @param attempt the attempt that was abandoned
 */
public record ReapedJob(UUID id, JobStatus status, String previousOwner, int attempt) {}
