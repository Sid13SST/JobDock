package com.jobdock.job;

/** Job lifecycle states. See docs/DESIGN.md section 1. */
public enum JobStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED_RETRY,
    DEAD
}
