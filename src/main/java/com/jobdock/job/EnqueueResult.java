package com.jobdock.job;

/** Outcome of an enqueue: a new job, or the original job for a replayed idempotency key. */
public sealed interface EnqueueResult {

    Job job();

    record Created(Job job) implements EnqueueResult {}

    record Replayed(Job job) implements EnqueueResult {}
}
