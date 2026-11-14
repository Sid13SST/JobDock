package com.jobdock.job;

import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class JobService {

    private final JobRepository repository;

    public JobService(JobRepository repository) {
        this.repository = repository;
    }

    /**
     * Enqueues a job. With an idempotency key, a repeated request with the same type and payload
     * returns the original job instead of creating a new one; a repeated key with a different
     * type or payload is rejected. {@code runAt} and {@code maxAttempts} are not part of the
     * comparison.
     */
    public EnqueueResult enqueue(NewJob newJob) {
        Optional<Job> inserted = repository.insertIfAbsent(UUID.randomUUID(), newJob);
        if (inserted.isPresent()) {
            return new EnqueueResult.Created(inserted.get());
        }
        String key = newJob.idempotencyKey();
        if (key == null) {
            throw new IllegalStateException("insert without idempotency key returned no row");
        }
        // Jobs are never deleted, so a key conflict means the row exists and is visible now.
        Job existing = repository.findByIdempotencyKey(key)
                .orElseThrow(() -> new IllegalStateException("idempotency key conflict but no row for key"));
        if (!existing.type().equals(newJob.type()) || !existing.payload().equals(newJob.payload())) {
            throw new IdempotencyConflictException(key);
        }
        return new EnqueueResult.Replayed(existing);
    }

    public Optional<Job> find(UUID id) {
        return repository.findById(id);
    }
}
