package com.jobdock.api;

import com.jobdock.job.EnqueueResult;
import com.jobdock.job.IdempotencyConflictException;
import com.jobdock.job.JobService;
import com.jobdock.job.NewJob;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobController {

    static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    static final String REPLAYED = "Idempotent-Replayed";

    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    /** 201 for a new job; 200 (with {@code Idempotent-Replayed: true}) for a replayed key; 409 on key misuse. */
    @PostMapping
    public ResponseEntity<JobResponse> enqueue(
            @RequestHeader(name = IDEMPOTENCY_KEY, required = false) @Size(min = 1, max = 255) String idempotencyKey,
            @Valid @RequestBody EnqueueRequest request) {
        EnqueueResult result = service.enqueue(
                new NewJob(request.type(), request.payload(), idempotencyKey, request.runAt(),
                        request.maxAttempts() == null ? NewJob.DEFAULT_MAX_ATTEMPTS : request.maxAttempts()));
        JobResponse body = JobResponse.from(result.job());
        return switch (result) {
            case EnqueueResult.Created created ->
                    ResponseEntity.created(URI.create("/jobs/" + body.id())).body(body);
            case EnqueueResult.Replayed replayed ->
                    ResponseEntity.ok().header(REPLAYED, "true").location(URI.create("/jobs/" + body.id())).body(body);
        };
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobResponse> get(@PathVariable UUID id) {
        return service.find(id)
                .map(JobResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    ProblemDetail onConflict(IdempotencyConflictException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }
}
