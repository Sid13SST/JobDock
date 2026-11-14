# JobDock

A durable background job queue built on PostgreSQL (Java 21, Spring Boot 3).
Goal: jobs are not lost and not run twice by accident when workers crash or fail.

**Status: work in progress.** See [docs/TASKS.md](docs/TASKS.md) for what is
built and what is planned, and [docs/DESIGN.md](docs/DESIGN.md) for the state
machine, schema, claim protocol and delivery semantics.

## Build and test

Requires Java 21 and a running Docker daemon (integration tests start
PostgreSQL 16 with Testcontainers; the database is never mocked).

```
./mvnw verify
```

Unit tests (`*Test`) run in Surefire, integration tests (`*IT`) in Failsafe.
CI runs the same command on GitHub Actions (`.github/workflows/ci.yml`).

To run the app you need a PostgreSQL database (defaults:
`jdbc:postgresql://localhost:5432/jobdock`, user/password `jobdock`; override with
`JOBDOCK_DB_URL`, `JOBDOCK_DB_USER`, `JOBDOCK_DB_PASSWORD`), then `./mvnw spring-boot:run`.
A Docker Compose setup is planned (docs/TASKS.md, batch 4).

## API

`POST /jobs` — enqueue a job.

```
curl -i -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42' \
  -d '{"type":"email","payload":{"to":"a@example.com"}}'
```

| Response | When |
|----------|------|
| `201 Created` + `Location` | new job |
| `200 OK` + `Idempotent-Replayed: true` | same `Idempotency-Key`, same `type` and `payload`: original job returned, nothing created |
| `409 Conflict` | same key reused with a different `type` or `payload` |
| `400 Bad Request` | missing/blank `type` (max 100 chars), `payload` not a JSON object, key longer than 255 chars, `maxAttempts` outside 1–100 |

Optional body fields: `runAt` (ISO-8601) delays the job; `maxAttempts` (1–100,
default 5) is the total number of attempts before the job is dead-lettered. `GET /jobs/{id}` returns the job or `404`.

## Workers

Implement `com.jobdock.worker.JobHandler` as a Spring bean (one per job `type`).
Worker threads claim due jobs of the registered types with
`FOR UPDATE SKIP LOCKED`, run the handler outside any transaction, and mark the
job `SUCCEEDED`. Configuration (`jobdock.worker.*`): `enabled` (true),
`threads` (4), `batch-size` (10), `idle-delay` (500ms), `shutdown-timeout` (30s).

## Retries and dead-lettering

If a handler throws, the job goes to `FAILED_RETRY` and becomes claimable again
after `min(base * 2^(n-1), max) * jitter` (jitter in [0.5, 1.0), `n` = attempt
number; `jobdock.retry.base-delay` 1s, `jobdock.retry.max-delay` 5m). After
`maxAttempts` attempts (default 5) the job is `DEAD`; `lastError` holds the last
failure. Retry times use the database clock. Details: docs/DESIGN.md section 6.

## Leases and crash recovery

A claim holds a lease (`jobdock.worker.lease-duration`, default 30s) that a
heartbeat thread extends every lease/3 while the worker holds the job. If a
worker dies, its heartbeats stop; once the lease expires, a reaper (every
`jobdock.worker.reaper-interval`, default 5s, safe to run on many instances)
moves the job to `FAILED_RETRY`, or `DEAD` if its attempts are used up, and
another worker runs it again. Results are fenced by worker id and attempt, so a
worker whose lease expired cannot overwrite the new owner's result. Any
`Throwable` from a handler counts as a failed attempt. Details:
docs/DESIGN.md sections 4, 5 and 5a.

## Delivery semantics
At-least-once. Handlers must be idempotent. Exactly-once is not claimed.

## Out of scope
UI, auth, multi-tenancy, external brokers (Kafka/Redis), cloud deployment.
