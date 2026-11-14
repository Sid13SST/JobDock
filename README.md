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

Not yet implemented: a handler that throws leaves its job `RUNNING`; retries,
backoff, dead-lettering and lease-based crash recovery are the next batches.

## Delivery semantics
At-least-once. Handlers must be idempotent. Exactly-once is not claimed.

## Out of scope
UI, auth, multi-tenancy, external brokers (Kafka/Redis), cloud deployment.
