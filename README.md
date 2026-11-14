# JobDock

A durable background job queue built on PostgreSQL (Java 21, Spring Boot 3).
Goal: jobs are not lost and not run twice by accident when workers crash or fail.

**Status: work in progress.** See [docs/TASKS.md](docs/TASKS.md) for what is
built and what is planned, and [docs/DESIGN.md](docs/DESIGN.md) for the state
machine, schema, claim protocol and delivery semantics.

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
| `400 Bad Request` | missing/blank `type` (max 100 chars), `payload` not a JSON object, key longer than 255 chars |

Optional body field `runAt` (ISO-8601) delays the job. `GET /jobs/{id}` returns the job or `404`.

## Delivery semantics
At-least-once. Handlers must be idempotent. Exactly-once is not claimed.

## Out of scope
UI, auth, multi-tenancy, external brokers (Kafka/Redis), cloud deployment.
