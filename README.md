# JobDock

A durable background job queue built on PostgreSQL (Java 21, Spring Boot 3).
Goal: jobs are not lost and not run twice by accident when workers crash or fail.

**Status: work in progress.** See [docs/TASKS.md](docs/TASKS.md) for what is
built and what is planned, and [docs/DESIGN.md](docs/DESIGN.md) for the state
machine, schema, claim protocol and delivery semantics.

## Delivery semantics
At-least-once. Handlers must be idempotent. Exactly-once is not claimed.

## Out of scope
UI, auth, multi-tenancy, external brokers (Kafka/Redis), cloud deployment.
