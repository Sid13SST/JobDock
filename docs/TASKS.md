# Tasks

Each task = one reviewable commit with its own tests. `./mvnw verify` must pass.

## Batch 0 — project context
- [x] CLAUDE.md, docs/DESIGN.md, docs/TASKS.md, README.md

## Batch 1 — enqueue, claim, success path
- [x] 1.1 Maven project skeleton (Spring Boot 3.5, Java 21, Maven wrapper, Surefire/Failsafe split, Testcontainers wiring)
- [x] 1.2 Flyway migration `V1__create_jobs.sql` + migration IT
- [x] 1.3 `JobRepository`: insert with idempotency key, find, claim (SKIP LOCKED), mark succeeded + ITs
- [ ] 1.4 Enqueue REST API (`POST /jobs`, `GET /jobs/{id}`, `Idempotency-Key`) + unit and ITs
- [ ] 1.5 Worker: handler registry, claim loop, success path, worker pool lifecycle + unit tests
- [ ] 1.6 Concurrency IT: N workers x M jobs, each executed exactly once on the success path, none lost
- [ ] 1.7 GitHub Actions CI running `./mvnw verify`

## Batch 2 — failure handling (planned)
- [ ] 2.1 Migration: `max_attempts`; handler failure -> `FAILED_RETRY` with exponential backoff + jitter
- [ ] 2.2 `DEAD` after `max_attempts`; unknown job type -> `DEAD`
- [ ] 2.3 Claim also picks `FAILED_RETRY` rows whose `run_at` has passed

## Batch 3 — leases and crash recovery (planned)
- [ ] 3.1 Migration: `locked_until`; claim sets lease
- [ ] 3.2 Heartbeat while handler runs; lease-lost detection
- [ ] 3.3 Reaper: expired leases -> retry/dead; test with a simulated crashed worker
- [ ] 3.4 Late completion after reclaim is rejected (test)

## Batch 4 — ops (planned)
- [ ] 4.1 Metrics endpoint (counts per state, oldest queued age) via Actuator/Micrometer
- [ ] 4.2 Dockerfile + `compose.yaml` (app + Postgres), verified with `docker compose up`
- [ ] 4.3 Poll loop resilience when the DB is unavailable
