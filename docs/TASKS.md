# Tasks

Each task = one reviewable commit with its own tests. `./mvnw verify` must pass.

## Batch 0 — project context
- [x] CLAUDE.md, docs/DESIGN.md, docs/TASKS.md, README.md

## Batch 1 — enqueue, claim, success path
- [x] 1.1 Maven project skeleton (Spring Boot 3.5, Java 21, Maven wrapper, Surefire/Failsafe split, Testcontainers wiring)
- [x] 1.2 Flyway migration `V1__create_jobs.sql` + migration IT
- [x] 1.3 `JobRepository`: insert with idempotency key, find, claim (SKIP LOCKED), mark succeeded + ITs
- [x] 1.4 Enqueue REST API (`POST /jobs`, `GET /jobs/{id}`, `Idempotency-Key`) + unit and ITs
- [x] 1.5 Worker: handler registry, claim loop, success path, worker pool lifecycle + unit tests
- [x] 1.6 Concurrency IT: N workers x M jobs, each executed exactly once on the success path, none lost
- [x] 1.7 GitHub Actions CI running `./mvnw verify`

## Batch 2 — failure handling
- [x] 2.1 `BackoffPolicy`: exponential backoff with jitter, injected randomness + unit tests
- [x] 2.2 Migration `V2`: `max_attempts` (default 5, optional per job via API) + tests
- [x] 2.3 Repository: guarded failure transition (`FAILED_RETRY` with backoff, or `DEAD` keeping `last_error`); claim also picks due `FAILED_RETRY` rows + ITs
- [x] 2.4 Worker failure path wired to the repository + unit tests + retry-then-dead IT
- [x] 2.5 Concurrency IT: retrying jobs are held by exactly one worker at a time; mutation checks

Batch 2 mutation checks (each change made, tests run, change reverted; all detected):
claim also taking `RUNNING` rows -> `RetryConcurrencyIT` (overlap detected);
DEAD condition `attempts > max_attempts` -> `JobFailureIT`, `RetryLifecycleIT`;
failure update without status/owner guard -> `JobFailureIT`;
backoff without cap / without jitter -> `BackoffPolicyTest`.

## Batch 3 — leases and crash recovery (planned)
- [ ] 3.1 Migration: `locked_until`; claim sets lease
- [ ] 3.2 Heartbeat while handler runs; lease-lost detection
- [ ] 3.3 Reaper: expired leases -> retry/dead; test with a simulated crashed worker
- [ ] 3.4 Late completion after reclaim is rejected (test)

## Batch 4 — ops (planned)
- [ ] 4.1 Metrics endpoint (counts per state, oldest queued age) via Actuator/Micrometer
- [ ] 4.2 Dockerfile + `compose.yaml` (app + Postgres), verified with `docker compose up`
- [ ] 4.3 Poll loop resilience when the DB is unavailable
