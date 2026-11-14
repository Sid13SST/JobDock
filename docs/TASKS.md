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

## Batch 3 — leases and crash recovery
- [x] 3.1 Migration `V3`: `locked_until`; claim sets lease `now() + lease-duration` + tests
- [x] 3.2 Fencing: success/failure updates apply only if owner **and** attempt still match + ITs
- [x] 3.3 Heartbeat thread extends leases of held jobs; lease-lost detection + ITs
- [x] 3.4 Reaper (safe with concurrent reapers): expired lease -> `FAILED_RETRY` / `DEAD`; zombie and concurrent-reaper ITs
- [x] 3.5 Worker robustness: any `Throwable` from a handler is a failure; a failed result write does not strand the rest of the batch
- [x] 3.6 Real crash test: worker in a separate JVM killed with `kill -9` mid-job, job recovered and completed by another worker
- [x] 3.7 Mutation checks; DESIGN.md delivery semantics updated for leases

Batch 3 mutation checks (each change made, tests run, change reverted; all detected):
results fenced by owner only, not attempt -> `FencingIT` (same worker id, stale attempt);
results not fenced at all -> `FencingIT`, `ZombieWorkerIT`;
heartbeat disabled -> `HeartbeatIT`, `CrashRecoveryIT`;
heartbeat not fenced by attempt -> `LeaseIT`;
reaper without `FOR UPDATE SKIP LOCKED` -> `LeaseReaperIT` (concurrent reapers double-requeue);
reaper ignoring lease expiry -> `HeartbeatIT`, `LeaseReaperIT`;
reaper never reaping -> `CrashRecoveryIT`;
handler `Error`s not caught -> `WorkerTest`;
result-write failure aborting the batch -> `WorkerTest`, `ResultWriteFailureIT`.

## Batch 4 — ops, demo, README
- [x] 4.1 `GET /metrics` (counts per state, oldest due age, retries, dead-lettered, lease expirations; `V4` adds `lease_expirations`) + IT
- [ ] 4.2 Sample handler + Dockerfile + `docker-compose.yml` (api, workers, Postgres 16; pinned tags, healthchecks) + crash demo script, actually run
- [ ] 4.3 Verify the app starts standalone (`java -jar`) against a real Postgres
- [ ] 4.4 README for interviews: problem, architecture, guarantees, how to run, trade-offs, next steps
