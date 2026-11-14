# JobDock — project context

## Goal
A durable background job queue on PostgreSQL. The core problem: jobs must not be
lost, and must not run twice by accident, when workers crash or fail.
This is an interview-portfolio project: clear scope, honest claims, real tests.

## Scope
- Enqueue jobs via REST, with an optional idempotency key.
- Workers claim jobs safely with `SELECT ... FOR UPDATE SKIP LOCKED`.
- Visibility timeout / lease with heartbeat.
- Retries with exponential backoff and a max-attempts limit.
- Dead-letter state (`DEAD`).
- Recovery of jobs whose worker crashed mid-run (lease expiry -> requeue).
- Basic metrics endpoint.

## Non-goals (do not add)
UI, auth, multi-tenancy, Kafka/Redis or any other broker, cloud deployment,
paid services, secrets.

## Stack
Java 21, Spring Boot 3.5, Spring JDBC (`JdbcClient`, explicit SQL — no JPA),
PostgreSQL 16, Flyway, Maven (wrapper), JUnit 5, Testcontainers, Docker Compose.

## Build and test
```
./mvnw verify
```
- `*Test` classes are unit tests (Surefire, no database).
- `*IT` classes are integration tests (Failsafe) against a real PostgreSQL 16
  started by Testcontainers. Docker must be running.

## Architecture rules
- All queue state lives in the `jobs` table. No in-memory queue state that must
  survive a crash.
- Every state transition is a single SQL statement guarded by the expected
  current state (and, once leases exist, the owning worker). A transition that
  affects 0 rows means "you no longer own this job" — never ignore it.
- Job handlers run **outside** any database transaction. Row locks are held
  only for the duration of the claim statement.
- Schema changes only via new Flyway migrations (`V<n>__*.sql`); never edit an
  applied migration.
- Package layout: `api` (REST), `job` (domain + repository + service),
  `worker` (claim loop, handlers). Keep SQL in the repository class.
- Delivery semantics are at-least-once. Never claim exactly-once.
- Concurrency/locking behaviour is tested against real PostgreSQL, never a mock.

## Definition of done (per task)
- One small reviewable change with its own tests.
- `./mvnw verify` passes locally; exact test counts noted in the report.
- `docs/TASKS.md` ticked; `README.md` / `docs/DESIGN.md` updated if behaviour changed.
- No claims about behaviour that was not actually run. No invented numbers.

## Git rules (strict)
- Author and committer: `Sid13SST <220563227+Sid13SST@users.noreply.github.com>`
  (`git config user.name "Sid13SST" && git config user.email "220563227+Sid13SST@users.noreply.github.com"`).
- No `Co-Authored-By` trailers and no AI/assistant attribution anywhere
  (commit messages, PR text, files).
- One meaningful commit per task with a clear message. No empty or padding commits.
- Never rewrite or force-push history. Work on a feature branch, never `main`.
  Do not merge. Do not open PRs or publish anything unless explicitly asked.
