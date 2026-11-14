package com.jobdock.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * All SQL for the jobs table. Every state transition is one statement guarded by the
 * expected current state, so concurrent callers cannot both succeed.
 */
@Repository
public class JobRepository {

    private static final String COLUMNS = """
            id, type, payload, status, attempts, max_attempts, idempotency_key, run_at, locked_by,
            locked_until, last_error, created_at, updated_at, started_at, finished_at""";

    /** {@link #COLUMNS} qualified with alias {@code j}, for UPDATE ... FROM ... RETURNING. */
    private static final String J_COLUMNS = Arrays.stream(COLUMNS.split(","))
            .map(c -> "j." + c.strip())
            .collect(Collectors.joining(", "));

    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public JobRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /**
     * Inserts a new QUEUED job. If {@code idempotencyKey} is set and already used, nothing is
     * inserted and an empty Optional is returned (concurrent inserts with the same key are
     * serialized by the unique index, so exactly one of them creates a row).
     */
    public Optional<Job> insertIfAbsent(UUID id, NewJob job) {
        return jdbc.sql("""
                        INSERT INTO jobs (id, type, payload, status, idempotency_key, run_at, max_attempts)
                        VALUES (:id, :type, CAST(:payload AS jsonb), 'QUEUED', :key, COALESCE(:runAt, now()),
                                :maxAttempts)
                        ON CONFLICT (idempotency_key) DO NOTHING
                        RETURNING\s""" + COLUMNS)
                .param("id", id)
                .param("type", job.type())
                .param("payload", toJson(job))
                .param("key", job.idempotencyKey())
                .param("maxAttempts", job.maxAttempts())
                .param("runAt", job.runAt() == null ? null : Timestamp.from(job.runAt()))
                .query(this::mapRow)
                .optional();
    }

    public Optional<Job> findById(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM jobs WHERE id = ?")
                .param(id)
                .query(this::mapRow)
                .optional();
    }

    public Optional<Job> findByIdempotencyKey(String key) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM jobs WHERE idempotency_key = ?")
                .param(key)
                .query(this::mapRow)
                .optional();
    }

    /** Longer error texts are truncated before being stored in {@code last_error}. */
    public static final int MAX_ERROR_LENGTH = 4000;

    /**
     * Atomically claims up to {@code limit} due QUEUED or FAILED_RETRY jobs of the given {@code types} for
     * {@code workerId}: sets them RUNNING, records the owner, increments {@code attempts} and sets
     * the lease expiry {@code locked_until = now() + lease} (database clock).
     * Rows locked by a concurrent claimer are skipped, not waited on. The locking CTE is
     * evaluated exactly once. Jobs of other types are left untouched.
     */
    public List<Job> claim(String workerId, Collection<String> types, int limit, Duration lease) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        if (lease.isNegative() || lease.isZero()) {
            throw new IllegalArgumentException("lease must be positive");
        }
        if (types.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("""
                        WITH next AS (
                            SELECT id FROM jobs
                            WHERE status IN ('QUEUED', 'FAILED_RETRY') AND run_at <= now() AND type = ANY(:types)
                            ORDER BY run_at, created_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE jobs j
                        SET status = 'RUNNING', locked_by = :worker, attempts = j.attempts + 1,
                            locked_until = now() + :leaseMillis * interval '1 millisecond',
                            started_at = now(), updated_at = now()
                        FROM next
                        WHERE j.id = next.id
                        RETURNING\s""" + J_COLUMNS)
                .param("limit", limit)
                .param("worker", workerId)
                .param("leaseMillis", lease.toMillis())
                .param("types", types.toArray(String[]::new))
                .query(this::mapRow)
                .list();
    }

    /**
     * Marks a RUNNING job owned by {@code workerId} as SUCCEEDED.
     *
     * @return false if the job is not RUNNING or not owned by this worker (nothing changed)
     */
    public boolean markSucceeded(UUID id, String workerId) {
        int updated = jdbc.sql("""
                        UPDATE jobs
                        SET status = 'SUCCEEDED', locked_by = NULL, locked_until = NULL, finished_at = now(), updated_at = now()
                        WHERE id = ? AND status = 'RUNNING' AND locked_by = ?""")
                .params(id, workerId)
                .update();
        return updated == 1;
    }

    /**
     * Records a failed attempt of a RUNNING job owned by {@code workerId}, in one statement:
     * if the job has used all its attempts ({@code attempts >= max_attempts}) it becomes DEAD,
     * otherwise FAILED_RETRY with {@code run_at = now() + retryDelay} (database clock). The owner
     * is released and {@code error} stored in {@code last_error} in both cases.
     *
     * @return empty if the job is not RUNNING or not owned by this worker (nothing changed)
     */
    public Optional<FailureOutcome> markFailed(UUID id, String workerId, String error, Duration retryDelay) {
        if (retryDelay.isNegative()) {
            throw new IllegalArgumentException("retryDelay must not be negative");
        }
        return jdbc.sql("""
                        UPDATE jobs
                        SET status      = CASE WHEN attempts >= max_attempts THEN 'DEAD' ELSE 'FAILED_RETRY' END,
                            run_at      = CASE WHEN attempts >= max_attempts THEN run_at
                                               ELSE now() + :delayMillis * interval '1 millisecond' END,
                            finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
                            locked_by   = NULL,
                            locked_until = NULL,
                            last_error  = :error,
                            updated_at  = now()
                        WHERE id = :id AND status = 'RUNNING' AND locked_by = :worker
                        RETURNING status, run_at""")
                .param("delayMillis", retryDelay.toMillis())
                .param("error", truncate(error))
                .param("id", id)
                .param("worker", workerId)
                .query((rs, n) -> new FailureOutcome(JobStatus.valueOf(rs.getString("status")), instant(rs, "run_at")))
                .optional();
    }

    private static String truncate(String error) {
        if (error == null || error.length() <= MAX_ERROR_LENGTH) {
            return error;
        }
        return error.substring(0, MAX_ERROR_LENGTH);
    }

    private String toJson(NewJob job) {
        try {
            return objectMapper.writeValueAsString(job.payload());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("payload is not serializable", e);
        }
    }

    private Job mapRow(ResultSet rs, int rowNum) throws SQLException {
        try {
            return new Job(
                    rs.getObject("id", UUID.class),
                    rs.getString("type"),
                    objectMapper.readTree(rs.getString("payload")),
                    JobStatus.valueOf(rs.getString("status")),
                    rs.getInt("attempts"),
                    rs.getInt("max_attempts"),
                    rs.getString("idempotency_key"),
                    instant(rs, "run_at"),
                    rs.getString("locked_by"),
                    instant(rs, "locked_until"),
                    rs.getString("last_error"),
                    instant(rs, "created_at"),
                    instant(rs, "updated_at"),
                    instant(rs, "started_at"),
                    instant(rs, "finished_at"));
        } catch (JsonProcessingException e) {
            throw new SQLException("invalid payload JSON in job row", e);
        }
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
