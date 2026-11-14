package com.jobdock.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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
            id, type, payload, status, attempts, idempotency_key, run_at, locked_by,
            last_error, created_at, updated_at, started_at, finished_at""";

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
                        INSERT INTO jobs (id, type, payload, status, idempotency_key, run_at)
                        VALUES (:id, :type, CAST(:payload AS jsonb), 'QUEUED', :key, COALESCE(:runAt, now()))
                        ON CONFLICT (idempotency_key) DO NOTHING
                        RETURNING\s""" + COLUMNS)
                .param("id", id)
                .param("type", job.type())
                .param("payload", toJson(job))
                .param("key", job.idempotencyKey())
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

    /**
     * Atomically claims up to {@code limit} due QUEUED jobs of the given {@code types} for
     * {@code workerId}: sets them RUNNING, records the owner and increments {@code attempts}.
     * Rows locked by a concurrent claimer are skipped, not waited on. The locking CTE is
     * evaluated exactly once. Jobs of other types are left untouched.
     */
    public List<Job> claim(String workerId, Collection<String> types, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        if (types.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("""
                        WITH next AS (
                            SELECT id FROM jobs
                            WHERE status = 'QUEUED' AND run_at <= now() AND type = ANY(:types)
                            ORDER BY run_at, created_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE jobs j
                        SET status = 'RUNNING', locked_by = :worker, attempts = j.attempts + 1,
                            started_at = now(), updated_at = now()
                        FROM next
                        WHERE j.id = next.id
                        RETURNING\s""" + J_COLUMNS)
                .param("limit", limit)
                .param("worker", workerId)
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
                        SET status = 'SUCCEEDED', locked_by = NULL, finished_at = now(), updated_at = now()
                        WHERE id = ? AND status = 'RUNNING' AND locked_by = ?""")
                .params(id, workerId)
                .update();
        return updated == 1;
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
                    rs.getString("idempotency_key"),
                    instant(rs, "run_at"),
                    rs.getString("locked_by"),
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
