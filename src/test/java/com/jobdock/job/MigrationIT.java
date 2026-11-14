package com.jobdock.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.jobdock.PostgresIntegrationTest;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

class MigrationIT extends PostgresIntegrationTest {

    @Test
    void v1IsApplied() {
        String version = jdbc.sql("SELECT max(version) FROM flyway_schema_history WHERE success")
                .query(String.class).single();
        assertThat(version).isEqualTo("1");
    }

    @Test
    void defaultsAreApplied() {
        UUID id = UUID.randomUUID();
        jdbc.sql("INSERT INTO jobs (id, type, payload, status) VALUES (?, 'email', '{}'::jsonb, 'QUEUED')")
                .param(id).update();
        Integer attempts = jdbc.sql("SELECT attempts FROM jobs WHERE id = ?").param(id).query(Integer.class).single();
        assertThat(attempts).isZero();
    }

    @Test
    void rejectsUnknownStatus() {
        assertThatThrownBy(() -> insert(UUID.randomUUID(), "BOGUS", null, null))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void runningRequiresOwnerAndOtherStatesForbidIt() {
        assertThatThrownBy(() -> insert(UUID.randomUUID(), "RUNNING", null, null))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> insert(UUID.randomUUID(), "QUEUED", "worker-1", null))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void idempotencyKeyIsUnique() {
        insert(UUID.randomUUID(), "QUEUED", null, "key-1");
        assertThatThrownBy(() -> insert(UUID.randomUUID(), "QUEUED", null, "key-1"))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void multipleJobsWithoutIdempotencyKeyAreAllowed() {
        insert(UUID.randomUUID(), "QUEUED", null, null);
        insert(UUID.randomUUID(), "QUEUED", null, null);
        assertThat(countJobs()).isEqualTo(2);
    }

    private void insert(UUID id, String status, String lockedBy, String key) {
        jdbc.sql("INSERT INTO jobs (id, type, payload, status, locked_by, idempotency_key) "
                        + "VALUES (?, 'email', '{}'::jsonb, ?, ?, ?)")
                .params(id, status, lockedBy, key).update();
    }
}
