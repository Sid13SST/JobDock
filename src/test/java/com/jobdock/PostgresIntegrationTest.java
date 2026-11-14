package com.jobdock;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

/**
 * Base class for ITs against the shared real-Postgres context. Starts each test with an empty
 * jobs table. The background worker pool is disabled so tests drive workers explicitly.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "jobdock.worker.enabled=false")
public abstract class PostgresIntegrationTest {

    @Autowired
    protected JdbcClient jdbc;

    @BeforeEach
    void truncateJobs() {
        jdbc.sql("TRUNCATE jobs").update();
    }

    protected long countJobs() {
        return jdbc.sql("SELECT count(*) FROM jobs").query(Long.class).single();
    }
}
