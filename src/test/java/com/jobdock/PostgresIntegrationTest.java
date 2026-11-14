package com.jobdock;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Base class for ITs against the shared real-Postgres context. Starts each test with an empty jobs table. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
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
