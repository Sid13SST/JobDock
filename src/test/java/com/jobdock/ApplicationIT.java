package com.jobdock;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ApplicationIT {

    @Autowired
    JdbcClient jdbc;

    @Test
    void contextStartsAgainstRealPostgres16() {
        String version = jdbc.sql("SHOW server_version").query(String.class).single();
        assertThat(version).startsWith("16.");
    }
}
