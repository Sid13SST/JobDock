package com.jobdock.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobdock.PostgresIntegrationTest;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Heartbeat lease extension at the SQL level, against real Postgres. */
class LeaseIT extends PostgresIntegrationTest {

    private static final Set<String> NOOP = Set.of("noop");

    @Autowired
    JobRepository repository;

    @Test
    void extendLeasesMovesExpiryToNowPlusLeaseForCurrentClaims() {
        Job a = claimNew("w1", Duration.ofSeconds(5));
        Job b = claimNew("w1", Duration.ofSeconds(5));

        Set<UUID> extended = repository.extendLeases("w1", Map.of(a.id(), 1, b.id(), 1), Duration.ofMinutes(10));

        assertThat(extended).containsExactlyInAnyOrder(a.id(), b.id());
        Instant dbNow = jdbc.sql("SELECT now()").query(java.sql.Timestamp.class).single().toInstant();
        assertThat(load(a.id()).lockedUntil()).isAfter(dbNow.plus(Duration.ofMinutes(9)));
        assertThat(load(b.id()).lockedUntil()).isAfter(a.lockedUntil());
    }

    @Test
    void extendLeasesSkipsClaimsThatAreNotCurrent() {
        Job mine = claimNew("w1", Duration.ofSeconds(5));
        Job others = claimNew("w2", Duration.ofSeconds(5));
        Job finished = claimNew("w1", Duration.ofSeconds(5));
        repository.markSucceeded(finished.id(), "w1", 1);

        Set<UUID> extended = repository.extendLeases("w1",
                Map.of(mine.id(), 2 /* wrong attempt */, others.id(), 1, finished.id(), 1), Duration.ofMinutes(10));

        assertThat(extended).isEmpty();
        assertThat(load(mine.id()).lockedUntil()).isEqualTo(mine.lockedUntil());
        assertThat(load(others.id()).lockedUntil()).isEqualTo(others.lockedUntil());
        assertThat(load(finished.id()).lockedUntil()).isNull();
    }

    @Test
    void extendLeasesWithNoClaimsDoesNothing() {
        assertThat(repository.extendLeases("w1", Map.of(), Duration.ofSeconds(1))).isEmpty();
    }

    private Job claimNew(String worker, Duration lease) {
        repository.insertIfAbsent(UUID.randomUUID(), new NewJob("noop", JsonNodeFactory.instance.objectNode(), null, null));
        return repository.claim(worker, NOOP, 1, lease).getFirst();
    }

    private Job load(UUID id) {
        return repository.findById(id).orElseThrow();
    }
}
