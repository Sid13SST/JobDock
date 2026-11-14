package com.jobdock.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BackoffPolicyTest {

    private static final Duration BASE = Duration.ofSeconds(1);
    private static final Duration MAX = Duration.ofMinutes(5);

    @ParameterizedTest(name = "attempt {0} -> {1} ms at full jitter factor")
    @CsvSource({
        "1, 1000",
        "2, 2000",
        "3, 4000",
        "4, 8000",
        "9, 256000",
        "10, 300000", // 512 s capped at 300 s
        "50, 300000",
        "2147483647, 300000" // no overflow for absurd attempt counts
    })
    void doublesPerAttemptUpToTheCap(int attempt, long expectedMillis) {
        // r just below 1 gives a jitter factor just below 1.0; rounding lands on the exact value.
        BackoffPolicy policy = new BackoffPolicy(BASE, MAX, () -> Math.nextDown(1.0));

        assertThat(policy.delay(attempt)).isEqualTo(Duration.ofMillis(expectedMillis));
    }

    @Test
    void lowestJitterHalvesTheDelay() {
        BackoffPolicy policy = new BackoffPolicy(BASE, MAX, () -> 0.0);

        assertThat(policy.delay(1)).isEqualTo(Duration.ofMillis(500));
        assertThat(policy.delay(3)).isEqualTo(Duration.ofMillis(2000));
        assertThat(policy.delay(20)).isEqualTo(Duration.ofMillis(150_000));
    }

    @Test
    void jitterIsLinearInTheRandomValue() {
        ArrayDeque<Double> values = new ArrayDeque<>(List.of(0.25, 0.5, 0.75));
        BackoffPolicy policy = new BackoffPolicy(BASE, MAX, values::removeFirst);

        // attempt 3 -> 4000 ms * (0.5 + 0.5 r)
        assertThat(policy.delay(3)).isEqualTo(Duration.ofMillis(2500));
        assertThat(policy.delay(3)).isEqualTo(Duration.ofMillis(3000));
        assertThat(policy.delay(3)).isEqualTo(Duration.ofMillis(3500));
    }

    @Test
    void realRandomJitterStaysWithinBounds() {
        BackoffPolicy policy = BackoffPolicy.withRandomJitter(BASE, MAX);
        for (int i = 0; i < 1000; i++) {
            assertThat(policy.delay(4)).isBetween(Duration.ofMillis(4000), Duration.ofMillis(8000));
        }
    }

    @Test
    void rejectsInvalidInput() {
        BackoffPolicy policy = new BackoffPolicy(BASE, MAX, () -> 0.0);
        assertThatThrownBy(() -> policy.delay(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ZERO, MAX, () -> 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(MAX, BASE, () -> 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(BASE, MAX, () -> 1.0).delay(1))
                .isInstanceOf(IllegalStateException.class);
    }
}
