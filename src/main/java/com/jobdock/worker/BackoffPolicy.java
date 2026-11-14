package com.jobdock.worker;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Exponential backoff with jitter (docs/DESIGN.md section 6):
 *
 * <pre>
 *   delay(n) = min(base * 2^(n-1), max) * (0.5 + 0.5 * r),   r uniform in [0, 1)
 * </pre>
 *
 * where {@code n} is the attempt that just failed (1-based). The jitter factor is therefore in
 * [0.5, 1.0), which spreads retries of jobs that failed together. Randomness is injected so the
 * formula can be tested deterministically. The delay is relative: the database adds it to its own
 * {@code now()}, so no application clock is involved.
 */
public final class BackoffPolicy {

    private final Duration base;
    private final Duration max;
    private final DoubleSupplier random;

    public BackoffPolicy(Duration base, Duration max, DoubleSupplier random) {
        Objects.requireNonNull(base, "base");
        Objects.requireNonNull(max, "max");
        if (base.isNegative() || base.isZero()) {
            throw new IllegalArgumentException("base delay must be positive");
        }
        if (max.compareTo(base) < 0) {
            throw new IllegalArgumentException("max delay must be >= base delay");
        }
        this.base = base;
        this.max = max;
        this.random = Objects.requireNonNull(random, "random");
    }

    public static BackoffPolicy withRandomJitter(Duration base, Duration max) {
        return new BackoffPolicy(base, max, () -> ThreadLocalRandom.current().nextDouble());
    }

    /** Delay before the retry that follows failed attempt {@code attempt} (1-based). */
    public Duration delay(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1, was " + attempt);
        }
        double r = random.getAsDouble();
        if (!(r >= 0.0 && r < 1.0)) {
            throw new IllegalStateException("random source must return a value in [0, 1), got " + r);
        }
        // Doubles cannot overflow to a wrong value here: huge exponents become +Infinity and are capped.
        double uncappedMillis = base.toMillis() * Math.pow(2, attempt - 1);
        double cappedMillis = Math.min(uncappedMillis, (double) max.toMillis());
        double jittered = cappedMillis * (0.5 + 0.5 * r);
        return Duration.ofMillis(Math.round(jittered));
    }

    public Duration base() {
        return base;
    }

    public Duration max() {
        return max;
    }
}
