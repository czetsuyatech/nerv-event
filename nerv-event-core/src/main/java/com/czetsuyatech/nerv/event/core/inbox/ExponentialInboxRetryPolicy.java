package com.czetsuyatech.nerv.event.core.inbox;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import lombok.Getter;

/**
 * Exponential automatic retry policy; maxAttempts includes the initial handler attempt.
 */
@Getter
public final class ExponentialInboxRetryPolicy implements InboxRetryPolicy {

  private final int maxAttempts;
  private final Duration initialDelay;
  private final double multiplier;
  private final Duration maxDelay;

  public ExponentialInboxRetryPolicy(
      int maxAttempts,
      Duration initialDelay,
      double multiplier,
      Duration maxDelay
  )
  {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be at least one");
    }
    this.initialDelay = positive(
        initialDelay,
        "initialDelay"
    );
    if (!Double.isFinite(multiplier) || multiplier < 1.0) {
      throw new IllegalArgumentException("multiplier must be finite and at least 1.0");
    }
    this.maxDelay = positive(
        maxDelay,
        "maxDelay"
    );
    if (maxDelay.compareTo(initialDelay) < 0) {
      throw new IllegalArgumentException(
          "maxDelay must be greater than or equal to initialDelay"
      );
    }
    this.maxAttempts = maxAttempts;
    this.multiplier = multiplier;
  }

  @Override
  public boolean canRetry(int attemptCount) {
    return attemptCount >= 0 && attemptCount < maxAttempts;
  }

  @Override
  public Instant nextAttemptAt(
      int attemptCount,
      Instant failedAt
  ) {
    if (attemptCount < 1) {
      throw new IllegalArgumentException("attemptCount must be at least one");
    }
    Objects.requireNonNull(
        failedAt,
        "failedAt must not be null"
    );
    return failedAt.plus(scaledDelay(attemptCount));
  }

  private Duration scaledDelay(int attemptCount) {
    double scaledMillis = initialDelay.toMillis() * Math.pow(
        multiplier,
        attemptCount - 1L
    );
    if (!Double.isFinite(scaledMillis) || scaledMillis >= maxDelay.toMillis()) {
      return maxDelay;
    }
    return Duration.ofMillis(
        Math.min(
            maxDelay.toMillis(),
            Math.round(scaledMillis)
        )
    );
  }

  private static Duration positive(
      Duration value,
      String name
  ) {
    Objects.requireNonNull(
        value,
        name + " must not be null"
    );
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be greater than zero");
    }
    return value;
  }
}
