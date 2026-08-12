package com.czetsuyatech.nerv.event.spring.dispatcher;

import com.czetsuyatech.nerv.event.core.outbox.DispatchResult;
import java.time.Duration;
import java.util.Objects;
import java.util.function.DoubleSupplier;

/**
 * Calculates adaptive base and jittered delays without depending on Spring scheduling APIs.
 */
public final class DispatcherPollingPolicy {

  private final Duration minInterval;
  private final Duration maxInterval;
  private final double multiplier;
  private final double jitter;
  private final DoubleSupplier random;

  public DispatcherPollingPolicy(
      Duration minInterval,
      Duration maxInterval,
      double multiplier,
      double jitter,
      DoubleSupplier random
  )
  {
    this.minInterval = Objects.requireNonNull(
        minInterval,
        "minInterval must not be null"
    );
    this.maxInterval = Objects.requireNonNull(
        maxInterval,
        "maxInterval must not be null"
    );
    this.multiplier = multiplier;
    this.jitter = jitter;
    this.random = Objects.requireNonNull(
        random,
        "random must not be null"
    );
  }

  public Duration initialBaseDelay() {
    return minInterval;
  }

  public Duration nextBaseDelay(
      Duration currentBaseDelay,
      DispatchResult result
  ) {
    Objects.requireNonNull(
        result,
        "result must not be null"
    );
    return nextBaseDelay(
        currentBaseDelay,
        result.claimed() > 0
    );
  }

  /**
   * Calculates the next delay from whether the preceding cycle found claimable work.
   */
  public Duration nextBaseDelay(
      Duration currentBaseDelay,
      boolean claimedWork
  ) {
    Objects.requireNonNull(
        currentBaseDelay,
        "currentBaseDelay must not be null"
    );
    if (claimedWork) {
      return minInterval;
    }
    long multipliedNanos = Math.round(currentBaseDelay.toNanos() * multiplier);
    return Duration.ofNanos(
        Math.min(
            multipliedNanos,
            maxInterval.toNanos()
        )
    );
  }

  public Duration jitteredDelay(Duration baseDelay) {
    Objects.requireNonNull(
        baseDelay,
        "baseDelay must not be null"
    );
    double offset = (random.getAsDouble() * 2.0 - 1.0) * jitter;
    return Duration.ofNanos(
        Math.min(
            maxInterval.toNanos(),
            Math.max(
                1L,
                Math.round(baseDelay.toNanos() * (1.0 + offset))
            )
        )
    );
  }
}
