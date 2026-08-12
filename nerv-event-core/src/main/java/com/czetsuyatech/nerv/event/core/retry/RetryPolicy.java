package com.czetsuyatech.nerv.event.core.retry;

import java.time.Instant;

/**
 * Determines whether an event can be attempted again after a failed delivery.
 */
public interface RetryPolicy {

  boolean allowsRetry(int failedAttemptCount);

  Instant nextEligibleAt(
      int failedAttemptCount,
      Instant failedAt
  );
}
