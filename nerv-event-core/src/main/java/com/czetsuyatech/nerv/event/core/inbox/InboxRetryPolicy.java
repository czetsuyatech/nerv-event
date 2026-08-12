package com.czetsuyatech.nerv.event.core.inbox;

import java.time.Instant;

/**
 * <p>
 * Determines automatic retry eligibility and timing for failed {@code EventHandler} executions.
 * </p>
 *
 * <p>
 * {@code attemptCount} is the total number of handler executions already performed, including the latest failed
 * execution. It does not represent broker deliveries, claims, or acknowledgements.
 * </p>
 */
public interface InboxRetryPolicy {

  /**
   * Returns whether another automatic handler execution is allowed.
   */
  boolean canRetry(int attemptCount);

  /**
   * Returns when the next automatic handler execution becomes eligible.
   */
  Instant nextAttemptAt(
      int attemptCount,
      Instant failedAt
  );
}
