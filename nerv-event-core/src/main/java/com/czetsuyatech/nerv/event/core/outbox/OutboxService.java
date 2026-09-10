package com.czetsuyatech.nerv.event.core.outbox;

import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import java.time.Instant;
import java.util.List;

/**
 * Persistence port for the durable outbox lifecycle.
 */
public interface OutboxService {

  OutboxEvent save(OutboxEvent event);

  /**
   * Atomically claims eligible pending events and expired leases, increments their fencing tokens, and returns them in
   * PROCESSING state.
   */
  List<OutboxEvent> claimPending(
      Instant eligibleAt,
      int batchSize
  );

  /**
   * Completes the transition only when the row is still PROCESSING and owned by this service instance with the supplied
   * claim version. Returns {@code false} when ownership has changed.
   */
  boolean markPublished(
      OutboxId id,
      long claimVersion,
      BrokerPublishResult result
  );

  /** Returns {@code false} when the claim has been fenced by a newer owner/version. */
  boolean reschedule(
      OutboxId id,
      long claimVersion,
      int attemptCount,
      Instant nextAttemptAt,
      String failureReason
  );

  /** Returns {@code false} when the claim has been fenced by a newer owner/version. */
  boolean markFailed(
      OutboxId id,
      long claimVersion,
      int attemptCount,
      String failureReason
  );
}
