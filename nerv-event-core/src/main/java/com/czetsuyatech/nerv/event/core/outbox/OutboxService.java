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
   * Atomically claims eligible pending events and returns them in PROCESSING state.
   */
  List<OutboxEvent> claimPending(
      Instant eligibleAt,
      int batchSize
  );

  void markPublished(
      OutboxId id,
      BrokerPublishResult result
  );

  void reschedule(
      OutboxId id,
      int attemptCount,
      Instant nextAttemptAt,
      String failureReason
  );

  void markFailed(
      OutboxId id,
      int attemptCount,
      String failureReason
  );
}
