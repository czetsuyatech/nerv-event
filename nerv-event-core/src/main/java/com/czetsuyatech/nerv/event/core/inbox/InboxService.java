package com.czetsuyatech.nerv.event.core.inbox;

import com.czetsuyatech.nerv.event.model.EventId;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * <p>
 * Intent-oriented port for durable inbox registration and processing state.
 * </p>
 *
 * <p>
 * Legal transitions are {@code RECEIVED -> PROCESSING}, {@code PROCESSING -> PROCESSED}, {@code PROCESSING ->
 * RETRY_PENDING}, {@code PROCESSING -> FAILED}, and {@code RETRY_PENDING -> PROCESSING}. A processor may also reclaim
 * an expired {@code PROCESSING} lease into a new {@code PROCESSING} lease.
 * </p>
 */
public interface InboxService {

  InboxRegistration register(InboxEvent event);

  Optional<InboxEvent> find(EventId eventId);

  Optional<InboxEvent> claim(
      EventId eventId,
      Instant now,
      String owner,
      Duration leaseDuration
  );

  /**
   * Claims due {@link InboxStatus#RETRY_PENDING} events for another handler execution.
   */
  List<InboxEvent> claimPendingRetries(
      int limit,
      Instant now,
      String owner,
      Duration leaseDuration
  );

  void markProcessed(
      EventId eventId,
      String owner,
      Instant processedAt
  );

  /**
   * <p>
   * Records a handler failure that remains eligible for automatic retry.
   * </p>
   *
   * <p>
   * This is the explicit {@code PROCESSING -> RETRY_PENDING} transition. {@code attemptCount} counts actual {@code
   * EventHandler} executions, including the attempt that just failed.
   * </p>
   */
  void markRetryPending(
      EventId eventId,
      String owner,
      int attemptCount,
      Instant failedAt,
      Instant availableAt,
      String error
  );

  /**
   * <p>
   * Records a terminal handler failure after automatic retries are exhausted.
   * </p>
   *
   * <p>
   * This is the explicit {@code PROCESSING -> FAILED} transition. {@code attemptCount} counts actual {@code
   * EventHandler} executions, not broker deliveries, claims, or acknowledgements.
   * </p>
   */
  void markFailed(
      EventId eventId,
      String owner,
      int attemptCount,
      Instant failedAt,
      String error
  );
}
