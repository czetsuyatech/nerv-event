package com.czetsuyatech.nerv.event.persistence.repository;

import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import java.time.Instant;

/**
 * Closed Spring Data projection for payload-free outbox search pages.
 */
public interface OutboxSearchProjection {

  String getId();

  String getEventId();

  String getEventType();

  Instant getEventTimestamp();

  String getSource();

  String getCorrelationId();

  String getDestination();

  OutboxStatus getStatus();

  int getAttemptCount();

  Instant getAvailableAt();

  Instant getLockedAt();

  String getLockedBy();

  long getClaimVersion();

  String getLastError();

  Instant getCreatedAt();

  Instant getUpdatedAt();

  Instant getPublishedAt();
}
