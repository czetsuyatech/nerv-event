package com.czetsuyatech.nerv.event.persistence.repository;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import java.time.Instant;

/**
 * Closed Spring Data projection for payload-free inbox search pages.
 */
public interface InboxSearchProjection {

  String getEventId();

  String getEventType();

  Instant getEventTimestamp();

  String getSource();

  String getCorrelationId();

  InboxStatus getStatus();

  int getAttemptCount();

  Instant getReceivedAt();

  Instant getProcessingAt();

  String getProcessingBy();

  Instant getProcessedAt();

  Instant getFailedAt();

  Instant getAvailableAt();

  String getLastError();

  Instant getCreatedAt();

  Instant getUpdatedAt();

  String getContentType();
}
