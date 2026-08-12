package com.czetsuyatech.nerv.event.application.dto;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.services.InboxOperationService;
import java.time.Instant;

/**
 * Read-only operational representation of a durable inbox row.
 */
public record InboxEventView(
    EventId eventId,
    String eventType,
    Instant eventTimestamp,
    String source,
    String correlationId,
    InboxStatus status,
    int attemptCount,
    Instant receivedAt,
    Instant processingAt,
    String processingBy,
    Instant processedAt,
    Instant failedAt,
    Instant availableAt,
    String lastError,
    Instant createdAt,
    Instant updatedAt,
    String contentType,
    String payload
)
{
  /**
   * {@code payload} is present only for an individual {@link InboxOperationService#find} or retry result.
   */
}
