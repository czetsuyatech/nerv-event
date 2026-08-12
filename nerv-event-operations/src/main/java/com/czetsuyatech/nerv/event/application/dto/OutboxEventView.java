package com.czetsuyatech.nerv.event.application.dto;

import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.services.OutboxOperationService;
import java.time.Instant;

/**
 * Read-only operational representation of a durable outbox publication row.
 */
public record OutboxEventView(
    OutboxId outboxId,
    EventId eventId,
    String eventType,
    Instant eventTimestamp,
    String source,
    String correlationId,
    String destination,
    OutboxStatus status,
    int attemptCount,
    Instant availableAt,
    Instant lockedAt,
    String lockedBy,
    String lastError,
    Instant createdAt,
    Instant updatedAt,
    Instant publishedAt,
    String payload
)
{
  /**
   * {@code payload} is present only for an individual {@link OutboxOperationService#find} or retry result.
   */
}
