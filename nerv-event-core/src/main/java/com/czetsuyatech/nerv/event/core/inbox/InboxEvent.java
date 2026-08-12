package com.czetsuyatech.nerv.event.core.inbox;

import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import java.time.Instant;
import java.util.Objects;
import lombok.Builder;

/**
 * Broker-neutral durable representation of an inbound event.
 */
@Builder
public record InboxEvent(
    EventId eventId,
    String eventType,
    Instant timestamp,
    String source,
    String correlationId,
    SerializedPayload payload,
    InboxStatus status,
    int attemptCount,
    Instant receivedAt,
    Instant availableAt,
    Instant processingAt,
    String processingBy,
    Instant processedAt,
    Instant failedAt,
    String lastError
)
{

  public InboxEvent {
    Objects.requireNonNull(
        eventId,
        "eventId must not be null"
    );
    requireNonBlank(
        eventType,
        "eventType"
    );
    Objects.requireNonNull(
        timestamp,
        "timestamp must not be null"
    );
    requireNonBlank(
        source,
        "source"
    );
    Objects.requireNonNull(
        payload,
        "payload must not be null"
    );
    Objects.requireNonNull(
        status,
        "status must not be null"
    );
    if (attemptCount < 0) {
      throw new IllegalArgumentException("attemptCount must not be negative");
    }
    Objects.requireNonNull(
        receivedAt,
        "receivedAt must not be null"
    );
    requireAvailableAtMatchesStatus(
        status,
        availableAt
    );
  }

  public boolean retryPending() {
    return status == InboxStatus.RETRY_PENDING;
  }

  public boolean retryEligible(Instant now) {
    Objects.requireNonNull(
        now,
        "now must not be null"
    );
    return retryPending() && !availableAt.isAfter(now);
  }

  public boolean retryExhausted() {
    return status == InboxStatus.FAILED;
  }

  private static void requireAvailableAtMatchesStatus(
      InboxStatus status,
      Instant availableAt
  ) {
    if (status == InboxStatus.RETRY_PENDING && availableAt == null) {
      throw new IllegalArgumentException("availableAt must not be null for RETRY_PENDING events");
    }
    if (status != InboxStatus.RETRY_PENDING && availableAt != null) {
      throw new IllegalArgumentException("availableAt is only allowed for RETRY_PENDING events");
    }
  }

  private static void requireNonBlank(
      String value,
      String name
  ) {
    Objects.requireNonNull(
        value,
        name + " must not be null"
    );
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
