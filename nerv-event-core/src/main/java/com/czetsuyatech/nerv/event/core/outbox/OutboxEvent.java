package com.czetsuyatech.nerv.event.core.outbox;

import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.model.EventMessage;
import java.time.Instant;
import java.util.Objects;
import lombok.Builder;

/**
 * Immutable, persistence-agnostic representation of a pending event delivery.
 */
@Builder
public record OutboxEvent(
    OutboxId id,
    EventMessage<?> event,
    Destination destination,
    int attemptCount,
    Instant nextAttemptAt,
    OutboxStatus status
)
{

  public OutboxEvent {
    Objects.requireNonNull(
        id,
        "id must not be null"
    );
    Objects.requireNonNull(
        event,
        "event must not be null"
    );
    Objects.requireNonNull(
        destination,
        "destination must not be null"
    );
    if (attemptCount < 0) {
      throw new IllegalArgumentException("attemptCount must not be negative");
    }
    Objects.requireNonNull(
        nextAttemptAt,
        "nextAttemptAt must not be null"
    );
    Objects.requireNonNull(
        status,
        "status must not be null"
    );
  }
}
