package com.czetsuyatech.nerv.event.core.outbox;

import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.model.EventMessage;
import java.time.Instant;
import java.util.Objects;
import lombok.Builder;

/**
 * Immutable, persistence-agnostic representation of an event delivery. {@code claimVersion} is a monotonic fencing
 * token: every claim increments it, and workers must present the value they received for every post-claim transition.
 */
@Builder
public record OutboxEvent(
    OutboxId id,
    EventMessage<?> event,
    Destination destination,
    int attemptCount,
    Instant nextAttemptAt,
    OutboxStatus status,
    String orderingKey,
    String lockedBy,
    long claimVersion
)
{

  public OutboxEvent(
      OutboxId id,
      EventMessage<?> event,
      Destination destination,
      int attemptCount,
      Instant nextAttemptAt,
      OutboxStatus status
  )
  {
    this(id, event, destination, attemptCount, nextAttemptAt, status, null, null, 0);
  }

  public OutboxEvent(
      OutboxId id,
      EventMessage<?> event,
      Destination destination,
      int attemptCount,
      Instant nextAttemptAt,
      OutboxStatus status,
      String lockedBy,
      long claimVersion
  )
  {
    this(id, event, destination, attemptCount, nextAttemptAt, status, null, lockedBy, claimVersion);
  }

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
    if (claimVersion < 0) {
      throw new IllegalArgumentException("claimVersion must not be negative");
    }
    if (orderingKey != null && orderingKey.isBlank()) {
      throw new IllegalArgumentException("orderingKey must not be blank when configured");
    }
  }
}
