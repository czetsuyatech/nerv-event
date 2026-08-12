package com.czetsuyatech.nerv.event.model;

import java.time.Instant;
import java.util.Objects;
import lombok.Builder;

/**
 * Immutable event envelope and payload.
 *
 * @param <T>
 *          payload type
 */
@Builder
public record EventMessage<T>(
    EventId id,
    String type,
    Instant timestamp,
    String source,
    String correlationId,
    T payload
)
{

  public EventMessage {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(timestamp, "timestamp must not be null");
    Objects.requireNonNull(source, "source must not be null");
    Objects.requireNonNull(payload, "payload must not be null");

    if (type.isBlank()) {
      throw new IllegalArgumentException("type must not be blank");
    }

    if (source.isBlank()) {
      throw new IllegalArgumentException("source must not be blank");
    }
  }
}
