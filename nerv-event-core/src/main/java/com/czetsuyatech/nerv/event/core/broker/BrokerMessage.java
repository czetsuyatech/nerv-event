package com.czetsuyatech.nerv.event.core.broker;

import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import java.time.Instant;
import java.util.Objects;
import lombok.Builder;

/**
 * A generic serialized event addressed to a broker-specific target.
 */
@Builder
public record BrokerMessage(
    EventId eventId,
    String eventType,
    Instant timestamp,
    String source,
    String correlationId,
    String orderingKey,
    String target,
    SerializedPayload payload
)
{

  public BrokerMessage(
      EventId eventId,
      String eventType,
      Instant timestamp,
      String source,
      String correlationId,
      String target,
      SerializedPayload payload
  )
  {
    this(eventId, eventType, timestamp, source, correlationId, null, target, payload);
  }

  public BrokerMessage {
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
    requireNonBlank(
        target,
        "target"
    );
    if (orderingKey != null && orderingKey.isBlank()) {
      throw new IllegalArgumentException("orderingKey must not be blank when configured");
    }
    Objects.requireNonNull(
        payload,
        "payload must not be null"
    );
  }

  private static void requireNonBlank(
      String value,
      String fieldName
  ) {
    Objects.requireNonNull(
        value,
        fieldName + " must not be null"
    );
    if (value.isBlank()) {
      throw new IllegalArgumentException(fieldName + " must not be blank");
    }
  }
}
