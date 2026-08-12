package com.czetsuyatech.nerv.event.core.consumer;

import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import java.time.Instant;
import java.util.Objects;
import lombok.Builder;

/**
 * A broker-neutral encoded event received for application handling.
 */
@Builder
public record ConsumerMessage(
    EventId eventId,
    String eventType,
    Instant timestamp,
    String source,
    String correlationId,
    SerializedPayload payload
)
{
  public ConsumerMessage {
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
