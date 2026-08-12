package com.czetsuyatech.nerv.event.core.outbox;

import java.util.Objects;
import lombok.Builder;

/**
 * Identifier of a durable outbox event.
 */
@Builder
public record OutboxId(String value) {

  public OutboxId {
    Objects.requireNonNull(
        value,
        "value must not be null"
    );
    if (value.isBlank()) {
      throw new IllegalArgumentException("value must not be blank");
    }
  }
}
