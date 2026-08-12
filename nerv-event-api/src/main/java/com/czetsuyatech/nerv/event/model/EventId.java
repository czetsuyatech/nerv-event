package com.czetsuyatech.nerv.event.model;

import java.util.Objects;
import lombok.Builder;

/**
 * Stable identifier for an event.
 */
@Builder
public record EventId(String value) {

  public EventId {
    Objects.requireNonNull(value, "value must not be null");

    if (value.isBlank()) {
      throw new IllegalArgumentException("value must not be blank");
    }
  }
}
