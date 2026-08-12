package com.czetsuyatech.nerv.event.core.broker;

import java.util.Objects;
import lombok.Builder;

/**
 * Identifier of a configured broker integration.
 */
@Builder
public record BrokerId(String value) {

  public BrokerId {
    Objects.requireNonNull(
        value,
        "value must not be null"
    );
    if (value.isBlank()) {
      throw new IllegalArgumentException("value must not be blank");
    }
  }
}
