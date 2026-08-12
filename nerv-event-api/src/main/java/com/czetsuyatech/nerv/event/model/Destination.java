package com.czetsuyatech.nerv.event.model;

import java.util.Objects;
import lombok.Builder;

/**
 * Logical event destination, independent of any broker implementation.
 */
@Builder
public record Destination(String name) {

  public Destination {
    Objects.requireNonNull(name, "name must not be null");

    if (name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
  }
}
