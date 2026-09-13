package com.czetsuyatech.nerv.event.model;

import java.util.Objects;
import lombok.Builder;

/**
 * An event together with its logical destination.
 *
 * @param <T>
 *          event payload type
 */
@Builder
public record EventPublication<T>(EventMessage<T> event, Destination destination, String orderingKey) {

  public EventPublication(EventMessage<T> event, Destination destination) {
    this(event, destination, null);
  }

  public EventPublication {
    Objects.requireNonNull(event, "event must not be null");
    Objects.requireNonNull(destination, "destination must not be null");
    if (orderingKey != null && orderingKey.isBlank()) {
      throw new IllegalArgumentException("orderingKey must not be blank when configured");
    }
  }
}
