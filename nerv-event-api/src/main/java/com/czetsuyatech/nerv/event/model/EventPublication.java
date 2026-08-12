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
public record EventPublication<T>(EventMessage<T> event, Destination destination) {

  public EventPublication {
    Objects.requireNonNull(event, "event must not be null");
    Objects.requireNonNull(destination, "destination must not be null");
  }
}
