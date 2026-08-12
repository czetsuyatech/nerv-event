package com.czetsuyatech.nerv.event.core.inbox;

import java.util.Objects;
import lombok.Builder;

/**
 * Result of durable inbound registration; duplicate delivery is a normal outcome.
 */
@Builder
public record InboxRegistration(
    boolean created,
    InboxEvent event
)
{

  public InboxRegistration {
    Objects.requireNonNull(
        event,
        "event must not be null"
    );
  }
}
