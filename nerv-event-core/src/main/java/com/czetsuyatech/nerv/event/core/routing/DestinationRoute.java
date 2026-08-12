package com.czetsuyatech.nerv.event.core.routing;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import java.util.Objects;
import lombok.Builder;

/**
 * Physical delivery route for a logical destination.
 */
@Builder
public record DestinationRoute(
    BrokerId brokerId,
    String physicalTarget
)
{

  public DestinationRoute {
    Objects.requireNonNull(
        brokerId,
        "brokerId must not be null"
    );
    Objects.requireNonNull(
        physicalTarget,
        "physicalTarget must not be null"
    );
    if (physicalTarget.isBlank()) {
      throw new IllegalArgumentException("physicalTarget must not be blank");
    }
  }
}
