package com.czetsuyatech.nerv.event.sqs.routing;

import java.util.Map;
import java.util.Objects;

/**
 * Resolves a generic adapter target to immutable SQS infrastructure.
 */
public final class SqsDestinationResolver {
  private final Map<String, SqsDestination> destinations;

  public SqsDestinationResolver(Map<String, SqsDestination> destinations) {
    Objects.requireNonNull(
        destinations,
        "destinations must not be null"
    );
    this.destinations = Map.copyOf(destinations);
  }

  public SqsDestination resolve(String target) {
    Objects.requireNonNull(
        target,
        "target must not be null"
    );
    SqsDestination destination = destinations.get(target);
    if (destination == null) {
      throw new IllegalStateException("No SQS destination configured for target '" + target + "'");
    }
    return destination;
  }

  public int size() {
    return destinations.size();
  }
}
