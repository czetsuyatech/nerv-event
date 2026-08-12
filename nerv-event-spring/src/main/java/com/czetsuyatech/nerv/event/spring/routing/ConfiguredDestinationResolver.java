package com.czetsuyatech.nerv.event.spring.routing;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.routing.DestinationResolver;
import com.czetsuyatech.nerv.event.core.routing.DestinationRoute;
import com.czetsuyatech.nerv.event.core.routing.DestinationNotFoundException;
import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves logical destinations from immutable Spring Boot configuration.
 */
@Slf4j
public final class ConfiguredDestinationResolver implements DestinationResolver {

  private final Map<String, DestinationRoute> routes;

  public ConfiguredDestinationResolver(NervEventProperties properties) {
    Objects.requireNonNull(
        properties,
        "properties must not be null"
    ).validate();
    Map<String, DestinationRoute> configuredRoutes = new LinkedHashMap<>();
    properties.getDestinations()
        .forEach(
            (
                name,
                destination) -> {
              DestinationRoute route = new DestinationRoute(
                  new BrokerId(destination.getBroker()),
                  destination.getTarget()
              );
              configuredRoutes.put(
                  name,
                  route
              );
              log.debug(
                  "Registered event destination destination={} broker={} target={}",
                  name,
                  route.brokerId().value(),
                  route.physicalTarget()
              );
            }
        );
    routes = Map.copyOf(configuredRoutes);
    log.info(
        "nerv-event routing initialized destinations={}",
        routes.size()
    );
  }

  @Override
  public DestinationRoute resolve(Destination destination) {
    Destination nonNullDestination = Objects.requireNonNull(
        destination,
        "destination must not be null"
    );
    DestinationRoute route = routes.get(nonNullDestination.name());
    if (route == null) {
      throw new DestinationNotFoundException(nonNullDestination.name());
    }
    log.debug(
        "Resolved event destination destination={} broker={} target={}",
        nonNullDestination.name(),
        route.brokerId().value(),
        route.physicalTarget()
    );
    return route;
  }
}
