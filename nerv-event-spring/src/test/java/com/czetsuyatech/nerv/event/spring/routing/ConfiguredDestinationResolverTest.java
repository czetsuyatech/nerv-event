package com.czetsuyatech.nerv.event.spring.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.core.routing.DestinationRoute;
import com.czetsuyatech.nerv.event.core.routing.DestinationNotFoundException;
import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import org.junit.jupiter.api.Test;

class ConfiguredDestinationResolverTest {

  @Test
  void resolvesTheConfiguredLogicalDestinationWithoutChangingIt() {
    NervEventProperties properties = properties();
    ConfiguredDestinationResolver resolver = new ConfiguredDestinationResolver(properties);
    Destination destination = new Destination("orders");

    DestinationRoute route = resolver.resolve(destination);

    assertThat(destination.name()).isEqualTo("orders");
    assertThat(route.brokerId().value()).isEqualTo("kafka");
    assertThat(route.physicalTarget()).isEqualTo("order-events");
  }

  @Test
  void rejectsAnUnknownLogicalDestinationClearly() {
    ConfiguredDestinationResolver resolver = new ConfiguredDestinationResolver(properties());

    assertThatThrownBy(() -> resolver.resolve(new Destination("payments")))
        .isInstanceOf(DestinationNotFoundException.class)
        .hasMessage("No route configured for destination 'payments'");
  }

  private static NervEventProperties properties() {
    NervEventProperties properties = new NervEventProperties();
    NervEventProperties.DestinationProperties destination = new NervEventProperties.DestinationProperties();
    destination.setBroker("kafka");
    destination.setTarget("order-events");
    properties.getDestinations()
        .put(
            "orders",
            destination
        );
    return properties;
  }
}
