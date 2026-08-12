package com.czetsuyatech.nerv.event.spring.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class DispatcherOwnerResolverTest {

  @Test
  void prefersTheConfiguredOwnerThenTheKubernetesPodName() {
    NervEventProperties.Dispatcher properties = new NervEventProperties.Dispatcher();
    MockEnvironment environment = new MockEnvironment().withProperty(
        "POD_NAME",
        "orders-6f46c"
    );

    assertThat(
        DispatcherOwnerResolver.resolve(
            properties,
            environment
        )
    ).isEqualTo("orders-6f46c");

    properties.setOwner("orders-worker");
    assertThat(
        DispatcherOwnerResolver.resolve(
            properties,
            environment
        )
    ).isEqualTo("orders-worker");
  }

  @Test
  void generatesAReadableFallbackUsingTheApplicationName() {
    NervEventProperties.Dispatcher properties = new NervEventProperties.Dispatcher();
    MockEnvironment environment = new MockEnvironment().withProperty(
        "spring.application.name",
        "orders"
    );

    assertThat(
        DispatcherOwnerResolver.resolve(
            properties,
            environment
        )
    ).startsWith("orders-");
  }
}
