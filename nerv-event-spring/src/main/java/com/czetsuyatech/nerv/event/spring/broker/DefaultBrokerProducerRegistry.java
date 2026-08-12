package com.czetsuyatech.nerv.event.spring.broker;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducerRegistry;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds the core immutable registry from broker producer beans.
 */
@Slf4j
public final class DefaultBrokerProducerRegistry {

  private DefaultBrokerProducerRegistry() {
  }

  public static BrokerProducerRegistry create(Collection<? extends BrokerProducer> producers) {
    return create(
        producers,
        List.of()
    );
  }

  public static BrokerProducerRegistry create(
      Collection<? extends BrokerProducer> producers,
      Collection<? extends BrokerProducerDecorator> decorators
  ) {
    Objects.requireNonNull(
        producers,
        "producers must not be null"
    );
    Objects.requireNonNull(
        decorators,
        "decorators must not be null"
    );
    Map<BrokerId, BrokerProducer> registered = new LinkedHashMap<>();
    for (BrokerProducer producer : producers) {
      BrokerProducer nonNullProducer = Objects.requireNonNull(
          producer,
          "producer must not be null"
      );
      for (BrokerProducerDecorator decorator : decorators) {
        nonNullProducer = Objects.requireNonNull(
            decorator,
            "decorator must not be null"
        )
            .decorate(nonNullProducer);
      }
      BrokerId brokerId = Objects.requireNonNull(
          nonNullProducer.brokerId(),
          "producer brokerId must not be null"
      );
      BrokerProducer existing = registered.putIfAbsent(
          brokerId,
          nonNullProducer
      );
      if (existing != null) {
        throw new IllegalStateException(
            "Duplicate BrokerProducer for broker '" + brokerId.value() + "': "
                + existing.getClass().getName() + " and " + nonNullProducer.getClass().getName()
        );
      }
      log.debug(
          "Registered broker producer broker={} producer={}",
          brokerId.value(),
          nonNullProducer.getClass().getName()
      );
    }
    log.info(
        "nerv-event broker producer registry initialized brokers={}",
        registered.size()
    );
    return new BrokerProducerRegistry(registered.values());
  }
}
