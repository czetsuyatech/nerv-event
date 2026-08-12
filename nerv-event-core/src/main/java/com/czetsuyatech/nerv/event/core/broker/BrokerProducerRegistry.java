package com.czetsuyatech.nerv.event.core.broker;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves broker producers by their broker identifier.
 */
@Slf4j
public final class BrokerProducerRegistry {

  private final Map<BrokerId, BrokerProducer> producers;

  public BrokerProducerRegistry(Collection<? extends BrokerProducer> producers) {
    Objects.requireNonNull(
        producers,
        "producers must not be null"
    );
    Map<BrokerId, BrokerProducer> registeredProducers = new LinkedHashMap<>();
    for (BrokerProducer producer : producers) {
      BrokerProducer nonNullProducer = Objects.requireNonNull(
          producer,
          "producer must not be null"
      );
      BrokerId brokerId = Objects.requireNonNull(
          nonNullProducer.brokerId(),
          "producer brokerId must not be null"
      );
      if (registeredProducers.putIfAbsent(brokerId, nonNullProducer) != null) {
        log.error(
            "Duplicate broker producer registration broker={}",
            brokerId.value()
        );
        throw new IllegalArgumentException("Duplicate broker producer for " + brokerId.value());
      }
    }
    this.producers = Map.copyOf(registeredProducers);
  }

  public BrokerProducer producerFor(BrokerId brokerId) {
    Objects.requireNonNull(
        brokerId,
        "brokerId must not be null"
    );
    BrokerProducer producer = producers.get(brokerId);
    if (producer == null) {
      log.error(
          "No broker producer registered broker={}",
          brokerId.value()
      );
      throw new BrokerProducerNotFoundException(brokerId);
    }
    return producer;
  }
}
