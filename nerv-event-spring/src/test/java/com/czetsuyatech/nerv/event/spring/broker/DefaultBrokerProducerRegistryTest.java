package com.czetsuyatech.nerv.event.spring.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducerNotFoundException;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducerRegistry;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import java.util.List;
import org.junit.jupiter.api.Test;

class DefaultBrokerProducerRegistryTest {

  @Test
  void registersAndResolvesDifferentBrokerProducers() {
    BrokerProducer kafka = producer("kafka");
    BrokerProducer sqs = producer("sqs");

    BrokerProducerRegistry registry = DefaultBrokerProducerRegistry.create(
        List.of(
            kafka,
            sqs
        )
    );

    assertThat(registry.producerFor(new BrokerId("kafka"))).isSameAs(kafka);
    assertThat(registry.producerFor(new BrokerId("sqs"))).isSameAs(sqs);
  }

  @Test
  void permitsAnEmptyRegistryButRejectsUnknownBrokersClearly() {
    BrokerProducerRegistry registry = DefaultBrokerProducerRegistry.create(List.of());

    assertThatThrownBy(() -> registry.producerFor(new BrokerId("kafka")))
        .isInstanceOf(BrokerProducerNotFoundException.class)
        .hasMessage("No BrokerProducer registered for broker 'kafka'");
  }

  @Test
  void rejectsDuplicateBrokerIdsWithProducerTypes() {
    assertThatThrownBy(
        () -> DefaultBrokerProducerRegistry.create(
            List.of(
                producer("kafka"),
                producer("kafka")
            )
        )
    )
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Duplicate BrokerProducer for broker 'kafka'");
  }

  private static BrokerProducer producer(String brokerId) {
    return new BrokerProducer() {
      @Override
      public BrokerId brokerId() {
        return new BrokerId(brokerId);
      }

      @Override
      public BrokerPublishResult publish(BrokerMessage message) {
        return new BrokerPublishResult("published");
      }
    };
  }
}
