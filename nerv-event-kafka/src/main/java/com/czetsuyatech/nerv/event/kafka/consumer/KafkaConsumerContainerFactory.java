package com.czetsuyatech.nerv.event.kafka.consumer;

import com.czetsuyatech.nerv.event.kafka.autoconfigure.NervEventKafkaProperties;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * Creates the Kafka listener container for one configured Nerv Event consumer.
 */
public interface KafkaConsumerContainerFactory {

  MessageListenerContainer create(
      String name,
      NervEventKafkaProperties.Consumer consumer,
      KafkaConsumerAdapter adapter
  );
}
