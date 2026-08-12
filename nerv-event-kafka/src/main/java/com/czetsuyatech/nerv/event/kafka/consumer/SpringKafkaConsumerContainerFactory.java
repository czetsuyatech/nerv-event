package com.czetsuyatech.nerv.event.kafka.consumer;

import com.czetsuyatech.nerv.event.kafka.autoconfigure.NervEventKafkaProperties;
import java.util.Properties;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Builds explicit-manual-acknowledgement listener containers from Boot's consumer factory.
 */
@RequiredArgsConstructor
public class SpringKafkaConsumerContainerFactory implements KafkaConsumerContainerFactory {

  @NonNull
  private final ConsumerFactory<String, String> consumerFactory;

  @Override
  public MessageListenerContainer create(
      String name,
      NervEventKafkaProperties.Consumer consumer,
      KafkaConsumerAdapter adapter
  ) {
    ContainerProperties containerProperties = new ContainerProperties(consumer.getTopic());
    containerProperties.setGroupId(consumer.getGroupId());
    containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    containerProperties.setKafkaConsumerProperties(manualCommitProperties());
    containerProperties.setMessageListener(adapter);
    ConcurrentMessageListenerContainer<String, String> container = new ConcurrentMessageListenerContainer<>(
        consumerFactory,
        containerProperties
    );
    container.setBeanName("nerv-event-kafka-" + name);
    container.setConcurrency(consumer.getConcurrency());
    container.setCommonErrorHandler(nonCommittingNoRetryErrorHandler());
    return container;
  }

  private static Properties manualCommitProperties() {
    Properties properties = new Properties();
    properties.put(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        false
    );
    return properties;
  }

  /**
   * Leaves unresolved records uncommitted and performs no Kafka business-processing retries.
   */
  private static DefaultErrorHandler nonCommittingNoRetryErrorHandler() {
    DefaultErrorHandler errorHandler = new DefaultErrorHandler(
        new FixedBackOff(
            0L,
            0L
        )
    );
    errorHandler.setAckAfterHandle(false);
    errorHandler.setCommitRecovered(false);
    return errorHandler;
  }
}
