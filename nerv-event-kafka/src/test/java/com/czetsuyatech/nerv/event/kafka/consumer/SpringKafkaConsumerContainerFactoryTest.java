package com.czetsuyatech.nerv.event.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.kafka.autoconfigure.NervEventKafkaProperties;
import java.time.Clock;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;

class SpringKafkaConsumerContainerFactoryTest {

  @Test
  void createsAManualAcknowledgementContainerWithTheConfiguredTopicGroupAndConcurrency() {
    NervEventKafkaProperties.Consumer consumer = new NervEventKafkaProperties.Consumer();
    consumer.setTopic("order-events");
    consumer.setGroupId("order-service");
    consumer.setConcurrency(3);
    SpringKafkaConsumerContainerFactory factory = new SpringKafkaConsumerContainerFactory(mock(ConsumerFactory.class));

    MessageListenerContainer result = factory.create(
        "order-created",
        consumer,
        new KafkaConsumerAdapter(
            mock(ConsumerDispatcher.class),
            mock(InboxService.class),
            Clock.systemUTC(),
            "test-owner",
            Duration.ofSeconds(30),
            mock(InboxRetryPolicy.class)
        )
    );

    ConcurrentMessageListenerContainer<?, ?> container = (ConcurrentMessageListenerContainer<?, ?>) result;
    ContainerProperties properties = container.getContainerProperties();
    assertThat(properties.getTopics()).containsExactly("order-events");
    assertThat(properties.getGroupId()).isEqualTo("order-service");
    assertThat(properties.getAckMode()).isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    assertThat(properties.getKafkaConsumerProperties().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
    assertThat(container.getConcurrency()).isEqualTo(3);
    assertThat(container.getCommonErrorHandler()).isNotNull();
  }
}
