package com.czetsuyatech.nerv.event.sqs.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.czetsuyatech.nerv.event.sqs.autoconfigure.NervEventSqsProperties;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

class SpringSqsConsumerContainerFactoryTest {
  @Test
  void appliesLongPollingAndVisibilityDefaults() {
    NervEventSqsProperties.Consumer consumer = new NervEventSqsProperties.Consumer();

    assertThat(consumer.getPollTimeout()).isEqualTo(Duration.ofSeconds(20));
    assertThat(consumer.getVisibilityTimeout()).isEqualTo(Duration.ofMinutes(2));
  }

  @Test
  void configuresManualImmediateAckLongPollingConcurrencyAndVisibility() {
    NervEventSqsProperties.Consumer consumer = new NervEventSqsProperties.Consumer();
    consumer.setQueue("orders");
    consumer.setMaxConcurrentMessages(7);
    consumer.setMaxMessagesPerPoll(5);
    consumer.setPollTimeout(Duration.ofSeconds(12));
    consumer.setVisibilityTimeout(Duration.ofSeconds(45));
    SqsMessageListenerContainer<String> container = (SqsMessageListenerContainer<String>) new SpringSqsConsumerContainerFactory()
        .create(
            "orders",
            consumer,
            mock(SqsAsyncClient.class),
            mock(SqsConsumerAdapter.class)
        );

    assertThat(container.getQueueNames()).containsExactly("orders");
    assertThat(container.getContainerOptions().getAcknowledgementMode())
        .isEqualTo(AcknowledgementMode.MANUAL);
    assertThat(container.getContainerOptions().getAcknowledgementInterval()).isEqualTo(Duration.ZERO);
    assertThat(container.getContainerOptions().getAcknowledgementThreshold()).isZero();
    assertThat(container.getContainerOptions().getMaxConcurrentMessages()).isEqualTo(7);
    assertThat(container.getContainerOptions().getMaxMessagesPerPoll()).isEqualTo(5);
    assertThat(container.getContainerOptions().getPollTimeout()).isEqualTo(Duration.ofSeconds(12));
    assertThat(container.getContainerOptions().getMessageVisibility()).isEqualTo(Duration.ofSeconds(45));
    assertThat(container.getContainerOptions().getMessageAttributeNames()).contains("All");
    assertThat(container.isAutoStartup()).isFalse();
  }
}
