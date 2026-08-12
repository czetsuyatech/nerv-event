package com.czetsuyatech.nerv.event.sqs.consumer;

import com.czetsuyatech.nerv.event.sqs.autoconfigure.NervEventSqsProperties;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import java.time.Duration;
import java.util.List;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Builds manual, immediate-acknowledgement, long-polling SQS containers.
 */
public final class SpringSqsConsumerContainerFactory implements SqsConsumerContainerFactory {
  @Override
  public MessageListenerContainer<String> create(
      String name,
      NervEventSqsProperties.Consumer consumer,
      SqsAsyncClient client,
      SqsConsumerAdapter adapter
  ) {
    SqsMessageListenerContainer<String> container = SqsMessageListenerContainer.<String>builder()
        .id("nerv-event-sqs-" + name)
        .sqsAsyncClient(client)
        .queueNames(consumer.getQueue())
        .messageListener(adapter::onMessage)
        .configure(
            options -> options
                .autoStartup(false)
                .acknowledgementMode(AcknowledgementMode.MANUAL)
                .acknowledgementInterval(Duration.ZERO)
                .acknowledgementThreshold(0)
                .maxConcurrentMessages(consumer.getMaxConcurrentMessages())
                .maxMessagesPerPoll(consumer.getMaxMessagesPerPoll())
                .pollTimeout(consumer.getPollTimeout())
                .messageVisibility(consumer.getVisibilityTimeout())
                .messageAttributeNames(List.of("All"))
        )
        .build();
    container.setPayloadDeserializationType(String.class);
    return container;
  }
}
