package com.czetsuyatech.nerv.event.sqs.consumer;

import com.czetsuyatech.nerv.event.sqs.autoconfigure.NervEventSqsProperties;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Creates one programmatic Spring Cloud AWS container.
 */
public interface SqsConsumerContainerFactory {
  MessageListenerContainer<String> create(
      String name,
      NervEventSqsProperties.Consumer consumer,
      SqsAsyncClient client,
      SqsConsumerAdapter adapter
  );
}
