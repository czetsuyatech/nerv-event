package com.czetsuyatech.nerv.event.sqs.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.czetsuyatech.nerv.event.sqs.autoconfigure.NervEventSqsProperties;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientId;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistration;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistry;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

class SqsConsumerManagerTest {
  @Test
  void routesEachEnabledConsumerToItsConfiguredClientAndManagesLifecycleOnce() {
    SqsAsyncClient accountA = mock(SqsAsyncClient.class);
    SqsAsyncClient accountB = mock(SqsAsyncClient.class);
    SqsClientRegistry registry = new SqsClientRegistry(
        List.of(
            SqsClientRegistration.applicationProvided(
                new SqsClientId("account-a"),
                accountA
            ),
            SqsClientRegistration.applicationProvided(
                new SqsClientId("account-b"),
                accountB
            )
        )
    );
    NervEventSqsProperties properties = properties();
    SqsConsumerContainerFactory factory = mock(SqsConsumerContainerFactory.class);
    MessageListenerContainer<String> orders = container();
    MessageListenerContainer<String> notifications = container();
    when(
        factory.create(
            org.mockito.ArgumentMatchers.eq("order-events"),
            any(),
            org.mockito.ArgumentMatchers.same(accountA),
            any()
        )
    ).thenReturn(orders);
    when(
        factory.create(
            org.mockito.ArgumentMatchers.eq("notification-events"),
            any(),
            org.mockito.ArgumentMatchers.same(accountB),
            any()
        )
    ).thenReturn(notifications);
    SqsConsumerManager manager = new SqsConsumerManager(
        properties,
        registry,
        factory,
        mock(SqsConsumerAdapter.class)
    );

    manager.start();
    manager.start();
    assertThat(manager.containers()).containsOnlyKeys(
        "order-events",
        "notification-events"
    );
    verify(factory).create(
        org.mockito.ArgumentMatchers.eq("order-events"),
        any(),
        org.mockito.ArgumentMatchers.same(accountA),
        any()
    );
    verify(factory).create(
        org.mockito.ArgumentMatchers.eq("notification-events"),
        any(),
        org.mockito.ArgumentMatchers.same(accountB),
        any()
    );
    verify(orders).start();
    verify(notifications).start();
    manager.stop();
    verify(orders).stop();
    verify(notifications).stop();
  }

  @Test
  void disabledConsumerDoesNotCreateAContainer() {
    NervEventSqsProperties properties = properties();
    properties.getConsumers().get("order-events").setEnabled(false);
    properties.getConsumers().get("notification-events").setEnabled(false);
    SqsConsumerContainerFactory factory = mock(SqsConsumerContainerFactory.class);
    SqsConsumerManager manager = new SqsConsumerManager(
        properties,
        new SqsClientRegistry(List.of()),
        factory,
        mock(SqsConsumerAdapter.class)
    );
    manager.start();
    assertThat(manager.containers()).isEmpty();
    verify(
        factory,
        never()
    ).create(
        anyString(),
        any(),
        any(),
        any()
    );
  }

  @SuppressWarnings("unchecked")
  private static MessageListenerContainer<String> container() {
    return mock(MessageListenerContainer.class);
  }

  private static NervEventSqsProperties properties() {
    NervEventSqsProperties properties = new NervEventSqsProperties();
    properties.setConsumers(new LinkedHashMap<>());
    properties.getConsumers()
        .put(
            "order-events",
            consumer(
                "account-a",
                "orders"
            )
        );
    properties.getConsumers()
        .put(
            "notification-events",
            consumer(
                "account-b",
                "notifications"
            )
        );
    return properties;
  }

  private static NervEventSqsProperties.Consumer consumer(
      String client,
      String queue
  ) {
    NervEventSqsProperties.Consumer consumer = new NervEventSqsProperties.Consumer();
    consumer.setEnabled(true);
    consumer.setClient(client);
    consumer.setQueue(queue);
    return consumer;
  }
}
