package com.czetsuyatech.nerv.event.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.kafka.autoconfigure.NervEventKafkaProperties;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.MessageListenerContainer;

class KafkaConsumerManagerTest {

  @Test
  void startsOneContainerForAnEnabledConfiguredConsumerAndDoesNotDuplicateIt() {
    NervEventKafkaProperties properties = properties(true);
    KafkaConsumerContainerFactory factory = mock(KafkaConsumerContainerFactory.class);
    MessageListenerContainer container = mock(MessageListenerContainer.class);
    KafkaConsumerAdapter adapter = adapter();
    when(
        factory.create(
            eq("order-created"),
            any(),
            any(KafkaConsumerAdapter.class)
        )
    ).thenReturn(container);
    KafkaConsumerManager manager = new KafkaConsumerManager(
        properties,
        factory,
        adapter
    );

    manager.start();
    manager.start();

    verify(
        factory,
        times(1)
    ).create(
        eq("order-created"),
        any(),
        any(KafkaConsumerAdapter.class)
    );
    verify(
        container,
        times(1)
    ).start();
    assertThat(manager.containers()).containsEntry(
        "order-created",
        container
    );
    assertThat(manager.isRunning()).isTrue();
  }

  @Test
  void doesNotCreateContainersForDisabledConsumers() {
    NervEventKafkaProperties properties = properties(false);
    KafkaConsumerContainerFactory factory = mock(KafkaConsumerContainerFactory.class);
    KafkaConsumerManager manager = new KafkaConsumerManager(
        properties,
        factory,
        adapter()
    );

    manager.start();

    verify(
        factory,
        org.mockito.Mockito.never()
    ).create(
        any(),
        any(),
        any()
    );
    assertThat(manager.containers()).isEmpty();
  }

  @Test
  void stopsAllManagedContainersOnShutdown() {
    NervEventKafkaProperties properties = properties(true);
    KafkaConsumerContainerFactory factory = mock(KafkaConsumerContainerFactory.class);
    MessageListenerContainer container = mock(MessageListenerContainer.class);
    KafkaConsumerAdapter adapter = adapter();
    when(
        factory.create(
            any(),
            any(),
            any(KafkaConsumerAdapter.class)
        )
    ).thenReturn(container);
    KafkaConsumerManager manager = new KafkaConsumerManager(
        properties,
        factory,
        adapter
    );
    manager.start();

    manager.stop();

    verify(container).stop();
    assertThat(manager.isRunning()).isFalse();
  }

  private static NervEventKafkaProperties properties(boolean enabled) {
    NervEventKafkaProperties.Consumer consumer = new NervEventKafkaProperties.Consumer();
    consumer.setEnabled(enabled);
    consumer.setTopic("order-events");
    consumer.setGroupId("order-service");
    consumer.setConcurrency(3);
    NervEventKafkaProperties properties = new NervEventKafkaProperties();
    properties.getConsumers()
        .put(
            "order-created",
            consumer
        );
    return properties;
  }

  private static KafkaConsumerAdapter adapter() {
    return new KafkaConsumerAdapter(
        mock(ConsumerDispatcher.class),
        mock(InboxService.class),
        Clock.systemUTC(),
        "test-owner",
        Duration.ofSeconds(30),
        mock(InboxRetryPolicy.class)
    );
  }
}
