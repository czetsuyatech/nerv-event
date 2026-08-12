package com.czetsuyatech.nerv.event.sqs.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.sqs.consumer.SqsConsumerContainerFactory;
import com.czetsuyatech.nerv.event.sqs.consumer.SqsConsumerManager;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventAutoConfiguration;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

class NervEventSqsConsumerAutoConfigurationTest {
  private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(
          AutoConfigurations.of(
              NervEventSqsAutoConfiguration.class,
              NervEventAutoConfiguration.class,
              NervEventSqsConsumerAutoConfiguration.class
          )
      )
      .withPropertyValues("nerv.event.sqs.enabled=true");

  @Test
  void configuresAndStartsConsumerWhenGenericDependenciesExist() {
    SqsConsumerContainerFactory factory = factory();
    contextRunner.withBean(
        SqsAsyncClient.class,
        () -> mock(SqsAsyncClient.class)
    )
        .withBean(
            ConsumerDispatcher.class,
            () -> mock(ConsumerDispatcher.class)
        )
        .withBean(
            InboxService.class,
            () -> mock(InboxService.class)
        )
        .withBean(
            InboxRetryPolicy.class,
            () -> mock(InboxRetryPolicy.class)
        )
        .withBean(
            SqsConsumerContainerFactory.class,
            () -> factory
        )
        .withPropertyValues(
            "nerv.event.sqs.consumers.orders.enabled=true",
            "nerv.event.sqs.consumers.orders.client=default",
            "nerv.event.sqs.consumers.orders.queue=orders",
            "nerv.event.sqs.consumers.orders.max-concurrent-messages=4",
            "nerv.event.sqs.consumers.orders.max-messages-per-poll=2",
            "nerv.event.sqs.consumers.orders.poll-timeout=8s",
            "nerv.event.sqs.consumers.orders.visibility-timeout=45s"
        )
        .run(context -> {
          assertThat(context).hasSingleBean(SqsConsumerManager.class);
          assertThat(context.getBean(SqsConsumerManager.class).containers()).containsOnlyKeys("orders");
        });
  }

  @Test
  void doesNotCreateManagerWithoutDispatcherOrInboxRepository() {
    contextRunner.withBean(
        SqsAsyncClient.class,
        () -> mock(SqsAsyncClient.class)
    )
        .withBean(
            InboxRetryPolicy.class,
            () -> mock(InboxRetryPolicy.class)
        )
        .run(context -> assertThat(context).doesNotHaveBean(SqsConsumerManager.class));
  }

  @Test
  void allowsMultipleConsumerMetricsBeans() {
    contextRunner.withBean(
        SqsAsyncClient.class,
        () -> mock(SqsAsyncClient.class)
    )
        .withBean(
            ConsumerDispatcher.class,
            () -> mock(ConsumerDispatcher.class)
        )
        .withBean(
            InboxService.class,
            () -> mock(InboxService.class)
        )
        .withBean(
            InboxRetryPolicy.class,
            () -> mock(InboxRetryPolicy.class)
        )
        .withBean(
            "firstConsumerMetrics",
            ConsumerMetrics.class,
            ConsumerMetrics::noop
        )
        .withBean(
            "secondConsumerMetrics",
            ConsumerMetrics.class,
            ConsumerMetrics::noop
        )
        .run(context -> assertThat(context).hasSingleBean(SqsConsumerManager.class));
  }

  @Test
  void unknownConsumerClientFailsAtStartup() {
    contextRunner.withBean(
        SqsAsyncClient.class,
        () -> mock(SqsAsyncClient.class)
    )
        .withBean(
            ConsumerDispatcher.class,
            () -> mock(ConsumerDispatcher.class)
        )
        .withBean(
            InboxService.class,
            () -> mock(InboxService.class)
        )
        .withBean(
            InboxRetryPolicy.class,
            () -> mock(InboxRetryPolicy.class)
        )
        .withBean(
            SqsConsumerContainerFactory.class,
            NervEventSqsConsumerAutoConfigurationTest::factory
        )
        .withPropertyValues(
            "nerv.event.sqs.consumers.orders.enabled=true",
            "nerv.event.sqs.consumers.orders.client=account-a",
            "nerv.event.sqs.consumers.orders.queue=orders"
        )
        .run(
            context -> assertThat(context).hasFailed()
                .getFailure()
                .hasMessageContaining("Invalid SQS consumer 'orders': client 'account-a' is not configured")
        );
  }

  @Test
  void applicationProvidedManagerOverridesDefault() {
    SqsConsumerManager manager = mock(SqsConsumerManager.class);
    contextRunner.withBean(
        SqsConsumerManager.class,
        () -> manager
    )
        .run(context -> assertThat(context.getBean(SqsConsumerManager.class)).isSameAs(manager));
  }

  @Test
  void invalidEnabledConsumerConfigurationFailsFast() {
    contextRunner.withBean(
        SqsAsyncClient.class,
        () -> mock(SqsAsyncClient.class)
    )
        .withPropertyValues(
            "nerv.event.sqs.consumers.orders.enabled=true",
            "nerv.event.sqs.consumers.orders.client=default",
            "nerv.event.sqs.consumers.orders.queue=orders",
            "nerv.event.sqs.consumers.orders.max-messages-per-poll=11"
        )
        .run(
            context -> assertThat(context).hasFailed()
                .getFailure()
                .hasMessageContaining("Invalid SQS consumer 'orders': max-messages-per-poll")
        );
  }

  @SuppressWarnings("unchecked")
  private static SqsConsumerContainerFactory factory() {
    SqsConsumerContainerFactory factory = mock(SqsConsumerContainerFactory.class);
    when(
        factory.create(
            anyString(),
            any(),
            any(),
            any()
        )
    )
        .thenReturn(mock(MessageListenerContainer.class));
    return factory;
  }
}
