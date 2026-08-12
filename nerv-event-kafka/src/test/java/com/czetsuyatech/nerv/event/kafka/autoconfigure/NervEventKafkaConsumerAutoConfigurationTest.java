package com.czetsuyatech.nerv.event.kafka.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.kafka.consumer.KafkaConsumerAdapter;
import com.czetsuyatech.nerv.event.kafka.consumer.KafkaConsumerManager;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.ConsumerFactory;
import tools.jackson.databind.ObjectMapper;

class NervEventKafkaConsumerAutoConfigurationTest {

  private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(
          AutoConfigurations.of(
              NervEventAutoConfiguration.class,
              NervEventKafkaAutoConfiguration.class,
              NervEventKafkaConsumerAutoConfiguration.class
          )
      )
      .withPropertyValues("nerv.event.kafka.enabled=true");

  @Test
  void createsConsumerInfrastructureWithADispatcherAndConsumerFactory() {
    contextRunner
        .withBean(
            ObjectMapper.class,
            ObjectMapper::new
        )
        .withBean(
            InboxService.class,
            () -> mock(InboxService.class)
        )
        .withBean(
            ConsumerFactory.class,
            () -> mock(ConsumerFactory.class)
        )
        .run(context -> {
          assertThat(context).hasSingleBean(ConsumerDispatcher.class);
          assertThat(context).hasSingleBean(KafkaConsumerAdapter.class);
          assertThat(context).hasSingleBean(KafkaConsumerManager.class);
          assertThat(context.getBean(KafkaConsumerManager.class).containers()).isEmpty();
        });
  }

  @Test
  void doesNotCreateKafkaConsumerInfrastructureWhenInboxRetryIsDisabled() {
    contextRunner
        .withBean(
            ObjectMapper.class,
            ObjectMapper::new
        )
        .withBean(
            InboxService.class,
            () -> mock(InboxService.class)
        )
        .withBean(
            ConsumerFactory.class,
            () -> mock(ConsumerFactory.class)
        )
        .withPropertyValues("nerv.event.inbox.retry.enabled=false")
        .run(context -> {
          assertThat(context).doesNotHaveBean(KafkaConsumerAdapter.class);
          assertThat(context).doesNotHaveBean(KafkaConsumerManager.class);
        });
  }

  @Test
  void allowsMultipleConsumerMetricsBeans() {
    contextRunner
        .withBean(
            ObjectMapper.class,
            ObjectMapper::new
        )
        .withBean(
            InboxService.class,
            () -> mock(InboxService.class)
        )
        .withBean(
            ConsumerFactory.class,
            () -> mock(ConsumerFactory.class)
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
        .run(context -> assertThat(context).hasSingleBean(KafkaConsumerAdapter.class));
  }

  @Test
  void doesNotCreateTheManagerWithoutAConsumerDispatcher() {
    contextRunner
        .withBean(
            ConsumerFactory.class,
            () -> mock(ConsumerFactory.class)
        )
        .run(context -> assertThat(context).doesNotHaveBean(KafkaConsumerManager.class));
  }

  @Test
  void doesNotCreateTheManagerWithoutAConsumerFactory() {
    contextRunner
        .withBean(
            ObjectMapper.class,
            ObjectMapper::new
        )
        .run(context -> assertThat(context).doesNotHaveBean(KafkaConsumerManager.class));
  }

  @Test
  void applicationManagerOverridesTheDefault() {
    KafkaConsumerManager applicationManager = mock(KafkaConsumerManager.class);

    contextRunner
        .withBean(
            KafkaConsumerManager.class,
            () -> applicationManager
        )
        .run(context -> assertThat(context.getBean(KafkaConsumerManager.class)).isSameAs(applicationManager));
  }

  @Test
  void rejectsEnabledConsumersWithoutARequiredTopic() {
    contextRunner
        .withPropertyValues("nerv.event.kafka.consumers.order-created.enabled=true")
        .run(
            context -> assertThat(context).hasFailed()
                .getFailure()
                .hasMessageContaining("order-created.topic must not be blank when enabled")
        );
  }

  @Test
  void bindsEnabledConsumerDefinitionsWithoutRequiringAHandlerBeanName() {
    contextRunner
        .withPropertyValues(
            "nerv.event.kafka.consumers.order-created.enabled=true",
            "nerv.event.kafka.consumers.order-created.topic=order-events",
            "nerv.event.kafka.consumers.order-created.group-id=order-service",
            "nerv.event.kafka.consumers.order-created.concurrency=3"
        )
        .run(context -> {
          NervEventKafkaProperties.Consumer consumer = context.getBean(NervEventKafkaProperties.class)
              .getConsumers()
              .get("order-created");
          assertThat(consumer.getTopic()).isEqualTo("order-events");
          assertThat(consumer.getGroupId()).isEqualTo("order-service");
          assertThat(consumer.getConcurrency()).isEqualTo(3);
        });
  }
}
