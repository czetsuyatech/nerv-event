package com.czetsuyatech.nerv.event.spring.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.broker.BrokerProducerRegistry;
import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.outbox.OutboxDispatcher;
import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import com.czetsuyatech.nerv.event.core.inbox.ExponentialInboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryDispatcher;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.EventHandlerRegistry;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.core.retry.RetryPolicy;
import com.czetsuyatech.nerv.event.core.retention.EventRetention;
import com.czetsuyatech.nerv.event.core.routing.DestinationResolver;
import com.czetsuyatech.nerv.event.core.serialization.EventSerializer;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventPublication;
import com.czetsuyatech.nerv.event.publisher.EventPublisher;
import com.czetsuyatech.nerv.event.spring.dispatcher.OutboxDispatchScheduler;
import com.czetsuyatech.nerv.event.spring.dispatcher.InboxRetryScheduler;
import com.czetsuyatech.nerv.event.spring.dispatcher.DispatcherPollingPolicy;
import com.czetsuyatech.nerv.event.spring.publisher.DefaultEventPublisher;
import com.czetsuyatech.nerv.event.spring.routing.ConfiguredDestinationResolver;
import com.czetsuyatech.nerv.event.spring.retention.EventRetentionScheduler;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;

class NervEventAutoConfigurationTest {
  private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(NervEventAutoConfiguration.class));

  @Test
  void autoConfiguresEventPublisherWhenAnOutboxRepositoryExists() {
    contextRunner.withUserConfiguration(OutboxRepositoryConfiguration.class).run(context -> {
      assertThat(context).hasSingleBean(EventPublisher.class);
      assertThat(context.getBean(EventPublisher.class)).isInstanceOf(DefaultEventPublisher.class);
    });
  }

  @Test
  void doesNotCreateEventPublisherWithoutAnOutboxRepository() {
    contextRunner.run(context -> assertThat(context).doesNotHaveBean(EventPublisher.class));
  }

  @Test
  void doesNotStartRetentionUnlessExplicitlyEnabled() {
    contextRunner.withBean(
        EventRetention.class,
        NervEventAutoConfigurationTest::eventRetention
    ).run(context -> assertThat(context).doesNotHaveBean(EventRetentionScheduler.class));
  }

  @Test
  void applicationEventPublisherOverridesTheDefault() {
    contextRunner.withUserConfiguration(OverrideEventPublisherConfiguration.class).run(context -> {
      assertThat(context).hasSingleBean(EventPublisher.class);
      assertThat(context.getBean(EventPublisher.class)).isSameAs(context.getBean("applicationEventPublisher"));
    });
  }

  private static EventRetention eventRetention() {
    return (EventRetention) Proxy.newProxyInstance(
        NervEventAutoConfigurationTest.class.getClassLoader(),
        new Class<?>[]{EventRetention.class},
        (proxy, method, arguments) -> null
    );
  }

  @Test
  void applicationClockOverridesTheDefaultClock() {
    contextRunner.withUserConfiguration(ClockConfiguration.class)
        .run(
            context -> assertThat(context.getBean(Clock.class)).isSameAs(ClockConfiguration.CLOCK)
        );
  }

  @Test
  void defaultClockUsesUtc() {
    contextRunner.run(context -> assertThat(context.getBean(Clock.class).getZone()).isEqualTo(ZoneOffset.UTC));
  }

  @Test
  void autoConfigurationIsRegisteredForBootDiscovery() {
    assertThat(
        getClass().getClassLoader()
            .getResource(
                "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports"
            )
    ).isNotNull();
  }

  @Test
  void bindsDispatcherPropertiesAndDefaults() {
    contextRunner.withPropertyValues(
        "nerv.event.dispatcher.batch-size=25",
        "nerv.event.dispatcher.polling.min-interval=2s",
        "nerv.event.dispatcher.polling.max-interval=45s",
        "nerv.event.dispatcher.polling.multiplier=3.0",
        "nerv.event.dispatcher.polling.jitter=0.2",
        "nerv.event.dispatcher.lease-duration=45s",
        "nerv.event.dispatcher.owner=orders-worker"
    ).run(context -> {
      NervEventProperties.Dispatcher dispatcher = context.getBean(NervEventProperties.class).getDispatcher();

      assertThat(dispatcher.isEnabled()).isTrue();
      assertThat(dispatcher.getBatchSize()).isEqualTo(25);
      assertThat(dispatcher.getPolling().getMinInterval()).isEqualTo(java.time.Duration.ofSeconds(2));
      assertThat(dispatcher.getPolling().getMaxInterval()).isEqualTo(java.time.Duration.ofSeconds(45));
      assertThat(dispatcher.getPolling().getMultiplier()).isEqualTo(3.0);
      assertThat(dispatcher.getPolling().getJitter()).isEqualTo(0.2);
      assertThat(dispatcher.getLeaseDuration()).isEqualTo(java.time.Duration.ofSeconds(45));
      assertThat(dispatcher.getOwner()).isEqualTo("orders-worker");
    });
  }

  @Test
  void appliesDispatcherDefaults() {
    contextRunner.run(context -> {
      NervEventProperties.Dispatcher dispatcher = context.getBean(NervEventProperties.class).getDispatcher();

      assertThat(dispatcher.isEnabled()).isTrue();
      assertThat(dispatcher.getBatchSize()).isEqualTo(100);
      assertThat(dispatcher.getPolling().getMinInterval()).isEqualTo(java.time.Duration.ofSeconds(1));
      assertThat(dispatcher.getPolling().getMaxInterval()).isEqualTo(java.time.Duration.ofSeconds(30));
      assertThat(dispatcher.getPolling().getMultiplier()).isEqualTo(2.0);
      assertThat(dispatcher.getPolling().getJitter()).isEqualTo(0.10);
      assertThat(dispatcher.getLeaseDuration()).isEqualTo(java.time.Duration.ofMinutes(2));
      assertThat(dispatcher.getOwner()).isNull();
    });
  }

  @Test
  void appliesInboxRetryDefaultsAndAutoConfiguresThePolicy() {
    contextRunner.run(context -> {
      NervEventProperties.Inbox.Retry retry = context.getBean(NervEventProperties.class)
          .getInbox()
          .getRetry();

      assertThat(retry.isEnabled()).isTrue();
      assertThat(retry.getMaxAttempts()).isEqualTo(5);
      assertThat(retry.getInitialDelay()).isEqualTo(Duration.ofSeconds(5));
      assertThat(retry.getMultiplier()).isEqualTo(2.0);
      assertThat(retry.getMaxDelay()).isEqualTo(Duration.ofMinutes(10));
      assertThat(context).hasSingleBean(InboxRetryPolicy.class);
      assertThat(context.getBean(InboxRetryPolicy.class)).isInstanceOf(ExponentialInboxRetryPolicy.class);
    });
  }

  @Test
  void bindsInboxRetryPropertiesIntoTheDefaultPolicy() {
    contextRunner.withPropertyValues(
        "nerv.event.inbox.retry.max-attempts=3",
        "nerv.event.inbox.retry.initial-delay=2s",
        "nerv.event.inbox.retry.multiplier=3.0",
        "nerv.event.inbox.retry.max-delay=20s"
    ).run(context -> {
      ExponentialInboxRetryPolicy policy = (ExponentialInboxRetryPolicy) context.getBean(
          InboxRetryPolicy.class
      );

      assertThat(policy.getMaxAttempts()).isEqualTo(3);
      assertThat(policy.getInitialDelay()).isEqualTo(Duration.ofSeconds(2));
      assertThat(policy.getMultiplier()).isEqualTo(3.0);
      assertThat(policy.getMaxDelay()).isEqualTo(Duration.ofSeconds(20));
    });
  }

  @Test
  void bindsAndValidatesInboxRetryDispatcherProperties() {
    contextRunner.withPropertyValues(
        "nerv.event.inbox.dispatcher.batch-size=25",
        "nerv.event.inbox.dispatcher.lease-duration=45s",
        "nerv.event.inbox.dispatcher.polling.min-interval=2s",
        "nerv.event.inbox.dispatcher.polling.max-interval=45s",
        "nerv.event.inbox.dispatcher.polling.multiplier=3.0",
        "nerv.event.inbox.dispatcher.polling.jitter=0.2",
        "nerv.event.inbox.dispatcher.owner=orders-worker"
    ).run(context -> {
      NervEventProperties.Inbox.Dispatcher dispatcher = context.getBean(NervEventProperties.class)
          .getInbox()
          .getDispatcher();
      assertThat(dispatcher.getBatchSize()).isEqualTo(25);
      assertThat(dispatcher.getLeaseDuration()).isEqualTo(Duration.ofSeconds(45));
      assertThat(dispatcher.getPolling().getMinInterval()).isEqualTo(Duration.ofSeconds(2));
      assertThat(dispatcher.getPolling().getMaxInterval()).isEqualTo(Duration.ofSeconds(45));
      assertThat(dispatcher.getPolling().getMultiplier()).isEqualTo(3.0);
      assertThat(dispatcher.getPolling().getJitter()).isEqualTo(0.2);
      assertThat(dispatcher.getOwner()).isEqualTo("orders-worker");
    });
    contextRunner.withPropertyValues("nerv.event.inbox.dispatcher.batch-size=0")
        .run(
            context -> assertThat(context.getStartupFailure()).hasMessageContaining("inbox.dispatcher.batch-size")
        );
  }

  @Test
  void rejectsInvalidInboxRetryPropertiesAtStartup() {
    contextRunner.withPropertyValues("nerv.event.inbox.retry.max-attempts=0")
        .run(
            context -> assertThat(context.getStartupFailure()).hasMessageContaining("retry.max-attempts")
        );
    contextRunner.withPropertyValues("nerv.event.inbox.retry.initial-delay=PT0S")
        .run(
            context -> assertThat(context.getStartupFailure()).hasMessageContaining("retry.initial-delay")
        );
    contextRunner.withPropertyValues("nerv.event.inbox.retry.multiplier=0.9")
        .run(
            context -> assertThat(context.getStartupFailure()).hasMessageContaining("retry.multiplier")
        );
    contextRunner.withPropertyValues(
        "nerv.event.inbox.retry.initial-delay=10s",
        "nerv.event.inbox.retry.max-delay=1s"
    )
        .run(
            context -> assertThat(context.getStartupFailure())
                .hasMessageContaining("retry.max-delay must be greater than or equal")
        );
  }

  @Test
  void doesNotAutoConfigureTheDefaultPolicyWhenInboxRetryIsDisabled() {
    contextRunner.withPropertyValues("nerv.event.inbox.retry.enabled=false")
        .run(
            context -> assertThat(context).doesNotHaveBean(InboxRetryPolicy.class)
        );
  }

  @Test
  void customInboxRetryPolicyOverridesTheDefault() {
    contextRunner.withUserConfiguration(CustomInboxRetryPolicyConfiguration.class).run(context -> {
      assertThat(context).hasSingleBean(InboxRetryPolicy.class);
      assertThat(context.getBean(InboxRetryPolicy.class))
          .isSameAs(context.getBean("customInboxRetryPolicy"));
    });
  }

  @Test
  void autoConfiguresGenericInboxRetryInfrastructureWithoutKafkaOrSqs() {
    contextRunner.withUserConfiguration(InboxRetryDependenciesConfiguration.class).run(context -> {
      assertThat(context).hasSingleBean(InboxRetryDispatcher.class);
      assertThat(context).hasSingleBean(InboxRetryScheduler.class);
      assertThat(context.getBean(InboxRetryDispatcher.class).getOwner()).endsWith(":inbox-retry");
    });
  }

  @Test
  void customInboxRetryDispatcherOverridesTheDefault() {
    contextRunner.withUserConfiguration(CustomInboxRetryDispatcherConfiguration.class).run(context -> {
      assertThat(context).hasSingleBean(InboxRetryDispatcher.class);
      assertThat(context.getBean(InboxRetryDispatcher.class))
          .isSameAs(context.getBean("applicationInboxRetryDispatcher"));
    });
  }

  @Test
  void doesNotScheduleInboxRetriesWhenTheDispatcherOrRetriesAreDisabled() {
    contextRunner.withUserConfiguration(InboxRetryDependenciesConfiguration.class)
        .withPropertyValues("nerv.event.inbox.dispatcher.enabled=false")
        .run(context -> assertThat(context).doesNotHaveBean(InboxRetryScheduler.class));
    contextRunner.withUserConfiguration(InboxRetryDependenciesConfiguration.class)
        .withPropertyValues("nerv.event.inbox.retry.enabled=false")
        .run(context -> {
          assertThat(context).doesNotHaveBean(InboxRetryPolicy.class);
          assertThat(context).doesNotHaveBean(InboxRetryDispatcher.class);
          assertThat(context).doesNotHaveBean(InboxRetryScheduler.class);
        });
  }

  @Test
  void rejectsInvalidDispatcherPropertiesAtStartup() {
    contextRunner.withPropertyValues("nerv.event.dispatcher.batch-size=0").run(context -> {
      assertThat(context.getStartupFailure()).hasMessageContaining("batch-size must be greater than zero");
    });
    contextRunner.withPropertyValues("nerv.event.dispatcher.polling.min-interval=PT0S").run(context -> {
      assertThat(context.getStartupFailure()).hasMessageContaining("min-interval must be greater than zero");
    });
    contextRunner.withPropertyValues(
        "nerv.event.dispatcher.polling.min-interval=30s",
        "nerv.event.dispatcher.polling.max-interval=1s"
    ).run(context -> {
      assertThat(context.getStartupFailure()).hasMessageContaining("max-interval must be greater than or equal");
    });
    contextRunner.withPropertyValues("nerv.event.dispatcher.polling.multiplier=0.9").run(context -> {
      assertThat(context.getStartupFailure()).hasMessageContaining("multiplier must be at least 1.0");
    });
    contextRunner.withPropertyValues("nerv.event.dispatcher.polling.jitter=1.1").run(context -> {
      assertThat(context.getStartupFailure()).hasMessageContaining("jitter must be between 0.0 and 1.0");
    });
    contextRunner.withPropertyValues("nerv.event.dispatcher.lease-duration=PT0S").run(context -> {
      assertThat(context.getStartupFailure()).hasMessageContaining("lease-duration must be greater than zero");
    });
  }

  @Test
  void bindsAndValidatesDestinationConfiguration() {
    contextRunner.withPropertyValues(
        "nerv.event.destinations.orders.broker=kafka",
        "nerv.event.destinations.orders.target=order-events",
        "nerv.event.destinations.notifications.broker=sqs",
        "nerv.event.destinations.notifications.target=notification-events"
    ).run(context -> {
      assertThat(context.getBean(NervEventProperties.class).getDestinations())
          .hasSize(2)
          .containsKeys(
              "orders",
              "notifications"
          );
      assertThat(context.getBean(NervEventProperties.class).getDestinations().get("orders").getBroker())
          .isEqualTo("kafka");
    });
    contextRunner.withPropertyValues("nerv.event.destinations.orders.target=order-events")
        .run(
            context -> assertThat(context.getStartupFailure())
                .hasMessageContaining("destination='orders', broker must not be blank")
        );
    contextRunner.withPropertyValues(
        "nerv.event.destinations.orders.broker= ",
        "nerv.event.destinations.orders.target=order-events"
    )
        .run(
            context -> assertThat(context.getStartupFailure())
                .hasMessageContaining("destination='orders', broker must not be blank")
        );
    contextRunner.withPropertyValues("nerv.event.destinations.orders.broker=kafka")
        .run(
            context -> assertThat(context.getStartupFailure())
                .hasMessageContaining("destination='orders', target must not be blank")
        );
    contextRunner.withPropertyValues(
        "nerv.event.destinations.orders.broker=kafka",
        "nerv.event.destinations.orders.target= "
    )
        .run(
            context -> assertThat(context.getStartupFailure())
                .hasMessageContaining("destination='orders', target must not be blank")
        );
  }

  @Test
  void autoConfiguresRoutingAndProducerRegistryButAllowsNoProducers() {
    contextRunner.run(context -> {
      assertThat(context).hasSingleBean(DestinationResolver.class);
      assertThat(context.getBean(DestinationResolver.class)).isInstanceOf(ConfiguredDestinationResolver.class);
      assertThat(context).hasSingleBean(BrokerProducerRegistry.class);
    });
  }

  @Test
  void customRoutingAndRegistryBeansOverrideTheDefaults() {
    contextRunner.withUserConfiguration(CustomRoutingConfiguration.class).run(context -> {
      assertThat(context.getBean(DestinationResolver.class))
          .isSameAs(context.getBean("customDestinationResolver"));
      assertThat(context.getBean(BrokerProducerRegistry.class))
          .isSameAs(context.getBean("customBrokerProducerRegistry"));
    });
  }

  @Test
  void registersAvailableProducerBeansAndValidatesEnabledDispatcherRoutes() {
    contextRunner.withUserConfiguration(
        DispatcherConfiguration.class,
        KafkaProducerConfiguration.class
    )
        .withPropertyValues(
            "nerv.event.destinations.orders.broker=kafka",
            "nerv.event.destinations.orders.target=order-events"
        )
        .run(
            context -> assertThat(
                context.getBean(BrokerProducerRegistry.class)
                    .producerFor(new BrokerId("kafka"))
            ).isNotNull()
        );

    contextRunner.withUserConfiguration(DispatcherConfiguration.class)
        .withPropertyValues(
            "nerv.event.destinations.orders.broker=kafka",
            "nerv.event.destinations.orders.target=order-events"
        )
        .run(
            context -> assertThat(context.getStartupFailure())
                .hasMessageContaining("destination 'orders' requires no registered BrokerProducer for broker 'kafka'")
        );

    contextRunner.withUserConfiguration(DispatcherConfiguration.class)
        .withPropertyValues(
            "nerv.event.dispatcher.enabled=false",
            "nerv.event.destinations.orders.broker=kafka",
            "nerv.event.destinations.orders.target=order-events"
        )
        .run(context -> assertThat(context.getStartupFailure()).isNull());
  }

  @Test
  void createsSchedulerOnlyWhenEnabledAndADispatcherExists() {
    contextRunner.withUserConfiguration(DispatcherConfiguration.class)
        .run(
            context -> assertThat(context).hasSingleBean(OutboxDispatchScheduler.class)
        );
  }

  @Test
  void autoConfiguresTheCoreDispatcherWhenAllOfItsDependenciesExist() {
    contextRunner.withUserConfiguration(CoreDispatcherDependenciesConfiguration.class).run(context -> {
      assertThat(context).hasSingleBean(OutboxDispatcher.class);
      assertThat(context).hasSingleBean(OutboxDispatchScheduler.class);
    });
  }

  @Test
  void doesNotCreateSchedulerWhenDisabledOrWithoutADispatcher() {
    contextRunner.withPropertyValues("nerv.event.dispatcher.enabled=false")
        .withUserConfiguration(DispatcherConfiguration.class)
        .run(context -> assertThat(context).doesNotHaveBean(OutboxDispatchScheduler.class));
    contextRunner.run(context -> assertThat(context).doesNotHaveBean(OutboxDispatchScheduler.class));
  }

  @Test
  void applicationSchedulerOverridesTheDefault() {
    contextRunner.withUserConfiguration(OverrideSchedulerConfiguration.class).run(context -> {
      assertThat(context).hasSingleBean(OutboxDispatchScheduler.class);
      assertThat(context.getBean(OutboxDispatchScheduler.class))
          .isSameAs(context.getBean("applicationOutboxDispatchScheduler"));
    });
  }

  @Configuration(proxyBeanMethods = false)
  static class OutboxRepositoryConfiguration {
    @Bean
    OutboxService outboxRepository() {
      return new NoOpOutboxService();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class OverrideEventPublisherConfiguration {
    @Bean
    OutboxService outboxRepository() {
      return new NoOpOutboxService();
    }

    @Bean
    EventPublisher applicationEventPublisher() {
      return new EventPublisher() {
        @Override
        public <T> EventId publish(EventPublication<T> publication) {
          return publication.event().id();
        }
      };
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ClockConfiguration {
    static final Clock CLOCK = Clock.fixed(
        Instant.EPOCH,
        ZoneOffset.UTC
    );

    @Bean
    Clock clock() {
      return CLOCK;
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class CustomInboxRetryPolicyConfiguration {
    @Bean
    InboxRetryPolicy customInboxRetryPolicy() {
      return new InboxRetryPolicy() {
        @Override
        public boolean canRetry(int attemptCount) {
          return false;
        }

        @Override
        public Instant nextAttemptAt(
            int attemptCount,
            Instant failedAt
        ) {
          return failedAt;
        }
      };
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class InboxRetryDependenciesConfiguration {
    @Bean
    InboxService inboxRepository() {
      return new NoOpInboxService();
    }

    @Bean
    ConsumerDispatcher consumerDispatcher() {
      return noOpConsumerDispatcher();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class CustomInboxRetryDispatcherConfiguration {
    @Bean
    InboxRetryDispatcher applicationInboxRetryDispatcher() {
      return new InboxRetryDispatcher(
          new NoOpInboxService(),
          noOpConsumerDispatcher(),
          new InboxRetryPolicy() {
            @Override
            public boolean canRetry(int attemptCount) {
              return false;
            }

            @Override
            public Instant nextAttemptAt(
                int attemptCount,
                Instant failedAt
            ) {
              return failedAt;
            }
          },
          Clock.systemUTC(),
          "application:inbox-retry",
          Duration.ofSeconds(30)
      );
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class DispatcherConfiguration {
    @Bean
    OutboxDispatcher outboxDispatcher() {
      return new OutboxDispatcher(
          new NoOpOutboxService(),
          destination -> {
            throw new UnsupportedOperationException();
          },
          event -> {
            throw new UnsupportedOperationException();
          },
          new BrokerProducerRegistry(List.of()),
          new RetryPolicy() {
            @Override
            public boolean allowsRetry(int failedAttemptCount) {
              return false;
            }

            @Override
            public Instant nextEligibleAt(
                int failedAttemptCount,
                Instant failedAt
            ) {
              return failedAt;
            }
          },
          Clock.systemUTC()
      );
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class CoreDispatcherDependenciesConfiguration {
    @Bean
    OutboxService outboxRepository() {
      return new NoOpOutboxService();
    }

    @Bean
    DestinationResolver destinationResolver() {
      return destination -> {
        throw new UnsupportedOperationException();
      };
    }

    @Bean
    EventSerializer eventSerializer() {
      return event -> {
        throw new UnsupportedOperationException();
      };
    }

    @Bean
    BrokerProducerRegistry brokerProducerRegistry() {
      return new BrokerProducerRegistry(List.of());
    }

    @Bean
    RetryPolicy retryPolicy() {
      return new RetryPolicy() {
        @Override
        public boolean allowsRetry(int failedAttemptCount) {
          return false;
        }

        @Override
        public Instant nextEligibleAt(
            int failedAttemptCount,
            Instant failedAt
        ) {
          return failedAt;
        }
      };
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class CustomRoutingConfiguration {
    @Bean
    DestinationResolver customDestinationResolver() {
      return destination -> {
        throw new UnsupportedOperationException();
      };
    }

    @Bean
    BrokerProducerRegistry customBrokerProducerRegistry() {
      return new BrokerProducerRegistry(List.of());
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class KafkaProducerConfiguration {
    @Bean
    BrokerProducer kafkaProducer() {
      return new BrokerProducer() {
        @Override
        public BrokerId brokerId() {
          return new BrokerId("kafka");
        }

        @Override
        public BrokerPublishResult publish(BrokerMessage message) {
          return new BrokerPublishResult("published");
        }
      };
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class OverrideSchedulerConfiguration {
    @Bean
    OutboxDispatcher outboxDispatcher() {
      return new DispatcherConfiguration().outboxDispatcher();
    }

    @Bean
    OutboxDispatchScheduler applicationOutboxDispatchScheduler(OutboxDispatcher outboxDispatcher) {
      return OutboxDispatchScheduler.create(
          outboxDispatcher,
          new NervEventProperties.Dispatcher(),
          "application-scheduler",
          Clock.systemUTC(),
          noOpTaskScheduler(),
          new DispatcherPollingPolicy(
              java.time.Duration.ofSeconds(1),
              java.time.Duration.ofSeconds(30),
              2.0,
              0.0,
              () -> 0.5
          )
      );
    }
  }

  private static TaskScheduler noOpTaskScheduler() {
    return (TaskScheduler) Proxy.newProxyInstance(
        NervEventAutoConfigurationTest.class.getClassLoader(),
        new Class<?>[]{TaskScheduler.class},
        (
            proxy,
            method,
            arguments) -> null
    );
  }

  private static ConsumerDispatcher noOpConsumerDispatcher() {
    return new ConsumerDispatcher(
        new EventHandlerRegistry(List.of()),
        new EventDeserializer() {
          @Override
          public <T> T deserialize(
              SerializedPayload payload,
              Class<T> payloadType
          ) {
            throw new UnsupportedOperationException();
          }
        }
    );
  }

  private static final class NoOpInboxService implements InboxService {
    @Override
    public InboxRegistration register(InboxEvent event) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<InboxEvent> find(EventId eventId) {
      return Optional.empty();
    }

    @Override
    public Optional<InboxEvent> claim(
        EventId eventId,
        Instant now,
        String owner,
        Duration leaseDuration
    ) {
      return Optional.empty();
    }

    @Override
    public List<InboxEvent> claimPendingRetries(
        int limit,
        Instant now,
        String owner,
        Duration leaseDuration
    ) {
      return List.of();
    }

    @Override
    public void markProcessed(
        EventId eventId,
        String owner,
        Instant processedAt
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void markRetryPending(
        EventId eventId,
        String owner,
        int attemptCount,
        Instant failedAt,
        Instant availableAt,
        String error
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void markFailed(
        EventId eventId,
        String owner,
        int attemptCount,
        Instant failedAt,
        String error
    ) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class NoOpOutboxService implements OutboxService {
    @Override
    public OutboxEvent save(OutboxEvent event) {
      return event;
    }

    @Override
    public List<OutboxEvent> claimPending(
        Instant eligibleAt,
        int batchSize
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void markPublished(
        OutboxId id,
        BrokerPublishResult result
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reschedule(
        OutboxId id,
        int attemptCount,
        Instant nextAttemptAt,
        String failureReason
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void markFailed(
        OutboxId id,
        int attemptCount,
        String failureReason
    ) {
      throw new UnsupportedOperationException();
    }
  }
}
