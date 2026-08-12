package com.czetsuyatech.nerv.event.spring.autoconfigure;

import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.outbox.OutboxIdGenerator;
import com.czetsuyatech.nerv.event.core.outbox.OutboxDispatcher;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import com.czetsuyatech.nerv.event.core.retention.EventRetention;
import com.czetsuyatech.nerv.event.core.retention.RetentionSidecarCleaner;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducerRegistry;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducerNotFoundException;
import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.consumer.EventHandler;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.DefaultInboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.consumer.InboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.inbox.ExponentialInboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryDispatcher;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.consumer.EventHandlerRegistry;
import com.czetsuyatech.nerv.event.core.retry.RetryPolicy;
import com.czetsuyatech.nerv.event.core.routing.DestinationResolver;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.EventSerializer;
import com.czetsuyatech.nerv.event.publisher.EventPublisher;
import com.czetsuyatech.nerv.event.spring.dispatcher.DispatcherOwnerResolver;
import com.czetsuyatech.nerv.event.spring.dispatcher.DispatcherPollingPolicy;
import com.czetsuyatech.nerv.event.spring.dispatcher.OutboxDispatchScheduler;
import com.czetsuyatech.nerv.event.spring.dispatcher.InboxRetryScheduler;
import com.czetsuyatech.nerv.event.spring.dispatcher.InboxRetryCycleListener;
import com.czetsuyatech.nerv.event.spring.dispatcher.OutboxDispatchCycleListener;
import com.czetsuyatech.nerv.event.spring.retention.EventRetentionScheduler;
import com.czetsuyatech.nerv.event.spring.retention.RetentionCycleListener;
import com.czetsuyatech.nerv.event.spring.broker.DefaultBrokerProducerRegistry;
import com.czetsuyatech.nerv.event.spring.broker.BrokerProducerDecorator;
import com.czetsuyatech.nerv.event.spring.outbox.OutboxRepositoryDecorator;
import com.czetsuyatech.nerv.event.spring.routing.ConfiguredDestinationResolver;
import com.czetsuyatech.nerv.event.spring.publisher.DefaultEventPublisher;
import com.czetsuyatech.nerv.event.spring.publisher.UuidOutboxIdGenerator;
import com.czetsuyatech.nerv.event.spring.serialization.JacksonEventDeserializer;
import com.czetsuyatech.nerv.event.spring.serialization.JacksonEventSerializer;
import java.time.Clock;
import java.util.function.DoubleSupplier;
import java.util.List;
import java.util.Optional;
import java.util.random.RandomGenerator;
import org.springframework.core.env.Environment;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import tools.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

/**
 * Auto-configures the transactional outbox publication entry point.
 */
@AutoConfiguration(after = JacksonAutoConfiguration.class)
@Slf4j
@EnableConfigurationProperties(NervEventProperties.class)
public class NervEventAutoConfiguration {

  @Bean
  DispatcherPropertiesValidated dispatcherPropertiesValidated(NervEventProperties properties) {
    properties.validate();
    NervEventProperties.Inbox.Retry retry = properties.getInbox().getRetry();
    log.debug(
        "Inbox retry configured enabled={} maxAttempts={} initialDelay={} multiplier={} maxDelay={}",
        retry.isEnabled(),
        retry.getMaxAttempts(),
        retry.getInitialDelay(),
        retry.getMultiplier(),
        retry.getMaxDelay()
    );
    return DispatcherPropertiesValidated.INSTANCE;
  }

  @Bean
  @ConditionalOnMissingBean(Clock.class)
  Clock nervEventClock() {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean(InboxRetryPolicy.class)
  @ConditionalOnProperty(
      prefix = "nerv.event.inbox.retry",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  InboxRetryPolicy inboxRetryPolicy(NervEventProperties properties) {
    NervEventProperties.Inbox.Retry retry = properties.getInbox().getRetry();
    return new ExponentialInboxRetryPolicy(
        retry.getMaxAttempts(),
        retry.getInitialDelay(),
        retry.getMultiplier(),
        retry.getMaxDelay()
    );
  }

  @Bean
  @ConditionalOnMissingBean(OutboxIdGenerator.class)
  OutboxIdGenerator outboxIdGenerator() {
    return new UuidOutboxIdGenerator();
  }

  @Bean
  @ConditionalOnBean(OutboxService.class)
  @ConditionalOnMissingBean(EventPublisher.class)
  EventPublisher eventPublisher(
      OutboxService outboxService,
      OutboxIdGenerator outboxIdGenerator,
      Clock clock,
      ObjectProvider<OutboxRepositoryDecorator> decorators
  ) {
    return new DefaultEventPublisher(
        decorate(
            outboxService,
            decorators.orderedStream()
                .toList()
        ),
        outboxIdGenerator,
        clock
    );
  }

  @Bean
  @ConditionalOnMissingBean(DestinationResolver.class)
  DestinationResolver destinationResolver(NervEventProperties properties) {
    return new ConfiguredDestinationResolver(properties);
  }

  @Bean
  @ConditionalOnMissingBean(BrokerProducerRegistry.class)
  BrokerProducerRegistry brokerProducerRegistry(
      List<BrokerProducer> producers,
      ObjectProvider<BrokerProducerDecorator> decorators
  ) {
    return DefaultBrokerProducerRegistry.create(
        producers,
        decorators.orderedStream()
            .toList()
    );
  }

  @Bean
  @ConditionalOnMissingBean(EventHandlerRegistry.class)
  EventHandlerRegistry eventHandlerRegistry(List<EventHandler<?>> handlers) {
    return new EventHandlerRegistry(handlers);
  }

  @Bean
  @ConditionalOnMissingBean(InboxFailureClassifier.class)
  InboxFailureClassifier inboxFailureClassifier() {
    return new DefaultInboxFailureClassifier();
  }

  @Bean
  @ConditionalOnBean(ObjectMapper.class)
  @ConditionalOnMissingBean(EventDeserializer.class)
  EventDeserializer eventDeserializer(ObjectMapper objectMapper) {
    return new JacksonEventDeserializer(objectMapper);
  }

  @Bean
  @ConditionalOnBean(ObjectMapper.class)
  @ConditionalOnMissingBean(EventSerializer.class)
  EventSerializer eventSerializer(ObjectMapper objectMapper) {
    return new JacksonEventSerializer(objectMapper);
  }

  @Bean
  @ConditionalOnBean({EventHandlerRegistry.class, EventDeserializer.class})
  @ConditionalOnMissingBean(ConsumerDispatcher.class)
  ConsumerDispatcher consumerDispatcher(
      EventHandlerRegistry eventHandlerRegistry,
      EventDeserializer eventDeserializer
  ) {
    return new ConsumerDispatcher(
        eventHandlerRegistry,
        eventDeserializer
    );
  }

  @Bean
  @ConditionalOnBean({InboxService.class, ConsumerDispatcher.class, InboxRetryPolicy.class})
  @ConditionalOnMissingBean(InboxRetryDispatcher.class)
  InboxRetryDispatcher inboxRetryDispatcher(
      InboxService inboxService,
      ConsumerDispatcher consumerDispatcher,
      InboxRetryPolicy inboxRetryPolicy,
      InboxFailureClassifier inboxFailureClassifier,
      NervEventProperties properties,
      Clock clock,
      Environment environment
  ) {
    NervEventProperties.Inbox.Dispatcher dispatcher = properties.getInbox().getDispatcher();
    String owner = DispatcherOwnerResolver.resolve(
        dispatcher.getOwner(),
        environment
    ) + ":inbox-retry";
    return new InboxRetryDispatcher(
        inboxService,
        consumerDispatcher,
        inboxRetryPolicy,
        clock,
        owner,
        dispatcher.getLeaseDuration(),
        inboxFailureClassifier
    );
  }

  @Bean
  @ConditionalOnBean(InboxRetryDispatcher.class)
  @ConditionalOnMissingBean(InboxRetryScheduler.class)
  @ConditionalOnProperty(
      prefix = "nerv.event.inbox.dispatcher",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  @ConditionalOnProperty(
      prefix = "nerv.event.inbox.retry",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  InboxRetryScheduler inboxRetryScheduler(
      InboxRetryDispatcher inboxRetryDispatcher,
      NervEventProperties properties,
      Clock clock,
      @Qualifier("nervEventInboxRetryTaskScheduler") TaskScheduler taskScheduler,
      ObjectProvider<InboxRetryCycleListener> cycleListeners
  ) {
    NervEventProperties.Inbox.Dispatcher dispatcher = properties.getInbox().getDispatcher();
    return InboxRetryScheduler.create(
        inboxRetryDispatcher,
        dispatcher,
        inboxRetryDispatcher.getOwner(),
        clock,
        taskScheduler,
        inboxDispatcherPollingPolicy(dispatcher),
        cycleListeners.orderedStream().toList()
    );
  }

  @Bean("nervEventInboxRetryTaskScheduler")
  @ConditionalOnBean(InboxRetryDispatcher.class)
  @ConditionalOnMissingBean(name = "nervEventInboxRetryTaskScheduler")
  @ConditionalOnProperty(
      prefix = "nerv.event.inbox.dispatcher",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  @ConditionalOnProperty(
      prefix = "nerv.event.inbox.retry",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  TaskScheduler nervEventInboxRetryTaskScheduler() {
    ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
    taskScheduler.setPoolSize(1);
    taskScheduler.setThreadNamePrefix("nerv-event-inbox-retry-");
    taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
    return taskScheduler;
  }

  @Bean
  @ConditionalOnBean(EventRetention.class)
  @ConditionalOnMissingBean(EventRetentionScheduler.class)
  @ConditionalOnProperty(prefix = "nerv.event.retention", name = "enabled", havingValue = "true")
  EventRetentionScheduler eventRetentionScheduler(
      EventRetention repository,
      ObjectProvider<RetentionSidecarCleaner> sidecarCleaner,
      NervEventProperties properties,
      Clock clock,
      @Qualifier("nervEventRetentionTaskScheduler") TaskScheduler taskScheduler,
      ObjectProvider<RetentionCycleListener> cycleListeners
  ) {
    return EventRetentionScheduler.create(
        repository,
        Optional.ofNullable(sidecarCleaner.getIfAvailable()),
        properties.getRetention(),
        clock,
        taskScheduler,
        retentionPollingPolicy(properties.getRetention()),
        cycleListeners.orderedStream().toList()
    );
  }

  @Bean("nervEventRetentionTaskScheduler")
  @ConditionalOnBean(EventRetention.class)
  @ConditionalOnMissingBean(name = "nervEventRetentionTaskScheduler")
  @ConditionalOnProperty(prefix = "nerv.event.retention", name = "enabled", havingValue = "true")
  TaskScheduler nervEventRetentionTaskScheduler() {
    ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
    taskScheduler.setPoolSize(1);
    taskScheduler.setThreadNamePrefix("nerv-event-retention-");
    taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
    return taskScheduler;
  }

  @Bean
  @ConditionalOnBean({
      OutboxService.class,
      DestinationResolver.class,
      EventSerializer.class,
      BrokerProducerRegistry.class,
      RetryPolicy.class
  })
  @ConditionalOnMissingBean(OutboxDispatcher.class)
  OutboxDispatcher outboxDispatcher(
      OutboxService outboxService,
      DestinationResolver destinationResolver,
      EventSerializer eventSerializer,
      BrokerProducerRegistry brokerProducerRegistry,
      RetryPolicy retryPolicy,
      Clock clock,
      ObjectProvider<OutboxRepositoryDecorator> decorators
  ) {
    return new OutboxDispatcher(
        decorate(
            outboxService,
            decorators.orderedStream().toList()
        ),
        destinationResolver,
        eventSerializer,
        brokerProducerRegistry,
        retryPolicy,
        clock
    );
  }

  @Bean
  @ConditionalOnBean(OutboxDispatcher.class)
  @ConditionalOnMissingBean(OutboxDispatchScheduler.class)
  @ConditionalOnProperty(
      prefix = "nerv.event.dispatcher",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  OutboxDispatchScheduler outboxDispatchScheduler(
      OutboxDispatcher outboxDispatcher,
      NervEventProperties properties,
      Clock clock,
      Environment environment,
      BrokerProducerRegistry brokerProducerRegistry,
      @Qualifier("nervEventTaskScheduler") TaskScheduler taskScheduler,
      ObjectProvider<OutboxDispatchCycleListener> cycleListeners
  ) {
    validateDispatcherBrokerAvailability(
        properties,
        brokerProducerRegistry
    );
    return OutboxDispatchScheduler.create(
        outboxDispatcher,
        properties.getDispatcher(),
        DispatcherOwnerResolver.resolve(
            properties.getDispatcher(),
            environment
        ),
        clock,
        taskScheduler,
        dispatcherPollingPolicy(properties.getDispatcher()),
        cycleListeners.orderedStream().toList()
    );
  }

  @Bean("nervEventTaskScheduler")
  @ConditionalOnBean(OutboxDispatcher.class)
  @ConditionalOnMissingBean(name = "nervEventTaskScheduler")
  @ConditionalOnProperty(
      prefix = "nerv.event.dispatcher",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  TaskScheduler nervEventTaskScheduler() {
    ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
    taskScheduler.setPoolSize(1);
    taskScheduler.setThreadNamePrefix("nerv-event-dispatcher-");
    taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
    return taskScheduler;
  }

  private static DispatcherPollingPolicy dispatcherPollingPolicy(
      NervEventProperties.Dispatcher properties
  ) {
    NervEventProperties.Dispatcher.Polling polling = properties.getPolling();
    DoubleSupplier random = RandomGenerator.getDefault()::nextDouble;
    return new DispatcherPollingPolicy(
        polling.getMinInterval(),
        polling.getMaxInterval(),
        polling.getMultiplier(),
        polling.getJitter(),
        random
    );
  }

  private static DispatcherPollingPolicy inboxDispatcherPollingPolicy(
      NervEventProperties.Inbox.Dispatcher properties
  ) {
    NervEventProperties.Inbox.Dispatcher.Polling polling = properties.getPolling();
    DoubleSupplier random = RandomGenerator.getDefault()::nextDouble;
    return new DispatcherPollingPolicy(
        polling.getMinInterval(),
        polling.getMaxInterval(),
        polling.getMultiplier(),
        polling.getJitter(),
        random
    );
  }

  private static DispatcherPollingPolicy retentionPollingPolicy(
      NervEventProperties.Retention properties
  ) {
    NervEventProperties.Retention.Polling polling = properties.getPolling();
    DoubleSupplier random = RandomGenerator.getDefault()::nextDouble;
    return new DispatcherPollingPolicy(
        polling.getMinInterval(),
        polling.getMaxInterval(),
        polling.getMultiplier(),
        polling.getJitter(),
        random
    );
  }

  private static void validateDispatcherBrokerAvailability(
      NervEventProperties properties,
      BrokerProducerRegistry brokerProducerRegistry
  ) {
    properties.getDestinations()
        .forEach(
            (
                destinationName,
                destination) -> {
              BrokerId brokerId = new BrokerId(destination.getBroker());
              try {
                brokerProducerRegistry.producerFor(brokerId);
              } catch (BrokerProducerNotFoundException exception) {
                throw new IllegalStateException(
                    "Dispatcher is enabled but destination '" + destinationName
                        + "' requires no registered BrokerProducer for broker '" + brokerId.value() + "'",
                    exception
                );
              }
            }
        );
  }

  private static OutboxService decorate(
      OutboxService repository,
      List<OutboxRepositoryDecorator> decorators
  ) {
    OutboxService decorated = repository;
    for (OutboxRepositoryDecorator decorator : decorators)
      decorated = decorator.decorate(decorated);
    return decorated;
  }

  enum DispatcherPropertiesValidated {
    INSTANCE
  }
}
