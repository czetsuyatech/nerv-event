package com.czetsuyatech.nerv.event.observability.autoconfigure;

import com.czetsuyatech.nerv.event.core.outbox.DispatchResult;
import com.czetsuyatech.nerv.event.observability.InboxOperationalMetrics;
import com.czetsuyatech.nerv.event.observability.NervEventBacklogMetrics;
import com.czetsuyatech.nerv.event.observability.NervEventInboxHealthIndicator;
import com.czetsuyatech.nerv.event.observability.NervEventMetrics;
import com.czetsuyatech.nerv.event.observability.NervEventOutboxHealthIndicator;
import com.czetsuyatech.nerv.event.observability.NervEventRetentionHealthIndicator;
import com.czetsuyatech.nerv.event.observability.NervEventSchedulerMetrics;
import com.czetsuyatech.nerv.event.observability.NervEventSchedulersHealthIndicator;
import com.czetsuyatech.nerv.event.observability.ObservedBrokerProducer;
import com.czetsuyatech.nerv.event.observability.OutboxOperationalMetrics;
import com.czetsuyatech.nerv.event.observability.tracing.TraceContextStore;
import com.czetsuyatech.nerv.event.observability.tracing.TracingBrokerProducer;
import com.czetsuyatech.nerv.event.observability.tracing.TracingOutboxService;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventAutoConfiguration;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import com.czetsuyatech.nerv.event.spring.broker.BrokerProducerDecorator;
import com.czetsuyatech.nerv.event.spring.dispatcher.InboxRetryCycleListener;
import com.czetsuyatech.nerv.event.spring.dispatcher.InboxRetryScheduler;
import com.czetsuyatech.nerv.event.spring.dispatcher.OutboxDispatchCycleListener;
import com.czetsuyatech.nerv.event.spring.dispatcher.OutboxDispatchScheduler;
import com.czetsuyatech.nerv.event.spring.outbox.OutboxRepositoryDecorator;
import com.czetsuyatech.nerv.event.spring.retention.EventRetentionScheduler;
import com.czetsuyatech.nerv.event.spring.retention.RetentionCycleListener;
import com.czetsuyatech.nerv.event.spring.retention.RetentionResult;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatusProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;

/**
 * Optional observability bindings; it has no broker-specific dependency and changes no event lifecycle.
 */
@AutoConfiguration(after = NervEventAutoConfiguration.class)
@EnableConfigurationProperties(NervEventObservabilityProperties.class)
@ConditionalOnProperty(
    prefix = "nerv.event.observability",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
public class NervEventObservabilityAutoConfiguration {

  @Bean
  @ConditionalOnBean(MeterRegistry.class)
  @ConditionalOnProperty(
      prefix = "nerv.event.observability.metrics",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  NervEventMetrics nervEventMetrics(MeterRegistry registry) {
    return new NervEventMetrics(registry);
  }

  @Bean
  @ConditionalOnBean(MeterRegistry.class)
  @ConditionalOnProperty(
      prefix = "nerv.event.observability.metrics",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  NervEventSchedulerMetrics nervEventSchedulerMetrics(
      MeterRegistry registry,
      Clock clock,
      List<SchedulerStatusProvider> schedulers
  ) {
    return new NervEventSchedulerMetrics(registry, clock, schedulers);
  }

  @Bean("nervEventSchedulers")
  @ConditionalOnBean(SchedulerStatusProvider.class)
  @ConditionalOnProperty(
      prefix = "nerv.event.observability.health",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  @ConditionalOnMissingBean(name = "nervEventSchedulers")
  HealthIndicator nervEventSchedulersHealth(
      List<SchedulerStatusProvider> schedulers,
      Clock clock,
      NervEventProperties properties
  ) {
    return new NervEventSchedulersHealthIndicator(
        schedulers,
        clock,
        properties.getScheduler().getHealth().getExecutionGrace()
    );
  }

  @Bean
  @ConditionalOnBean(NervEventMetrics.class)
  BrokerProducerDecorator observedBrokerProducerDecorator(NervEventMetrics metrics) {
    return producer -> new ObservedBrokerProducer(
        producer,
        metrics
    );
  }

  @Bean
  @ConditionalOnBean({Tracer.class, Propagator.class, TraceContextStore.class})
  @ConditionalOnProperty(
      prefix = "nerv.event.observability.tracing",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  BrokerProducerDecorator tracingBrokerProducerDecorator(
      TraceContextStore store,
      Tracer tracer,
      Propagator propagator
  ) {
    return producer -> new TracingBrokerProducer(
        producer,
        store,
        tracer,
        propagator
    );
  }

  @Bean
  @ConditionalOnBean({Tracer.class, Propagator.class, TraceContextStore.class})
  @ConditionalOnProperty(
      prefix = "nerv.event.observability.tracing",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  OutboxRepositoryDecorator tracingOutboxRepositoryDecorator(
      TraceContextStore store,
      Tracer tracer,
      Propagator propagator
  ) {
    return repository -> new TracingOutboxService(
        repository,
        store,
        tracer,
        propagator
    );
  }

  @Bean
  @ConditionalOnBean(NervEventMetrics.class)
  OutboxDispatchCycleListener outboxMetricsListener(NervEventMetrics metrics) {
    return new OutboxDispatchCycleListener() {
      public void onSuccess(
          DispatchResult r,
          Duration d
      ) {
        metrics.outboxDispatch(r, d);
      }

      public void onFailure(
          Duration d,
          Exception e
      ) {
        metrics.outboxDispatchFailure(d);
      }
    };
  }

  @Bean
  @ConditionalOnBean(NervEventMetrics.class)
  InboxRetryCycleListener inboxMetricsListener(NervEventMetrics metrics) {
    return new InboxRetryCycleListener() {
      public void onSuccess(
          com.czetsuyatech.nerv.event.core.inbox.InboxRetryResult r,
          Duration d
      ) {
        metrics.inboxRetry(r, d);
      }

      public void onFailure(
          Duration d,
          Exception e
      ) {
        metrics.inboxRetryFailure(d);
      }
    };
  }

  @Bean
  @ConditionalOnBean(NervEventMetrics.class)
  RetentionCycleListener retentionMetricsListener(NervEventMetrics metrics) {
    return new RetentionCycleListener() {
      public void onSuccess(
          RetentionResult r,
          Duration d
      ) {
        metrics.retention(r, d);
      }

      public void onFailure(
          Duration d,
          Exception e
      ) {
        metrics.retentionFailure(d);
      }
    };
  }

  @Bean
  @ConditionalOnBean(NervEventMetrics.class)
  NervEventBacklogMetrics nervEventBacklogMetrics(
      MeterRegistry registry,
      Clock clock,
      ObjectProvider<OutboxOperationalMetrics> outbox,
      ObjectProvider<InboxOperationalMetrics> inbox
  ) {
    return new NervEventBacklogMetrics(
        registry,
        clock,
        Optional.ofNullable(outbox.getIfAvailable()),
        Optional.ofNullable(inbox.getIfAvailable())
    );
  }

  @Bean("nervEventOutbox")
  @ConditionalOnBean(OutboxOperationalMetrics.class)
  @ConditionalOnProperty(
      prefix = "nerv.event.observability.health",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  @ConditionalOnMissingBean(name = "nervEventOutbox")
  HealthIndicator nervEventOutboxHealth(
      OutboxOperationalMetrics m,
      ObjectProvider<OutboxDispatchScheduler> scheduler,
      Clock clock,
      NervEventObservabilityProperties p
  ) {
    return new NervEventOutboxHealthIndicator(
        m,
        scheduler.getIfAvailable(),
        clock,
        p.getHealth().getOutbox()
    );
  }

  @Bean("nervEventInbox")
  @ConditionalOnBean(InboxOperationalMetrics.class)
  @ConditionalOnProperty(
      prefix = "nerv.event.observability.health",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  @ConditionalOnMissingBean(name = "nervEventInbox")
  HealthIndicator nervEventInboxHealth(
      InboxOperationalMetrics m,
      ObjectProvider<InboxRetryScheduler> scheduler,
      Clock clock,
      NervEventObservabilityProperties p
  ) {
    return new NervEventInboxHealthIndicator(
        m,
        scheduler.getIfAvailable(),
        clock,
        p.getHealth().getInbox()
    );
  }

  @Bean("nervEventRetention")
  @ConditionalOnBean(EventRetentionScheduler.class)
  @ConditionalOnProperty(
      prefix = "nerv.event.observability.health",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  @ConditionalOnMissingBean(name = "nervEventRetention")
  HealthIndicator nervEventRetentionHealth(EventRetentionScheduler scheduler) {
    return new NervEventRetentionHealthIndicator(scheduler);
  }
}
