package com.czetsuyatech.nerv.event.sqs.autoconfigure;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.InboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistry;
import com.czetsuyatech.nerv.event.sqs.consumer.SpringSqsConsumerContainerFactory;
import com.czetsuyatech.nerv.event.sqs.consumer.SqsConsumerAdapter;
import com.czetsuyatech.nerv.event.sqs.consumer.SqsConsumerContainerFactory;
import com.czetsuyatech.nerv.event.sqs.consumer.SqsConsumerManager;
import com.czetsuyatech.nerv.event.sqs.consumer.SqsMessageMapper;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventAutoConfiguration;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import com.czetsuyatech.nerv.event.spring.dispatcher.DispatcherOwnerResolver;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import java.time.Clock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.env.Environment;

/**
 * Auto-configures programmatic SQS consumers after generic dispatch infrastructure.
 */
@AutoConfiguration(after = {NervEventAutoConfiguration.class, NervEventSqsAutoConfiguration.class})
@ConditionalOnClass(SqsMessageListenerContainer.class)
@ConditionalOnProperty(prefix = "nerv.event.sqs", name = "enabled", havingValue = "true")
public class NervEventSqsConsumerAutoConfiguration {
  @Bean
  @ConditionalOnMissingBean(SqsMessageMapper.class)
  SqsMessageMapper sqsMessageMapper() {
    return new SqsMessageMapper();
  }

  @Bean
  @ConditionalOnBean({ConsumerDispatcher.class, InboxService.class, InboxRetryPolicy.class})
  @ConditionalOnMissingBean(SqsConsumerAdapter.class)
  SqsConsumerAdapter sqsConsumerAdapter(
      ConsumerDispatcher dispatcher,
      InboxService repository,
      Clock clock,
      InboxRetryPolicy retryPolicy,
      InboxFailureClassifier inboxFailureClassifier,
      ObjectProvider<ConsumerMetrics> metrics,
      SqsMessageMapper mapper,
      NervEventProperties properties,
      Environment environment
  ) {
    return new SqsConsumerAdapter(
        dispatcher,
        repository,
        clock,
        DispatcherOwnerResolver.resolve(
            properties.getDispatcher(),
            environment
        ),
        properties.getDispatcher().getLeaseDuration(),
        retryPolicy,
        mapper,
        inboxFailureClassifier,
        ConsumerMetrics.composite(metrics.orderedStream().toList())
    );
  }

  @Bean
  @ConditionalOnMissingBean(SqsConsumerContainerFactory.class)
  SqsConsumerContainerFactory sqsConsumerContainerFactory() {
    return new SpringSqsConsumerContainerFactory();
  }

  @Bean
  @ConditionalOnBean({SqsConsumerAdapter.class, SqsClientRegistry.class,
      SqsConsumerContainerFactory.class})
  @ConditionalOnMissingBean(SqsConsumerManager.class)
  SqsConsumerManager sqsConsumerManager(
      NervEventSqsProperties properties,
      SqsClientRegistry registry,
      SqsConsumerContainerFactory factory,
      SqsConsumerAdapter adapter
  ) {
    return new SqsConsumerManager(
        properties,
        registry,
        factory,
        adapter
    );
  }
}
