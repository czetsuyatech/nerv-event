package com.czetsuyatech.nerv.event.kafka.autoconfigure;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.InboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.kafka.consumer.KafkaConsumerAdapter;
import com.czetsuyatech.nerv.event.kafka.consumer.KafkaConsumerContainerFactory;
import com.czetsuyatech.nerv.event.kafka.consumer.KafkaConsumerManager;
import com.czetsuyatech.nerv.event.kafka.consumer.SpringKafkaConsumerContainerFactory;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventAutoConfiguration;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import com.czetsuyatech.nerv.event.spring.dispatcher.DispatcherOwnerResolver;
import java.time.Clock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * Auto-configures programmatic Kafka consumers after generic consumer dispatch is available.
 */
@AutoConfiguration(after = {KafkaAutoConfiguration.class, NervEventAutoConfiguration.class})
@ConditionalOnClass(ConsumerFactory.class)
@ConditionalOnProperty(prefix = "nerv.event.kafka", name = "enabled", havingValue = "true")
public class NervEventKafkaConsumerAutoConfiguration {

  @Bean
  @ConditionalOnBean({ConsumerDispatcher.class, InboxService.class, InboxRetryPolicy.class})
  @ConditionalOnMissingBean(KafkaConsumerAdapter.class)
  KafkaConsumerAdapter kafkaConsumerAdapter(
      ConsumerDispatcher consumerDispatcher,
      InboxService inboxService,
      Clock clock,
      InboxRetryPolicy inboxRetryPolicy,
      InboxFailureClassifier inboxFailureClassifier,
      ObjectProvider<ConsumerMetrics> metrics,
      NervEventProperties properties,
      Environment environment
  ) {
    return new KafkaConsumerAdapter(
        consumerDispatcher,
        inboxService,
        clock,
        DispatcherOwnerResolver.resolve(
            properties.getDispatcher(),
            environment
        ),
        java.time.Duration.ofSeconds(30),
        inboxRetryPolicy,
        inboxFailureClassifier,
        ConsumerMetrics.composite(metrics.orderedStream().toList())
    );
  }

  @Bean
  @ConditionalOnBean({ConsumerDispatcher.class, InboxService.class, ConsumerFactory.class})
  @ConditionalOnMissingBean(KafkaConsumerContainerFactory.class)
  KafkaConsumerContainerFactory kafkaConsumerContainerFactory(
      ConsumerFactory<String, String> consumerFactory
  ) {
    return new SpringKafkaConsumerContainerFactory(consumerFactory);
  }

  @Bean
  @ConditionalOnBean({KafkaConsumerAdapter.class, KafkaConsumerContainerFactory.class})
  @ConditionalOnMissingBean(KafkaConsumerManager.class)
  KafkaConsumerManager kafkaConsumerManager(
      NervEventKafkaProperties properties,
      KafkaConsumerContainerFactory containerFactory,
      KafkaConsumerAdapter adapter
  ) {
    return new KafkaConsumerManager(
        properties,
        containerFactory,
        adapter
    );
  }
}
