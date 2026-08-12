package com.czetsuyatech.nerv.event.kafka.autoconfigure;

import com.czetsuyatech.nerv.event.kafka.producer.KafkaBrokerProducer;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Auto-configures the Kafka implementation of the generic broker producer contract.
 */
@AutoConfiguration(after = KafkaAutoConfiguration.class, before = NervEventAutoConfiguration.class)
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnProperty(prefix = "nerv.event.kafka", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(NervEventKafkaProperties.class)
public class NervEventKafkaAutoConfiguration {

  @Bean
  KafkaPropertiesValidated kafkaPropertiesValidated(NervEventKafkaProperties properties) {
    properties.validate();
    return KafkaPropertiesValidated.INSTANCE;
  }

  @Bean
  @ConditionalOnBean(KafkaTemplate.class)
  @ConditionalOnMissingBean(KafkaBrokerProducer.class)
  KafkaBrokerProducer kafkaBrokerProducer(
      KafkaTemplate<String, String> kafkaTemplate,
      NervEventKafkaProperties properties
  ) {
    return new KafkaBrokerProducer(
        kafkaTemplate,
        properties.getProducer().getSendTimeout()
    );
  }

  enum KafkaPropertiesValidated {
    INSTANCE
  }
}
