package com.czetsuyatech.nerv.event.kafka.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducerRegistry;
import com.czetsuyatech.nerv.event.kafka.producer.KafkaBrokerProducer;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventAutoConfiguration;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;

class NervEventKafkaAutoConfigurationTest {

  private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(
          AutoConfigurations.of(
              NervEventKafkaAutoConfiguration.class,
              NervEventAutoConfiguration.class
          )
      )
      .withPropertyValues("nerv.event.kafka.enabled=true");

  @Test
  void registersTheKafkaProducerBeforeTheGenericBrokerRegistry() {
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    contextRunner
        .withBean(
            KafkaTemplate.class,
            () -> kafkaTemplate
        )
        .run(context -> {
          assertThat(context).hasSingleBean(KafkaBrokerProducer.class);
          assertThat(context).hasSingleBean(BrokerProducerRegistry.class);
          assertThat(
              context.getBean(BrokerProducerRegistry.class)
                  .producerFor(KafkaBrokerProducer.BROKER_ID)
          ).isInstanceOf(KafkaBrokerProducer.class);
        });
  }

  @Test
  void doesNotCreateTheProducerWithoutAKafkaTemplate() {
    contextRunner.run(context -> assertThat(context).doesNotHaveBean(KafkaBrokerProducer.class));
  }

  @Test
  void allowsAnApplicationProvidedKafkaProducerToReplaceTheDefault() {
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    KafkaBrokerProducer applicationProducer = new KafkaBrokerProducer(
        kafkaTemplate,
        Duration.ofSeconds(5)
    );

    contextRunner
        .withBean(
            KafkaTemplate.class,
            () -> kafkaTemplate
        )
        .withBean(
            KafkaBrokerProducer.class,
            () -> applicationProducer
        )
        .run(context -> assertThat(context).getBean(KafkaBrokerProducer.class).isSameAs(applicationProducer));
  }

  @Test
  void failsFastWhenAnotherProducerUsesTheKafkaBrokerId() {
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    BrokerProducer duplicateKafkaProducer = new BrokerProducer() {
      @Override
      public BrokerId brokerId() {
        return KafkaBrokerProducer.BROKER_ID;
      }

      @Override
      public BrokerPublishResult publish(com.czetsuyatech.nerv.event.core.broker.BrokerMessage message) {
        throw new UnsupportedOperationException();
      }
    };

    contextRunner
        .withBean(
            KafkaTemplate.class,
            () -> kafkaTemplate
        )
        .withBean(
            BrokerProducer.class,
            () -> duplicateKafkaProducer
        )
        .run(
            context -> assertThat(context).hasFailed()
                .getFailure()
                .hasMessageContaining("Duplicate BrokerProducer for broker 'kafka'")
        );
  }

  @Test
  void bindsTheConfiguredSendTimeout() {
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

    contextRunner
        .withBean(
            KafkaTemplate.class,
            () -> kafkaTemplate
        )
        .withPropertyValues("nerv.event.kafka.producer.send-timeout=PT5S")
        .run(
            context -> assertThat(
                context.getBean(NervEventKafkaProperties.class)
                    .getProducer()
                    .getSendTimeout()
            ).isEqualTo(Duration.ofSeconds(5))
        );
  }

  @Test
  void rejectsANonPositiveSendTimeout() {
    contextRunner
        .withPropertyValues("nerv.event.kafka.producer.send-timeout=PT0S")
        .run(
            context -> assertThat(context).hasFailed()
                .getFailure()
                .hasMessageContaining("send-timeout must be greater than zero")
        );
  }

  @Test
  void appliesAConservativeConsumerLeaseDefault() {
    assertThat(new NervEventKafkaProperties.Consumer().getLeaseDuration()).isEqualTo(Duration.ofMinutes(2));
  }
}
