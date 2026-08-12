package com.czetsuyatech.nerv.event.sqs.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.czetsuyatech.nerv.event.core.broker.BrokerProducerRegistry;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientId;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistration;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistry;
import com.czetsuyatech.nerv.event.sqs.producer.SqsBrokerProducer;
import com.czetsuyatech.nerv.event.sqs.routing.SqsDestinationResolver;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventAutoConfiguration;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

class NervEventSqsAutoConfigurationTest {
  private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(
          AutoConfigurations.of(
              NervEventSqsAutoConfiguration.class,
              NervEventAutoConfiguration.class
          )
      )
      .withPropertyValues("nerv.event.sqs.enabled=true");

  @Test
  void singleApplicationClientBecomesDefaultAndProducerJoinsGenericRegistry() {
    SqsAsyncClient client = mock(SqsAsyncClient.class);
    contextRunner.withBean(
        "applicationSqs",
        SqsAsyncClient.class,
        () -> client
    )
        .withPropertyValues(
            "nerv.event.sqs.destinations.orders.client=default",
            "nerv.event.sqs.destinations.orders.queue=https://sqs.test/orders"
        )
        .run(context -> {
          assertThat(context).hasSingleBean(SqsBrokerProducer.class);
          assertThat(context.getBean(SqsClientRegistry.class).client(new SqsClientId("default"))).isSameAs(client);
          assertThat(context.getBean(BrokerProducerRegistry.class).producerFor(SqsBrokerProducer.BROKER_ID))
              .isSameAs(context.getBean(SqsBrokerProducer.class));
        });
  }

  @Test
  void multipleNamedClientsBindAndResolveExplicitly() {
    SqsAsyncClient a = mock(SqsAsyncClient.class);
    SqsAsyncClient b = mock(SqsAsyncClient.class);
    contextRunner
        .withBean(
            "clientA",
            SqsAsyncClient.class,
            () -> a
        )
        .withBean(
            "clientB",
            SqsAsyncClient.class,
            () -> b
        )
        .withPropertyValues(
            "nerv.event.sqs.clients.account-a.bean-name=clientA",
            "nerv.event.sqs.clients.account-b.bean-name=clientB",
            "nerv.event.sqs.destinations.orders.client=account-a",
            "nerv.event.sqs.destinations.orders.queue=orders-prod",
            "nerv.event.sqs.destinations.notifications.client=account-b",
            "nerv.event.sqs.destinations.notifications.queue=notifications-prod",
            "nerv.event.sqs.producer.send-timeout=5s"
        )
        .run(context -> {
          assertThat(context).hasNotFailed();
          SqsClientRegistry registry = context.getBean(SqsClientRegistry.class);
          assertThat(registry.client(new SqsClientId("account-a"))).isSameAs(a);
          assertThat(registry.client(new SqsClientId("account-b"))).isSameAs(b);
          assertThat(context.getBean(NervEventSqsProperties.class).getProducer().getSendTimeout())
              .isEqualTo(Duration.ofSeconds(5));
          assertThat(context.getBean(SqsDestinationResolver.class).resolve("orders").queue())
              .isEqualTo("orders-prod");
        });
  }

  @Test
  void multipleClientsWithoutExplicitRegistrationFailClearly() {
    contextRunner.withBean(
        "a",
        SqsAsyncClient.class,
        () -> mock(SqsAsyncClient.class)
    )
        .withBean(
            "b",
            SqsAsyncClient.class,
            () -> mock(SqsAsyncClient.class)
        )
        .run(
            context -> assertThat(context).hasFailed()
                .getFailure()
                .hasMessageContaining("Multiple SqsAsyncClient beans are available")
        );
  }

  @Test
  void unknownDestinationClientFailsAtStartup() {
    contextRunner.withBean(
        SqsAsyncClient.class,
        () -> mock(SqsAsyncClient.class)
    )
        .withPropertyValues(
            "nerv.event.sqs.destinations.orders.client=account-a",
            "nerv.event.sqs.destinations.orders.queue=orders-prod"
        )
        .run(
            context -> assertThat(context).hasFailed()
                .getFailure()
                .hasMessageContaining("Invalid SQS destination 'orders': client 'account-a' is not configured")
        );
  }

  @Test
  void customRegistryAndProducerOverrideDefaults() {
    SqsClientRegistry registry = new SqsClientRegistry(
        List.of(
            SqsClientRegistration.applicationProvided(
                new SqsClientId("custom"),
                mock(SqsAsyncClient.class)
            )
        )
    );
    SqsBrokerProducer producer = new SqsBrokerProducer(
        new SqsDestinationResolver(Map.of()),
        registry,
        Duration.ofSeconds(2)
    );
    contextRunner.withBean(
        SqsClientRegistry.class,
        () -> registry
    )
        .withBean(
            SqsBrokerProducer.class,
            () -> producer
        )
        .run(context -> {
          assertThat(context.getBean(SqsClientRegistry.class)).isSameAs(registry);
          assertThat(context.getBean(SqsBrokerProducer.class)).isSameAs(producer);
        });
  }

  @Test
  void invalidDestinationAndTimeoutFailFast() {
    contextRunner.withBean(
        SqsAsyncClient.class,
        () -> mock(SqsAsyncClient.class)
    )
        .withPropertyValues(
            "nerv.event.sqs.destinations.orders.client=default",
            "nerv.event.sqs.destinations.orders.queue= "
        )
        .run(
            context -> assertThat(context).hasFailed()
                .getFailure()
                .hasMessageContaining("Invalid SQS destination 'orders': queue must not be blank")
        );
    contextRunner.withPropertyValues("nerv.event.sqs.producer.send-timeout=0s")
        .run(
            context -> assertThat(context).hasFailed()
                .getFailure()
                .hasMessageContaining("send-timeout must be greater than zero")
        );
  }
}
