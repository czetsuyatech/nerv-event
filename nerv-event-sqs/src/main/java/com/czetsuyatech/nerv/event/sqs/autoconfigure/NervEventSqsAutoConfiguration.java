package com.czetsuyatech.nerv.event.sqs.autoconfigure;

import com.czetsuyatech.nerv.event.sqs.client.SqsClientId;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistration;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistry;
import com.czetsuyatech.nerv.event.sqs.producer.SqsBrokerProducer;
import com.czetsuyatech.nerv.event.sqs.routing.SqsDestination;
import com.czetsuyatech.nerv.event.sqs.routing.SqsDestinationResolver;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventAutoConfiguration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;

/**
 * Auto-configures the producer-only SQS adapter.
 */
@Slf4j
@AutoConfiguration(before = NervEventAutoConfiguration.class)
@ConditionalOnClass(SqsAsyncClient.class)
@ConditionalOnProperty(prefix = "nerv.event.sqs", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(NervEventSqsProperties.class)
public class NervEventSqsAutoConfiguration {

  @Bean(destroyMethod = "close")
  @ConditionalOnMissingBean(SqsClientRegistry.class)
  SqsClientRegistry sqsClientRegistry(
      NervEventSqsProperties properties,
      ListableBeanFactory beanFactory
  ) {
    properties.validateShape();
    Map<String, SqsAsyncClient> supplied = beanFactory.getBeansOfType(SqsAsyncClient.class);
    List<SqsClientRegistration> registrations = new ArrayList<>();
    if (properties.getClients().isEmpty()) {
      if (supplied.size() == 1) {
        registrations.add(
            SqsClientRegistration.applicationProvided(
                new SqsClientId("default"),
                supplied.values().iterator().next()
            )
        );
      } else if (supplied.size() > 1) {
        throw new IllegalStateException(
            "Multiple SqsAsyncClient beans are available; configure "
                + "nerv.event.sqs.clients with explicit bean-name references"
        );
      }
    } else {
      properties.getClients()
          .forEach(
              (
                  id,
                  config) -> registrations.add(
                      registration(
                          id,
                          config,
                          supplied
                      )
                  )
          );
    }
    return new SqsClientRegistry(registrations);
  }

  @Bean
  @ConditionalOnMissingBean(SqsDestinationResolver.class)
  SqsDestinationResolver sqsDestinationResolver(
      NervEventSqsProperties properties,
      SqsClientRegistry registry
  ) {
    properties.validateShape();
    Map<String, SqsDestination> destinations = new LinkedHashMap<>();
    properties.getDestinations()
        .forEach(
            (
                target,
                configured) -> {
              SqsClientId clientId = new SqsClientId(configured.getClient());
              if (!registry.contains(clientId)) {
                throw new IllegalStateException(
                    "Invalid SQS destination '" + target + "': client '"
                        + clientId.value() + "' is not configured"
                );
              }
              try {
                destinations.put(
                    target,
                    new SqsDestination(
                        clientId,
                        configured.getQueue()
                    )
                );
              } catch (IllegalArgumentException exception) {
                throw new IllegalStateException(
                    "Invalid SQS destination '" + target + "': "
                        + exception.getMessage(),
                    exception
                );
              }
            }
        );
    return new SqsDestinationResolver(destinations);
  }

  @Bean
  @ConditionalOnMissingBean(SqsBrokerProducer.class)
  SqsBrokerProducer sqsBrokerProducer(
      SqsDestinationResolver destinationResolver,
      SqsClientRegistry clientRegistry,
      NervEventSqsProperties properties
  ) {
    SqsBrokerProducer producer = new SqsBrokerProducer(
        destinationResolver,
        clientRegistry,
        properties.getProducer().getSendTimeout()
    );
    log.info(
        "SQS event adapter initialized clients={} destinations={}",
        clientRegistry.size(),
        destinationResolver.size()
    );
    return producer;
  }

  private static SqsClientRegistration registration(
      String id,
      NervEventSqsProperties.Client config,
      Map<String, SqsAsyncClient> supplied
  ) {
    SqsClientId clientId = new SqsClientId(id);
    if (config.getBeanName() != null && !config.getBeanName().isBlank()) {
      SqsAsyncClient client = supplied.get(config.getBeanName());
      if (client == null) {
        throw new IllegalStateException(
            "Invalid SQS client '" + id + "': no SqsAsyncClient bean named '"
                + config.getBeanName() + "'"
        );
      }
      return SqsClientRegistration.applicationProvided(
          clientId,
          client
      );
    }
    SqsAsyncClientBuilder builder = SqsAsyncClient.builder()
        .region(Region.of(config.getRegion()))
        .credentialsProvider(DefaultCredentialsProvider.builder().build());
    if (config.getEndpoint() != null) {
      builder.endpointOverride(config.getEndpoint());
    }
    return SqsClientRegistration.adapterOwned(
        clientId,
        builder.build()
    );
  }
}
