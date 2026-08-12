package com.czetsuyatech.nerv.event.sqs.autoconfigure;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Minimal SQS adapter configuration; advanced clients should be supplied as beans.
 */
@Getter
@Setter
@ConfigurationProperties("nerv.event.sqs")
public class NervEventSqsProperties {

  /**
   * Enables the SQS adapter supplied by the starter.
   */
  private boolean enabled;
  private Map<String, Client> clients = new LinkedHashMap<>();
  private Map<String, Destination> destinations = new LinkedHashMap<>();
  private Map<String, Consumer> consumers = new LinkedHashMap<>();
  private Producer producer = new Producer();

  @Getter
  @Setter
  public static class Client {

    private String region;
    private URI endpoint;
    private String beanName;
  }

  @Getter
  @Setter
  public static class Destination {

    private String client;
    private String queue;
  }

  @Getter
  @Setter
  public static class Producer {

    private Duration sendTimeout = Duration.ofSeconds(30);
  }

  @Getter
  @Setter
  public static class Consumer {

    private boolean enabled;
    private String client;
    private String queue;
    private int maxConcurrentMessages = 10;
    private int maxMessagesPerPoll = 10;
    private Duration pollTimeout = Duration.ofSeconds(20);
    private Duration visibilityTimeout = Duration.ofMinutes(2);
  }

  void validateShape() {
    if (clients == null) {
      throw new IllegalStateException("nerv.event.sqs.clients must not be null");
    }
    if (destinations == null) {
      throw new IllegalStateException("nerv.event.sqs.destinations must not be null");
    }
    if (consumers == null) {
      throw new IllegalStateException("nerv.event.sqs.consumers must not be null");
    }
    if (producer == null || producer.getSendTimeout() == null
        || producer.getSendTimeout().isZero() || producer.getSendTimeout().isNegative()) {
      throw new IllegalStateException("nerv.event.sqs.producer.send-timeout must be greater than zero");
    }
    clients.forEach(
        (
            id,
            client) -> {
          if (id == null || id.isBlank()) {
            throw new IllegalStateException("SQS client ID must not be blank");
          }
          if (client == null) {
            throw new IllegalStateException("Invalid SQS client '" + id + "': configuration must not be null");
          }
          boolean bean = client.getBeanName() != null && !client.getBeanName().isBlank();
          boolean region = client.getRegion() != null && !client.getRegion().isBlank();
          if (!bean && !region) {
            throw new IllegalStateException("Invalid SQS client '" + id + "': region or bean-name is required");
          }
          if (bean && region) {
            throw new IllegalStateException(
                "Invalid SQS client '" + id
                    + "': bean-name cannot be combined with region/endpoint"
            );
          }
          if (bean && client.getEndpoint() != null) {
            throw new IllegalStateException(
                "Invalid SQS client '" + id
                    + "': bean-name cannot be combined with region/endpoint"
            );
          }
        }
    );
    destinations.forEach(
        (
            target,
            destination) -> {
          if (target == null || target.isBlank()) {
            throw new IllegalStateException("SQS destination target must not be blank");
          }
          if (destination == null) {
            throw new IllegalStateException("Invalid SQS destination '" + target + "': configuration is missing");
          }
          if (destination.getClient() == null || destination.getClient().isBlank()) {
            throw new IllegalStateException("Invalid SQS destination '" + target + "': client must not be blank");
          }
          if (destination.getQueue() == null || destination.getQueue().isBlank()) {
            throw new IllegalStateException("Invalid SQS destination '" + target + "': queue must not be blank");
          }
        }
    );
    consumers.forEach(NervEventSqsProperties::validateConsumer);
  }

  private static void validateConsumer(
      String name,
      Consumer consumer
  ) {
    if (name == null || name.isBlank()) {
      throw new IllegalStateException("SQS consumer name must not be blank");
    }
    if (consumer == null) {
      throw new IllegalStateException("Invalid SQS consumer '" + name + "': configuration is missing");
    }
    if (!consumer.isEnabled()) {
      return;
    }
    if (consumer.getClient() == null || consumer.getClient().isBlank()) {
      throw new IllegalStateException("Invalid SQS consumer '" + name + "': client must not be blank");
    }
    if (consumer.getQueue() == null || consumer.getQueue().isBlank()) {
      throw new IllegalStateException("Invalid SQS consumer '" + name + "': queue must not be blank");
    }
    if (consumer.getQueue().endsWith(".fifo")) {
      throw new IllegalStateException(
          "Invalid SQS consumer '" + name + "': FIFO SQS queue '"
              + consumer.getQueue() + "' is not supported by the current nerv-event SQS adapter"
      );
    }
    if (consumer.getMaxConcurrentMessages() <= 0) {
      throw new IllegalStateException(
          "Invalid SQS consumer '" + name
              + "': max-concurrent-messages must be greater than zero"
      );
    }
    if (consumer.getMaxMessagesPerPoll() <= 0 || consumer.getMaxMessagesPerPoll() > 10
        || consumer.getMaxMessagesPerPoll() > consumer.getMaxConcurrentMessages()) {
      throw new IllegalStateException(
          "Invalid SQS consumer '" + name
              + "': max-messages-per-poll must be between 1 and 10 and no greater than max-concurrent-messages"
      );
    }
    requirePositive(
        name,
        "poll-timeout",
        consumer.getPollTimeout()
    );
    if (consumer.getPollTimeout().compareTo(Duration.ofSeconds(20)) > 0) {
      throw new IllegalStateException(
          "Invalid SQS consumer '" + name
              + "': poll-timeout must not exceed 20 seconds"
      );
    }
    requirePositive(
        name,
        "visibility-timeout",
        consumer.getVisibilityTimeout()
    );
    if (consumer.getVisibilityTimeout().compareTo(Duration.ofHours(12)) > 0) {
      throw new IllegalStateException(
          "Invalid SQS consumer '" + name
              + "': visibility-timeout must not exceed 12 hours"
      );
    }
  }

  private static void requirePositive(
      String name,
      String property,
      Duration value
  ) {
    if (value == null || value.isZero() || value.isNegative()) {
      throw new IllegalStateException(
          "Invalid SQS consumer '" + name + "': " + property
              + " must be greater than zero"
      );
    }
  }
}
