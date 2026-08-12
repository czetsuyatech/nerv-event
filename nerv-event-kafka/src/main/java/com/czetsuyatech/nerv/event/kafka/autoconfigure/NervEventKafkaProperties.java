package com.czetsuyatech.nerv.event.kafka.autoconfigure;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the Nerv Event Kafka producer.
 */
@Getter
@Setter
@ConfigurationProperties("nerv.event.kafka")
public class NervEventKafkaProperties {

  /**
   * Enables the Kafka adapter supplied by the starter.
   */
  private boolean enabled;
  private Producer producer = new Producer();
  private Map<String, Consumer> consumers = new LinkedHashMap<>();

  @Getter
  @Setter
  public static class Producer {
    private Duration sendTimeout = Duration.ofSeconds(30);
  }

  @Getter
  @Setter
  public static class Consumer {
    private boolean enabled;
    private String topic;
    private String groupId;
    private int concurrency = 1;
    private Duration leaseDuration = Duration.ofMinutes(2);
  }

  void validate() {
    if (producer == null || producer.getSendTimeout() == null) {
      throw new IllegalStateException("nerv.event.kafka.producer.send-timeout must not be null");
    }
    if (producer.getSendTimeout().isZero() || producer.getSendTimeout().isNegative()) {
      throw new IllegalStateException("nerv.event.kafka.producer.send-timeout must be greater than zero");
    }
    if (consumers == null) {
      throw new IllegalStateException("nerv.event.kafka.consumers must not be null");
    }
    consumers.forEach(
        (
            name,
            consumer) -> validateConsumer(
                name,
                consumer
            )
    );
  }

  private static void validateConsumer(
      String name,
      Consumer consumer
  ) {
    if (name == null || name.isBlank()) {
      throw new IllegalStateException("nerv.event.kafka.consumers consumer name must not be blank");
    }
    if (consumer == null) {
      throw new IllegalStateException("nerv.event.kafka.consumers." + name + " must not be null");
    }
    if (!consumer.isEnabled()) {
      return;
    }
    if (consumer.getTopic() == null || consumer.getTopic().isBlank()) {
      throw new IllegalStateException("nerv.event.kafka.consumers." + name + ".topic must not be blank when enabled");
    }
    if (consumer.getGroupId() == null || consumer.getGroupId().isBlank()) {
      throw new IllegalStateException(
          "nerv.event.kafka.consumers." + name + ".group-id must not be blank when enabled"
      );
    }
    if (consumer.getConcurrency() <= 0) {
      throw new IllegalStateException(
          "nerv.event.kafka.consumers." + name + ".concurrency must be greater than zero when enabled"
      );
    }
    if (consumer.getLeaseDuration() == null || consumer.getLeaseDuration().isZero()
        || consumer.getLeaseDuration().isNegative()) {
      throw new IllegalStateException(
          "nerv.event.kafka.consumers." + name + ".lease-duration must be greater than zero when enabled"
      );
    }
  }
}
