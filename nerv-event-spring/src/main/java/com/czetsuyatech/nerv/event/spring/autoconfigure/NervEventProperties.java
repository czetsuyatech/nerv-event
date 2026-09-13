package com.czetsuyatech.nerv.event.spring.autoconfigure;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Root namespace reserved for core Nerv Event configuration.
 */
@Getter
@Setter
@ConfigurationProperties("nerv.event")
public class NervEventProperties {

  private Dispatcher dispatcher = new Dispatcher();
  private Outbox outbox = new Outbox();
  private Inbox inbox = new Inbox();
  private Retention retention = new Retention();
  private Scheduler scheduler = new Scheduler();
  private Map<String, DestinationProperties> destinations = new LinkedHashMap<>();

  /**
   * Validates event configuration once during application startup.
   */
  public void validate() {
    dispatcher.validate();
    inbox.validate();
    retention.validate();
    scheduler.validate();
    destinations.forEach(this::validateDestination);
  }

  @Getter
  @Setter
  public static class Outbox {
    /** Whether the starter exposes the durable Outbox publication entry point. */
    private boolean enabled = true;
  }

  @Getter
  @Setter
  public static class Scheduler {
    private Health health = new Health();

    private void validate() {
      Dispatcher.validatePositiveDuration(health.executionGrace, "nerv.event.scheduler.health.execution-grace");
    }

    @Getter
    @Setter
    public static class Health {
      private Duration executionGrace = Duration.ofMinutes(2);
    }
  }

  private void validateDestination(
      String destinationName,
      DestinationProperties destination
  ) {
    if (destinationName == null || destinationName.isBlank()) {
      throw new IllegalStateException(
          "Invalid nerv-event destination configuration: destination name must not be blank"
      );
    }
    if (destination == null) {
      throw new IllegalStateException(
          "Invalid nerv-event destination configuration: destination='" + destinationName + "' must not be null"
      );
    }
    validateNonBlankDestinationValue(
        destinationName,
        destination.getBroker(),
        "broker"
    );
    validateNonBlankDestinationValue(
        destinationName,
        destination.getTarget(),
        "target"
    );
  }

  private static void validateNonBlankDestinationValue(
      String destinationName,
      String value,
      String propertyName
  ) {
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(
          "Invalid nerv-event destination configuration: destination='" + destinationName
              + "', " + propertyName + " must not be blank"
      );
    }
  }

  @Getter
  @Setter
  public static class DestinationProperties {
    private String broker;
    private String target;
  }

  /**
   * Settings for conservative cleanup of successful durable rows.
   */
  @Getter
  @Setter
  public static class Retention {
    private boolean enabled;
    private SuccessfulRows outbox = new SuccessfulRows();
    private SuccessfulRows inbox = new SuccessfulRows();
    private int batchSize = 500;
    private Polling polling = new Polling();

    public void validate() {
      if (batchSize <= 0) {
        throw new IllegalStateException("nerv.event.retention.batch-size must be greater than zero");
      }
      outbox.validate("nerv.event.retention.outbox.age");
      inbox.validate("nerv.event.retention.inbox.age");
      polling.validate();
    }

    @Getter
    @Setter
    public static class SuccessfulRows {
      private boolean enabled = true;
      private Duration age = Duration.ofDays(30);

      private void validate(String ageProperty) {
        Dispatcher.validatePositiveDuration(
            age,
            ageProperty
        );
      }
    }

    @Getter
    @Setter
    public static class Polling {
      private Duration minInterval = Duration.ofSeconds(30);
      private Duration maxInterval = Duration.ofMinutes(30);
      private double multiplier = 2.0;
      private double jitter = 0.10;

      private void validate() {
        Dispatcher.validatePositiveDuration(
            minInterval,
            "nerv.event.retention.polling.min-interval"
        );
        Dispatcher.validatePositiveDuration(
            maxInterval,
            "nerv.event.retention.polling.max-interval"
        );
        if (maxInterval.compareTo(minInterval) < 0) {
          throw new IllegalStateException(
              "nerv.event.retention.polling.max-interval must be greater than or equal to min-interval"
          );
        }
        if (!Double.isFinite(multiplier) || multiplier < 1.0) {
          throw new IllegalStateException("nerv.event.retention.polling.multiplier must be at least 1.0");
        }
        if (!Double.isFinite(jitter) || jitter < 0.0 || jitter > 1.0) {
          throw new IllegalStateException("nerv.event.retention.polling.jitter must be between 0.0 and 1.0");
        }
      }
    }
  }

  @Getter
  @Setter
  public static class Dispatcher {
    private boolean enabled = true;
    private int batchSize = 100;
    private Duration leaseDuration = Duration.ofMinutes(2);
    private String owner;
    private Polling polling = new Polling();

    /**
     * Validates scheduler settings when the optional dispatcher is activated.
     */
    public void validate() {
      if (batchSize <= 0) {
        throw new IllegalStateException("nerv.event.dispatcher.batch-size must be greater than zero");
      }
      validatePositiveDuration(
          leaseDuration,
          "nerv.event.dispatcher.lease-duration"
      );
      polling.validate();
      if (owner != null && owner.isBlank()) {
        throw new IllegalStateException("nerv.event.dispatcher.owner must not be blank when configured");
      }
    }

    private static void validatePositiveDuration(
        Duration value,
        String propertyName
    ) {
      if (value == null || value.isZero() || value.isNegative()) {
        throw new IllegalStateException(propertyName + " must be greater than zero");
      }
    }

    @Getter
    @Setter
    public static class Polling {
      private Duration minInterval = Duration.ofSeconds(1);
      private Duration maxInterval = Duration.ofSeconds(30);
      private double multiplier = 2.0;
      private double jitter = 0.10;

      public void validate() {
        validatePositiveDuration(
            minInterval,
            "nerv.event.dispatcher.polling.min-interval"
        );
        validatePositiveDuration(
            maxInterval,
            "nerv.event.dispatcher.polling.max-interval"
        );
        if (maxInterval.compareTo(minInterval) < 0) {
          throw new IllegalStateException(
              "nerv.event.dispatcher.polling.max-interval must be greater than or equal to min-interval"
          );
        }
        if (!Double.isFinite(multiplier) || multiplier < 1.0) {
          throw new IllegalStateException("nerv.event.dispatcher.polling.multiplier must be at least 1.0");
        }
        if (!Double.isFinite(jitter) || jitter < 0.0 || jitter > 1.0) {
          throw new IllegalStateException("nerv.event.dispatcher.polling.jitter must be between 0.0 and 1.0");
        }
      }
    }
  }

  @Getter
  @Setter
  public static class Inbox {
    private Retry retry = new Retry();
    private Dispatcher dispatcher = new Dispatcher();

    private void validate() {
      retry.validate();
      dispatcher.validate();
    }

    @Getter
    @Setter
    public static class Retry {
      /**
       * Whether the generic automatic inbox retry policy is exposed.
       */
      private boolean enabled = true;

      /**
       * <p>
       * Total actual {@code EventHandler} attempts, including the initial handler execution.
       * </p>
       *
       * <p>
       * This does not count broker deliveries, acknowledgements, duplicate redeliveries, or inbox claim attempts.
       * </p>
       */
      private int maxAttempts = 5;
      private Duration initialDelay = Duration.ofSeconds(5);
      private double multiplier = 2.0;
      private Duration maxDelay = Duration.ofMinutes(10);

      private void validate() {
        if (maxAttempts < 1) {
          throw new IllegalStateException(
              "nerv.event.inbox.retry.max-attempts must be at least one: " + maxAttempts
          );
        }
        NervEventProperties.Dispatcher.validatePositiveDuration(
            initialDelay,
            "nerv.event.inbox.retry.initial-delay"
        );
        NervEventProperties.Dispatcher.validatePositiveDuration(
            maxDelay,
            "nerv.event.inbox.retry.max-delay"
        );
        if (!Double.isFinite(multiplier) || multiplier < 1.0) {
          throw new IllegalStateException(
              "nerv.event.inbox.retry.multiplier must be at least 1.0: " + multiplier
          );
        }
        if (maxDelay.compareTo(initialDelay) < 0) {
          throw new IllegalStateException(
              "nerv.event.inbox.retry.max-delay must be greater than or equal to initial-delay"
          );
        }
      }
    }

    /**
     * Settings for the generic database-backed inbox retry scheduler.
     */
    @Getter
    @Setter
    public static class Dispatcher {
      private boolean enabled = true;
      private int batchSize = 100;
      private Duration leaseDuration = Duration.ofMinutes(2);
      private String owner;
      private Polling polling = new Polling();

      public void validate() {
        if (batchSize <= 0) {
          throw new IllegalStateException("nerv.event.inbox.dispatcher.batch-size must be greater than zero");
        }
        NervEventProperties.Dispatcher.validatePositiveDuration(
            leaseDuration,
            "nerv.event.inbox.dispatcher.lease-duration"
        );
        polling.validate();
        if (owner != null && owner.isBlank()) {
          throw new IllegalStateException("nerv.event.inbox.dispatcher.owner must not be blank when configured");
        }
      }

      @Getter
      @Setter
      public static class Polling {
        private Duration minInterval = Duration.ofSeconds(1);
        private Duration maxInterval = Duration.ofSeconds(30);
        private double multiplier = 2.0;
        private double jitter = 0.10;

        private void validate() {
          NervEventProperties.Dispatcher.validatePositiveDuration(
              minInterval,
              "nerv.event.inbox.dispatcher.polling.min-interval"
          );
          NervEventProperties.Dispatcher.validatePositiveDuration(
              maxInterval,
              "nerv.event.inbox.dispatcher.polling.max-interval"
          );
          if (maxInterval.compareTo(minInterval) < 0) {
            throw new IllegalStateException(
                "nerv.event.inbox.dispatcher.polling.max-interval must be greater than or equal to min-interval"
            );
          }
          if (!Double.isFinite(multiplier) || multiplier < 1.0) {
            throw new IllegalStateException(
                "nerv.event.inbox.dispatcher.polling.multiplier must be at least 1.0"
            );
          }
          if (!Double.isFinite(jitter) || jitter < 0.0 || jitter > 1.0) {
            throw new IllegalStateException(
                "nerv.event.inbox.dispatcher.polling.jitter must be between 0.0 and 1.0"
            );
          }
        }
      }
    }
  }
}
