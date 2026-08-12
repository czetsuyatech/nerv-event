package com.czetsuyatech.nerv.event.observability;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/**
 * Observes consumer processing without participating in Inbox state transitions.
 */
public interface ConsumerMetrics {

  static ConsumerMetrics noop() {
    return NoOpConsumerMetrics.INSTANCE;
  }

  static ConsumerMetrics composite(List<ConsumerMetrics> metrics) {
    if (metrics.isEmpty()) {
      return noop();
    }
    if (metrics.size() == 1) {
      return metrics.getFirst();
    }
    return new CompositeConsumerMetrics(metrics);
  }

  void received();

  void outcome(ConsumerOutcome outcome);

  void handlerExecutionStarted();

  void handlerExecutionCompleted(
      ConsumerHandlerResult result,
      Duration duration
  );

  void retryScheduled();

  enum ConsumerOutcome {
    PROCESSED("processed"), RETRY_PENDING("retry_pending"), FAILED("failed"), DUPLICATE("duplicate"), UNRESOLVED(
        "unresolved");

    private final String metricValue;

    ConsumerOutcome(String metricValue) {
      this.metricValue = metricValue;
    }

    public String metricValue() {
      return metricValue;
    }
  }

  enum ConsumerHandlerResult {
    SUCCESS("success"), FAILURE("failure");

    private final String metricValue;

    ConsumerHandlerResult(String metricValue) {
      this.metricValue = metricValue;
    }

    public String metricValue() {
      return metricValue;
    }
  }

  final class NoOpConsumerMetrics implements ConsumerMetrics {
    private static final NoOpConsumerMetrics INSTANCE = new NoOpConsumerMetrics();

    private NoOpConsumerMetrics() {
    }

    public void received() {
    }

    public void outcome(ConsumerOutcome outcome) {
    }

    public void handlerExecutionStarted() {
    }

    public void handlerExecutionCompleted(
        ConsumerHandlerResult result,
        Duration duration
    ) {
    }

    public void retryScheduled() {
    }
  }

  final class CompositeConsumerMetrics implements ConsumerMetrics {
    private final List<ConsumerMetrics> delegates;

    private CompositeConsumerMetrics(List<ConsumerMetrics> delegates) {
      this.delegates = List.copyOf(delegates);
    }

    public void received() {
      record(ConsumerMetrics::received);
    }

    public void outcome(ConsumerOutcome outcome) {
      record(metrics -> metrics.outcome(outcome));
    }

    public void handlerExecutionStarted() {
      record(ConsumerMetrics::handlerExecutionStarted);
    }

    public void handlerExecutionCompleted(
        ConsumerHandlerResult result,
        Duration duration
    ) {
      record(metrics -> metrics.handlerExecutionCompleted(result, duration));
    }

    public void retryScheduled() {
      record(ConsumerMetrics::retryScheduled);
    }

    private void record(Consumer<ConsumerMetrics> action) {
      delegates.forEach(metrics -> {
        try {
          action.accept(metrics);
        } catch (RuntimeException ignored) {
        }
      });
    }
  }
}
