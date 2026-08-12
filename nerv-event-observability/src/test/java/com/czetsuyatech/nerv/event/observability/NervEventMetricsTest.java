package com.czetsuyatech.nerv.event.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.observability.ConsumerMetrics.ConsumerHandlerResult;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics.ConsumerOutcome;
import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.outbox.DispatchResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class NervEventMetricsTest {
  @Test
  void uses_only_bounded_result_and_broker_tags_for_real_dispatch_and_publish_outcomes() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    NervEventMetrics metrics = new NervEventMetrics(registry);
    metrics.outboxDispatch(
        new DispatchResult(
            3,
            1,
            1,
            1,
            0
        ),
        Duration.ofMillis(8)
    );
    BrokerProducer observed = new ObservedBrokerProducer(new BrokerProducer() {
      public BrokerId brokerId() {
        return new BrokerId("kafka");
      }

      public BrokerPublishResult publish(BrokerMessage ignored) {
        return new BrokerPublishResult("ack");
      }
    },
        metrics
    );
    observed.publish(null);
    assertThat(
        registry.get("nerv.event.outbox.dispatch.events")
            .tag(
                "result",
                "published"
            )
            .counter()
            .count()
    ).isEqualTo(1);
    assertThat(registry.get("nerv.event.outbox.dispatch.duration").timer().count()).isEqualTo(1);
    assertThat(
        registry.get("nerv.event.broker.publish")
            .tags(
                "broker",
                "kafka",
                "result",
                "success"
            )
            .counter()
            .count()
    ).isEqualTo(1);
    assertThat(registry.getMeters().stream().flatMap(m -> m.getId().getTags().stream()).map(t -> t.getKey()))
        .doesNotContain(
            "eventId",
            "correlationId",
            "outboxId",
            "owner",
            "payload",
            "exception"
        );
  }

  @Test
  void distinguishes_timeout_publish_failures() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    NervEventMetrics metrics = new NervEventMetrics(registry);
    BrokerProducer observed = new ObservedBrokerProducer(new BrokerProducer() {
      public BrokerId brokerId() {
        return new BrokerId("sqs");
      }

      public BrokerPublishResult publish(BrokerMessage ignored) throws Exception {
        throw new TimeoutException("ignored");
      }
    },
        metrics
    );
    try {
      observed.publish(null);
    } catch (Exception ignored) {
    }
    assertThat(
        registry.get("nerv.event.broker.publish")
            .tags(
                "broker",
                "sqs",
                "result",
                "timeout"
            )
            .counter()
            .count()
    ).isEqualTo(1);
  }

  @Test
  void recordsBoundedConsumerMetricsForConfirmedOutcomes() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    NervEventMetrics metrics = new NervEventMetrics(registry);

    metrics.received();
    metrics.handlerExecutionStarted();
    assertThat(registry.get("nerv.event.consumer.in.progress").gauge().value()).isEqualTo(1);
    metrics.handlerExecutionCompleted(
        ConsumerHandlerResult.SUCCESS,
        Duration.ofMillis(1)
    );
    metrics.outcome(ConsumerOutcome.PROCESSED);
    metrics.retryScheduled();

    assertThat(registry.get("nerv.event.consumer.received").counter().count()).isEqualTo(1);
    assertThat(
        registry.get("nerv.event.consumer.events")
            .tag("result", "processed")
            .counter()
            .count()
    ).isEqualTo(1);
    assertThat(
        registry.get("nerv.event.consumer.handler.executions")
            .tag("result", "success")
            .counter()
            .count()
    ).isEqualTo(1);
    assertThat(registry.get("nerv.event.consumer.retries").counter().count()).isEqualTo(1);
    assertThat(
        registry.get("nerv.event.consumer.processing.duration")
            .tag("result", "success")
            .timer()
            .count()
    ).isEqualTo(1);
    assertThat(registry.get("nerv.event.consumer.in.progress").gauge().value()).isZero();
  }

  @Test
  void keepsMetricSeriesBoundedDuringThousandsOfDistinctEventOutcomes() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    NervEventMetrics metrics = new NervEventMetrics(registry);

    for (int event = 0; event < 10_000; event++) {
      metrics.outboxDispatch(
          new DispatchResult(
              1,
              1,
              0,
              0,
              0
          ),
          Duration.ofMillis(1)
      );
      metrics.received();
      metrics.handlerExecutionStarted();
      metrics.handlerExecutionCompleted(
          event % 2 == 0 ? ConsumerHandlerResult.SUCCESS : ConsumerHandlerResult.FAILURE,
          Duration.ofMillis(1)
      );
      metrics.outcome(event % 2 == 0 ? ConsumerOutcome.PROCESSED : ConsumerOutcome.FAILED);
      metrics.brokerPublish(
          event % 2 == 0 ? "kafka" : "sqs",
          "success",
          Duration.ofMillis(1)
      );
    }

    assertThat(registry.getMeters()).hasSizeLessThanOrEqualTo(16);
    assertThat(
        registry.getMeters()
            .stream()
            .flatMap(meter -> meter.getId().getTags().stream())
            .map(tag -> tag.getKey())
    )
        .doesNotContain(
            "eventId",
            "correlationId",
            "outboxId",
            "inboxId",
            "owner",
            "payload"
        );
  }
}
