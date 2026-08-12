package com.czetsuyatech.nerv.event.core.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.exception.EventRetryableException;
import com.czetsuyatech.nerv.event.model.EventId;
import org.junit.jupiter.api.Test;

class DefaultInboxFailureClassifierTest {

  private final InboxFailureClassifier classifier = new DefaultInboxFailureClassifier();

  @Test
  void classifiesOnlyEventRetryableExceptionAsRetryable() {
    assertThat(classifier.isRetryable(new EventRetryableException("temporary"))).isTrue();
    assertThat(classifier.isRetryable(new RuntimeException("ordinary"))).isFalse();
    assertThat(classifier.isRetryable(new IllegalArgumentException("invalid"))).isFalse();
    assertThat(classifier.isRetryable(new EventHandlerNotFoundException("order.created"))).isFalse();
    assertThat(
        classifier.isRetryable(
            new EventDeserializationException(
                new EventId("event-1"),
                "order.created",
                String.class,
                "application/json",
                new IllegalArgumentException("invalid JSON")
            )
        )
    ).isFalse();
  }

  @Test
  void findsRetryabilityInWrappedCausesWithoutClassNameMatching() {
    assertThat(
        classifier.isRetryable(
            new IllegalStateException(
                "dispatcher failed",
                new EventRetryableException("temporary")
            )
        )
    ).isTrue();
    assertThat(
        classifier.isRetryable(
            new IllegalStateException(
                "dispatcher failed",
                new IllegalArgumentException("invalid")
            )
        )
    ).isFalse();
  }
}
