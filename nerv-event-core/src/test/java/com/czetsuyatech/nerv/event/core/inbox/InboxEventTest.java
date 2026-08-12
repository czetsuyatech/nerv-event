package com.czetsuyatech.nerv.event.core.inbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import java.time.Instant;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class InboxEventTest {

  private static final Instant RECEIVED_AT = Instant.parse("2026-08-17T00:00:00Z");
  private static final Instant AVAILABLE_AT = Instant.parse("2026-08-17T00:01:00Z");

  @Test
  void includesRetryPendingStatus() {
    assertThat(Arrays.asList(InboxStatus.values())).contains(InboxStatus.RETRY_PENDING);
  }

  @Test
  void identifiesOnlyRetryPendingEventsAsRetryPending() {
    for (InboxStatus status : InboxStatus.values()) {
      assertThat(
          event(
              status,
              status == InboxStatus.RETRY_PENDING ? AVAILABLE_AT : null
          ).retryPending()
      )
          .isEqualTo(status == InboxStatus.RETRY_PENDING);
    }
  }

  @Test
  void identifiesDueRetryPendingEventsAsEligible() {
    assertThat(
        event(
            InboxStatus.RETRY_PENDING,
            AVAILABLE_AT
        ).retryEligible(AVAILABLE_AT)
    ).isTrue();
    assertThat(
        event(
            InboxStatus.RETRY_PENDING,
            AVAILABLE_AT
        ).retryEligible(AVAILABLE_AT.plusSeconds(1))
    )
        .isTrue();
  }

  @Test
  void doesNotTreatFutureOrTerminalEventsAsRetryEligible() {
    assertThat(
        event(
            InboxStatus.RETRY_PENDING,
            AVAILABLE_AT
        ).retryEligible(AVAILABLE_AT.minusSeconds(1))
    )
        .isFalse();
    assertThat(
        event(
            InboxStatus.FAILED,
            null
        ).retryEligible(AVAILABLE_AT.plusSeconds(1))
    ).isFalse();
  }

  @Test
  void identifiesOnlyFailedEventsAsRetryExhausted() {
    assertThat(
        event(
            InboxStatus.FAILED,
            null
        ).retryExhausted()
    ).isTrue();
    assertThat(
        event(
            InboxStatus.RETRY_PENDING,
            AVAILABLE_AT
        ).retryExhausted()
    ).isFalse();
  }

  @Test
  void allowsNullAvailableAtForNonRetryPendingEvents() {
    assertThat(
        event(
            InboxStatus.PROCESSED,
            null
        ).availableAt()
    ).isNull();
    assertThat(
        event(
            InboxStatus.FAILED,
            null
        ).availableAt()
    ).isNull();
  }

  @Test
  void enforcesAvailableAtLifecycleSemantics() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> event(
                InboxStatus.RETRY_PENDING,
                null
            )
        );
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> event(
                InboxStatus.FAILED,
                AVAILABLE_AT
            )
        );
  }

  @Test
  void requiresNowForRetryEligibility() {
    assertThatNullPointerException()
        .isThrownBy(
            () -> event(
                InboxStatus.RETRY_PENDING,
                AVAILABLE_AT
            ).retryEligible(null)
        );
  }

  private InboxEvent event(
      InboxStatus status,
      Instant availableAt
  ) {
    return InboxEvent.builder()
        .eventId(new EventId("event-1"))
        .eventType("example.event")
        .timestamp(RECEIVED_AT)
        .source("test")
        .payload(
            new SerializedPayload(
                "{}",
                "application/json"
            )
        )
        .status(status)
        .attemptCount(0)
        .receivedAt(RECEIVED_AT)
        .availableAt(availableAt)
        .build();
  }
}
