package com.czetsuyatech.nerv.event.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.observability.autoconfigure.NervEventObservabilityProperties;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;

class NervEventBacklogHealthIndicatorTest {
  private static final Clock CLOCK = Clock.fixed(
      Instant.parse("2026-08-30T00:00:00Z"),
      ZoneOffset.UTC
  );

  @Test
  void reportsDownWhenOutboxPendingThresholdIsExceeded() {
    NervEventObservabilityProperties.Health.Outbox thresholds = new NervEventObservabilityProperties.Health.Outbox();
    thresholds.setMaxPending(10L);

    assertThat(
        new NervEventOutboxHealthIndicator(
            new FixedOutboxMetrics(),
            null,
            CLOCK,
            thresholds
        )
            .health()
            .getStatus()
    ).isEqualTo(Status.DOWN);
  }

  @Test
  void reportsDownWhenInboxRetryAgeThresholdIsExceeded() {
    NervEventObservabilityProperties.Health.Inbox thresholds = new NervEventObservabilityProperties.Health.Inbox();
    thresholds.setMaxOldestRetryAge(java.time.Duration.ofSeconds(30));

    assertThat(
        new NervEventInboxHealthIndicator(
            new FixedInboxMetrics(),
            null,
            CLOCK,
            thresholds
        )
            .health()
            .getStatus()
    ).isEqualTo(Status.DOWN);
  }

  private static final class FixedOutboxMetrics implements OutboxOperationalMetrics {
    @Override
    public long count(OutboxStatus status) {
      return status == OutboxStatus.PENDING ? 11 : 0;
    }

    @Override
    public Optional<Instant> oldestPendingAt() {
      return Optional.empty();
    }
  }

  private static final class FixedInboxMetrics implements InboxOperationalMetrics {
    @Override
    public long count(InboxStatus status) {
      return status == InboxStatus.RETRY_PENDING ? 1 : 0;
    }

    @Override
    public Optional<Instant> oldestRetryPendingAt() {
      return Optional.of(CLOCK.instant().minusSeconds(31));
    }
  }
}
