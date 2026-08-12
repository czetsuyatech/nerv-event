package com.czetsuyatech.nerv.event.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerState;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatus;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatusProvider;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;

class NervEventSchedulersHealthIndicatorTest {

  private static final Instant NOW = Instant.parse("2026-08-30T12:00:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
  private static final Duration GRACE = Duration.ofSeconds(30);

  @Test
  void activeWorkUsesItsActualStartTimeInsteadOfThePreviousMaximumInterval() {
    SchedulerStatus status = status(
        SchedulerState.RUNNING,
        false,
        true,
        NOW.minus(Duration.ofMinutes(33)),
        NOW.minusSeconds(10),
        null
    );

    assertThat(indicator(status).health().getStatus()).isEqualTo(Status.UP);
  }

  @Test
  void activeWorkBecomesStaleAfterTheExecutionGrace() {
    SchedulerStatus status = status(
        SchedulerState.RUNNING,
        false,
        true,
        NOW.minus(Duration.ofMinutes(33)),
        NOW.minusSeconds(31),
        null
    );

    assertThat(indicator(status).health().getStatus()).isEqualTo(Status.DOWN);
    assertThat(indicator(status).health().getDetails().toString()).contains("stale=true");
  }

  @Test
  void failedIsDownWhileIntentionallyStoppedIsUp() {
    assertThat(
        indicator(status(SchedulerState.FAILED, false, false, NOW, null, null)).health().getStatus()
    ).isEqualTo(Status.DOWN);
    assertThat(
        indicator(status(SchedulerState.STOPPED, false, false, NOW, null, null)).health().getStatus()
    ).isEqualTo(Status.UP);
  }

  private static NervEventSchedulersHealthIndicator indicator(SchedulerStatus status) {
    SchedulerStatusProvider provider = new SchedulerStatusProvider() {
      @Override
      public SchedulerType schedulerType() {
        return SchedulerType.EVENT_RETENTION;
      }

      @Override
      public SchedulerStatus status() {
        return status;
      }
    };
    return new NervEventSchedulersHealthIndicator(List.of(provider), CLOCK, GRACE);
  }

  private static SchedulerStatus status(
      SchedulerState state,
      boolean scheduled,
      boolean workInProgress,
      Instant lastSuccessfulCompletionAt,
      Instant lastStartedAt,
      Instant nextExecutionAt
  ) {
    return new SchedulerStatus(
        state,
        scheduled,
        workInProgress,
        NOW.minus(Duration.ofHours(1)),
        lastStartedAt,
        lastSuccessfulCompletionAt,
        null,
        null,
        null,
        null,
        nextExecutionAt,
        0,
        0,
        0,
        Duration.ofMinutes(30)
    );
  }
}
