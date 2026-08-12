package com.czetsuyatech.nerv.event.spring.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.outbox.DispatchResult;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class DispatcherPollingPolicyTest {

  @Test
  void startsAtTheMinimumIntervalAndResetsWhenWorkIsClaimed() {
    DispatcherPollingPolicy policy = policy(() -> 0.5);
    Duration backedOff = policy.nextBaseDelay(
        Duration.ofSeconds(8),
        DispatchResult.empty()
    );

    assertThat(policy.initialBaseDelay()).isEqualTo(Duration.ofSeconds(1));
    assertThat(
        policy.nextBaseDelay(
            backedOff,
            new DispatchResult(
                1,
                1,
                0,
                0,
                0
            )
        )
    )
        .isEqualTo(Duration.ofSeconds(1));
  }

  @Test
  void increasesEmptyPollsByTheConfiguredMultiplierWithoutExceedingTheMaximum() {
    DispatcherPollingPolicy policy = policy(() -> 0.5);
    Duration delay = policy.initialBaseDelay();

    for (int index = 0; index < 6; index++) {
      delay = policy.nextBaseDelay(
          delay,
          DispatchResult.empty()
      );
    }

    assertThat(delay).isEqualTo(Duration.ofSeconds(30));
    assertThat(
        policy.nextBaseDelay(
            delay,
            DispatchResult.empty()
        )
    ).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void appliesJitterWithinTheConfiguredBoundsWithoutChangingTheBaseDelay() {
    Duration baseDelay = Duration.ofSeconds(10);

    assertThat(policy(() -> 0.0).jitteredDelay(baseDelay)).isEqualTo(Duration.ofSeconds(9));
    assertThat(policy(() -> 1.0).jitteredDelay(baseDelay)).isEqualTo(Duration.ofSeconds(11));
    assertThat(baseDelay).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  void doesNotAllowPositiveJitterToExceedTheMaximumInterval() {
    DispatcherPollingPolicy policy = policy(() -> 1.0);

    assertThat(policy.jitteredDelay(Duration.ofSeconds(30))).isEqualTo(Duration.ofSeconds(30));
  }

  private static DispatcherPollingPolicy policy(java.util.function.DoubleSupplier random) {
    return new DispatcherPollingPolicy(
        Duration.ofSeconds(1),
        Duration.ofSeconds(30),
        2.0,
        0.10,
        random
    );
  }
}
