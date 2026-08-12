package com.czetsuyatech.nerv.event.core.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

class DispatchResultTest {

  @Test
  void emptyContainsZeroForEveryCounter() {
    assertThat(DispatchResult.empty()).isEqualTo(
        new DispatchResult(
            0,
            0,
            0,
            0,
            0
        )
    );
  }

  @Test
  void enforcesTheClaimedOutcomeInvariant() {
    assertThatIllegalArgumentException().isThrownBy(
        () -> new DispatchResult(
            2,
            1,
            0,
            0,
            0
        )
    );
  }

  @Test
  void rejectsNegativeCounters() {
    assertThatIllegalArgumentException().isThrownBy(
        () -> new DispatchResult(
            -1,
            0,
            0,
            0,
            0
        )
    );
  }

  @Test
  void isImmutable() {
    assertThat(DispatchResult.class.isRecord()).isTrue();
    assertThat(DispatchResult.class.getDeclaredFields())
        .allMatch(field -> field.isSynthetic() || Modifier.isFinal(field.getModifiers()));
  }
}
