package com.czetsuyatech.nerv.event.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class EventIdTest {

  @Test
  void createsIdDirectlyAndWithBuilder() {
    assertThat(new EventId("evt-1").value()).isEqualTo("evt-1");
    assertThat(EventId.builder().value("evt-2").build()).isEqualTo(new EventId("evt-2"));
  }

  @ParameterizedTest
  @EmptySource
  @ValueSource(strings = {" ", "\t"})
  void rejectsBlankValues(String value) {
    assertThatIllegalArgumentException().isThrownBy(() -> new EventId(value));
  }

  @Test
  void rejectsNullValue() {
    assertThatNullPointerException().isThrownBy(() -> new EventId(null));
  }
}
