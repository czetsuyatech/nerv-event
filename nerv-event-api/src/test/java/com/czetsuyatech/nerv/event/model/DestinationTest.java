package com.czetsuyatech.nerv.event.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class DestinationTest {

  @Test
  void createsDestinationDirectlyAndWithBuilder() {
    assertThat(new Destination("orders.created").name()).isEqualTo("orders.created");
    assertThat(Destination.builder().name("orders.cancelled").build())
        .isEqualTo(new Destination("orders.cancelled"));
  }

  @ParameterizedTest
  @EmptySource
  @ValueSource(strings = {" ", "\t"})
  void rejectsBlankLogicalNames(String logicalName) {
    assertThatIllegalArgumentException().isThrownBy(() -> new Destination(logicalName));
  }

  @Test
  void rejectsNullLogicalName() {
    assertThatNullPointerException().isThrownBy(() -> new Destination(null));
  }
}
