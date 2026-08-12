package com.czetsuyatech.nerv.event.spring.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class JacksonEventDeserializerTest {

  @Test
  void readsTextualJsonIntoTheExplicitPayloadType() {
    JacksonEventDeserializer deserializer = new JacksonEventDeserializer(new ObjectMapper());

    OrderCreated payload = deserializer.deserialize(
        new SerializedPayload(
            "{\"orderId\":42}",
            "application/json"
        ),
        OrderCreated.class
    );

    assertThat(payload.orderId()).isEqualTo(42);
  }

  private record OrderCreated(int orderId) {
  }
}
