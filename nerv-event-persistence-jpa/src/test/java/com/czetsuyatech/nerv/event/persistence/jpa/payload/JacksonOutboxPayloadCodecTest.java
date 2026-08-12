package com.czetsuyatech.nerv.event.persistence.jpa.payload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.persistence.application.dto.JacksonOutboxPayloadCodec;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class JacksonOutboxPayloadCodecTest {

  private final JacksonOutboxPayloadCodec codec = new JacksonOutboxPayloadCodec(new ObjectMapper());

  @Test
  void serializesAnArbitraryObjectToReadableJsonText() {
    OrderPayload payload = new OrderPayload(
        "123",
        new Customer(
            "456",
            "Ada"
        ),
        new BigDecimal("1200.50"),
        null
    );

    String json = codec.serialize(payload);

    assertThat(json)
        .isEqualTo(
            "{\"orderId\":\"123\",\"customer\":{\"id\":\"456\",\"name\":\"Ada\"},\"amount\":1200.50,\"note\":null}"
        );
  }

  @Test
  void roundTripsNestedCollectionsMapsUnicodeAndNullFields() {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put(
        "order",
        Map.of(
            "id",
            "123"
        )
    );
    payload.put(
        "items",
        List.of(
            Map.of(
                "sku",
                "book",
                "quantity",
                2
            )
        )
    );
    payload.put(
        "message",
        "Salamat, こんにちは, مرحبًا"
    );
    payload.put(
        "optional",
        null
    );

    Object decoded = codec.deserialize(codec.serialize(payload));

    assertThat(decoded).isInstanceOf(Map.class);
    Map<?, ?> decodedPayload = (Map<?, ?>) decoded;
    assertThat(decodedPayload.get("order")).isEqualTo(
        Map.of(
            "id",
            "123"
        )
    );
    assertThat(decodedPayload.get("items")).isEqualTo(
        List.of(
            Map.of(
                "sku",
                "book",
                "quantity",
                2
            )
        )
    );
    assertThat(decodedPayload.get("message")).isEqualTo("Salamat, こんにちは, مرحبًا");
    assertThat(decodedPayload.containsKey("optional")).isTrue();
    assertThat(decodedPayload.get("optional")).isNull();
  }

  @Test
  void rejectsPayloadsThatCannotBeSerialized() {
    SelfReferencingPayload payload = new SelfReferencingPayload();

    assertThatThrownBy(() -> codec.serialize(payload))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to serialize outbox payload as JSON");
  }

  @Test
  void rejectsMalformedPersistedJson() {
    assertThatThrownBy(() -> codec.deserialize("{not-json"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to deserialize outbox payload JSON");
  }

  private record OrderPayload(
      String orderId,
      Customer customer,
      BigDecimal amount,
      String note
  )
  {

  }

  private record Customer(
      String id,
      String name
  )
  {

  }

  private static final class SelfReferencingPayload {

    private final SelfReferencingPayload self = this;

    public SelfReferencingPayload getSelf() {
      return self;
    }
  }
}
