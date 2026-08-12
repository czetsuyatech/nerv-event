package com.czetsuyatech.nerv.event.sqs.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import com.czetsuyatech.nerv.event.sqs.producer.SqsBrokerProducer;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.support.MessageBuilder;

class SqsMessageMapperTest {
  private final SqsMessageMapper mapper = new SqsMessageMapper();

  @Test
  void mapsBodyAndAllGenericMetadataWithoutReserialization() {
    ConsumerMessage mapped = mapper.toConsumerMessage(validMessage(true));
    assertThat(mapped.eventId().value()).isEqualTo("event-1");
    assertThat(mapped.eventType()).isEqualTo("OrderCreated");
    assertThat(mapped.source()).isEqualTo("orders-service");
    assertThat(mapped.timestamp()).hasToString("2026-08-18T00:00:00Z");
    assertThat(mapped.correlationId()).isEqualTo("corr-1");
    assertThat(mapped.payload().contentType()).isEqualTo("application/json");
    assertThat(mapped.payload().value()).isEqualTo("{\"order\":42}");
  }

  @Test
  void optionalCorrelationIdMayBeAbsent() {
    assertThat(mapper.toConsumerMessage(validMessage(false)).correlationId()).isNull();
  }

  @Test
  void missingRequiredAttributeAndMalformedTimestampFailExplicitly() {
    assertThatThrownBy(
        () -> mapper.toConsumerMessage(
            MessageBuilder.withPayload("{}")
                .setHeader(
                    SqsBrokerProducer.EVENT_ID_ATTRIBUTE,
                    "event-1"
                )
                .build()
        )
    )
        .isInstanceOf(InvalidSqsEventMessageException.class)
        .hasMessageContaining("nerv-event-type");
    assertThatThrownBy(
        () -> mapper.toConsumerMessage(
            MessageBuilder.fromMessage(validMessage(false))
                .setHeader(
                    SqsBrokerProducer.EVENT_TIMESTAMP_ATTRIBUTE,
                    "not-an-instant"
                )
                .build()
        )
    )
        .isInstanceOf(InvalidSqsEventMessageException.class)
        .hasMessage("invalid attribute 'nerv-event-timestamp'");
  }

  static org.springframework.messaging.Message<String> validMessage(boolean correlation) {
    MessageBuilder<String> builder = MessageBuilder.withPayload("{\"order\":42}")
        .setHeader(
            SqsBrokerProducer.EVENT_ID_ATTRIBUTE,
            "event-1"
        )
        .setHeader(
            SqsBrokerProducer.EVENT_TYPE_ATTRIBUTE,
            "OrderCreated"
        )
        .setHeader(
            SqsBrokerProducer.EVENT_SOURCE_ATTRIBUTE,
            "orders-service"
        )
        .setHeader(
            SqsBrokerProducer.EVENT_TIMESTAMP_ATTRIBUTE,
            "2026-08-18T00:00:00Z"
        )
        .setHeader(
            SqsBrokerProducer.CONTENT_TYPE_ATTRIBUTE,
            "application/json"
        )
        .setHeader(
            "Sqs_RawMessageId",
            "sqs-message-1"
        );
    if (correlation) {
      builder.setHeader(
          SqsBrokerProducer.CORRELATION_ID_ATTRIBUTE,
          "corr-1"
      );
    }
    return builder.build();
  }
}
