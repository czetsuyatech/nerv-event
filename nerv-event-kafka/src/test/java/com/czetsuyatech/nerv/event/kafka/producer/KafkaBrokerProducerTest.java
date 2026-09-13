package com.czetsuyatech.nerv.event.kafka.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class KafkaBrokerProducerTest {

  @Test
  void ownsTheKafkaBrokerId() {
    KafkaBrokerProducer producer = new KafkaBrokerProducer(
        mock(KafkaTemplate.class),
        Duration.ofSeconds(1)
    );

    assertThat(producer.brokerId()).isEqualTo(KafkaBrokerProducer.BROKER_ID);
    assertThat(producer.brokerId().value()).isEqualTo("kafka");
  }

  @Test
  void sendsTheTextPayloadAndGenericMetadataHeaders() {
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    SendResult<String, String> sendResult = mock(SendResult.class);
    RecordMetadata metadata = mock(RecordMetadata.class);
    when(metadata.topic()).thenReturn("orders-topic");
    when(metadata.partition()).thenReturn(2);
    when(metadata.offset()).thenReturn(42L);
    when(sendResult.getRecordMetadata()).thenReturn(metadata);
    when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(sendResult));
    KafkaBrokerProducer producer = new KafkaBrokerProducer(
        kafkaTemplate,
        Duration.ofSeconds(1)
    );

    BrokerPublishResult result = producer.publish(message("correlation-1"));

    ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(kafkaTemplate).send(recordCaptor.capture());
    ProducerRecord<String, String> record = recordCaptor.getValue();
    assertThat(record.topic()).isEqualTo("orders-topic");
    assertThat(record.key()).isNull();
    assertThat(record.value()).isEqualTo("{\"orderId\":42}");
    assertThat(
        headerValue(
            record,
            KafkaBrokerProducer.EVENT_ID_HEADER
        )
    ).isEqualTo("event-1");
    assertThat(
        headerValue(
            record,
            KafkaBrokerProducer.EVENT_TYPE_HEADER
        )
    ).isEqualTo("order.created");
    assertThat(
        headerValue(
            record,
            KafkaBrokerProducer.EVENT_SOURCE_HEADER
        )
    ).isEqualTo("orders");
    assertThat(
        headerValue(
            record,
            KafkaBrokerProducer.EVENT_TIMESTAMP_HEADER
        )
    )
        .isEqualTo("2026-08-15T00:00:00Z");
    assertThat(
        headerValue(
            record,
            KafkaBrokerProducer.CONTENT_TYPE_HEADER
        )
    ).isEqualTo("application/json");
    assertThat(
        headerValue(
            record,
            KafkaBrokerProducer.CORRELATION_ID_HEADER
        )
    ).isEqualTo("correlation-1");
    assertThat(result).isEqualTo(new BrokerPublishResult("orders-topic:2:42"));
  }

  @Test
  void omitsTheCorrelationHeaderWhenTheEventHasNoCorrelationId() {
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    SendResult<String, String> sendResult = mock(SendResult.class);
    RecordMetadata metadata = mock(RecordMetadata.class);
    when(metadata.topic()).thenReturn("orders-topic");
    when(sendResult.getRecordMetadata()).thenReturn(metadata);
    when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(sendResult));
    KafkaBrokerProducer producer = new KafkaBrokerProducer(
        kafkaTemplate,
        Duration.ofSeconds(1)
    );

    producer.publish(message(null));

    ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(kafkaTemplate).send(recordCaptor.capture());
    assertThat(recordCaptor.getValue().headers().lastHeader(KafkaBrokerProducer.CORRELATION_ID_HEADER)).isNull();
  }

  @Test
  void usesTheOrderingKeyAsTheKafkaRecordKey() {
    KafkaTemplate<String, String> kafkaTemplate = acknowledgedKafkaTemplate();
    KafkaBrokerProducer producer = new KafkaBrokerProducer(kafkaTemplate, Duration.ofSeconds(1));

    producer.publish(message(null, "customer-42"));

    ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(kafkaTemplate).send(recordCaptor.capture());
    assertThat(recordCaptor.getValue().key()).isEqualTo("customer-42");
  }

  @Test
  void omitsTheCorrelationHeaderWhenTheEventHasABlankCorrelationId() {
    KafkaTemplate<String, String> kafkaTemplate = acknowledgedKafkaTemplate();
    KafkaBrokerProducer producer = new KafkaBrokerProducer(
        kafkaTemplate,
        Duration.ofSeconds(1)
    );

    producer.publish(message(" "));

    ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(kafkaTemplate).send(recordCaptor.capture());
    assertThat(recordCaptor.getValue().headers().lastHeader(KafkaBrokerProducer.CORRELATION_ID_HEADER)).isNull();
  }

  @Test
  void propagatesKafkaSendFailuresWithTheirOriginalCause() {
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    IllegalArgumentException sendFailure = new IllegalArgumentException("broker unavailable");
    when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(
        CompletableFuture.failedFuture(sendFailure)
    );
    KafkaBrokerProducer producer = new KafkaBrokerProducer(
        kafkaTemplate,
        Duration.ofSeconds(1)
    );

    assertThatThrownBy(() -> producer.publish(message(null)))
        .isInstanceOf(KafkaPublishException.class)
        .hasMessageContaining("eventId=event-1")
        .hasMessageContaining("topic=orders-topic")
        .cause()
        .isSameAs(sendFailure);
  }

  @Test
  void reportsTimeoutAsAnAmbiguousDelivery() {
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<SendResult<String, String>>());
    KafkaBrokerProducer producer = new KafkaBrokerProducer(
        kafkaTemplate,
        Duration.ofMillis(1)
    );

    assertThatThrownBy(() -> producer.publish(message(null)))
        .isInstanceOf(KafkaPublishException.class)
        .hasMessageContaining("eventId=event-1")
        .hasMessageContaining("topic=orders-topic")
        .hasMessageContaining("PT0.001S")
        .hasMessageContaining("ambiguous")
        .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
  }

  private static KafkaTemplate<String, String> acknowledgedKafkaTemplate() {
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    SendResult<String, String> sendResult = mock(SendResult.class);
    RecordMetadata metadata = mock(RecordMetadata.class);
    when(metadata.topic()).thenReturn("orders-topic");
    when(sendResult.getRecordMetadata()).thenReturn(metadata);
    when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(sendResult));
    return kafkaTemplate;
  }

  private static BrokerMessage message(String correlationId) {
    return message(correlationId, null);
  }

  private static BrokerMessage message(
      String correlationId,
      String orderingKey
  ) {
    return new BrokerMessage(
        new EventId("event-1"),
        "order.created",
        Instant.parse("2026-08-15T00:00:00Z"),
        "orders",
        correlationId,
        orderingKey,
        "orders-topic",
        new SerializedPayload(
            "{\"orderId\":42}",
            "application/json"
        )
    );
  }

  private static String headerValue(
      ProducerRecord<String, String> record,
      String headerName
  ) {
    Header header = record.headers().lastHeader(headerName);
    return new String(
        header.value(),
        StandardCharsets.UTF_8
    );
  }
}
