package com.czetsuyatech.nerv.event.kafka.producer;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.observability.tracing.TraceContextCarrier;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * Publishes generic broker messages to Kafka topics.
 */
@RequiredArgsConstructor
@Slf4j
public class KafkaBrokerProducer implements BrokerProducer {

  public static final BrokerId BROKER_ID = new BrokerId("kafka");
  public static final String EVENT_ID_HEADER = "nerv-event-id";
  public static final String EVENT_TYPE_HEADER = "nerv-event-type";
  public static final String EVENT_SOURCE_HEADER = "nerv-event-source";
  public static final String EVENT_TIMESTAMP_HEADER = "nerv-event-timestamp";
  public static final String CONTENT_TYPE_HEADER = "nerv-event-content-type";
  public static final String CORRELATION_ID_HEADER = "nerv-event-correlation-id";
  @NonNull
  private final KafkaTemplate<String, String> kafkaTemplate;

  @NonNull
  private final Duration sendTimeout;

  @Override
  public BrokerId brokerId() {
    return BROKER_ID;
  }

  @Override
  public BrokerPublishResult publish(BrokerMessage message) {
    if (message == null) {
      throw new NullPointerException("message must not be null");
    }
    log.debug(
        "Publishing event to Kafka eventId={} eventType={} topic={} correlationId={}",
        message.eventId().value(),
        message.eventType(),
        message.target(),
        message.correlationId()
    );
    ProducerRecord<String, String> record = new ProducerRecord<>(
        message.target(),
        null,
        message.payload().value()
    );
    addHeader(
        record,
        EVENT_ID_HEADER,
        message.eventId().value()
    );
    addHeader(
        record,
        EVENT_TYPE_HEADER,
        message.eventType()
    );
    addHeader(
        record,
        EVENT_SOURCE_HEADER,
        message.source()
    );
    addHeader(
        record,
        EVENT_TIMESTAMP_HEADER,
        message.timestamp().toString()
    );
    addHeader(
        record,
        CONTENT_TYPE_HEADER,
        message.payload().contentType()
    );
    if (message.correlationId() != null && !message.correlationId().isBlank()) {
      addHeader(
          record,
          CORRELATION_ID_HEADER,
          message.correlationId()
      );
    }
    TraceContextCarrier.current()
        .forEach(
            (
                name,
                value) -> {
              if (record.headers().lastHeader(name) == null) {
                addHeader(
                    record,
                    name,
                    value
                );
              }
            }
        );
    try {
      SendResult<String, String> sendResult = kafkaTemplate.send(record)
          .get(
              sendTimeout.toMillis(),
              TimeUnit.MILLISECONDS
          );
      RecordMetadata metadata = sendResult.getRecordMetadata();
      if (metadata == null) {
        throw new IllegalStateException("Kafka acknowledgement must include record metadata");
      }
      log.debug(
          "Kafka event published eventId={} eventType={} topic={} partition={} offset={}",
          message.eventId().value(),
          message.eventType(),
          metadata.topic(),
          metadata.partition(),
          metadata.offset()
      );

      return new BrokerPublishResult(metadata.topic() + ":" + metadata.partition() + ":" + metadata.offset());

    } catch (TimeoutException exception) {
      throw new KafkaPublishException(
          "Kafka acknowledgement timed out for eventId=" + message.eventId().value()
              + " topic=" + message.target() + " after " + sendTimeout
              + "; delivery may have succeeded and is ambiguous",
          exception
      );

    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new KafkaPublishException(
          "Kafka publish interrupted for eventId=" + message.eventId().value()
              + " topic=" + message.target(),
          exception
      );

    } catch (ExecutionException exception) {
      throw new KafkaPublishException(
          "Kafka publish failed for eventId=" + message.eventId().value()
              + " topic=" + message.target(),
          exception.getCause()
      );
    }
  }

  private static void addHeader(
      ProducerRecord<String, String> record,
      String name,
      String value
  ) {
    record.headers()
        .add(
            new RecordHeader(
                name,
                value.getBytes(StandardCharsets.UTF_8)
            )
        );
  }
}
