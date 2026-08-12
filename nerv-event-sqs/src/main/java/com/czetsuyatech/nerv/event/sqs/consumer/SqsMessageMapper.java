package com.czetsuyatech.nerv.event.sqs.consumer;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.sqs.producer.SqsBrokerProducer;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.springframework.messaging.Message;

/**
 * Converts Spring Cloud AWS messages to the broker-neutral consumer contract.
 */
public final class SqsMessageMapper {
  public ConsumerMessage toConsumerMessage(Message<String> message) {
    if (message == null) {
      throw new NullPointerException("message must not be null");
    }
    if (message.getPayload() == null) {
      throw new InvalidSqsEventMessageException("message body must not be null");
    }
    try {
      return new ConsumerMessage(
          new EventId(
              required(
                  message,
                  SqsBrokerProducer.EVENT_ID_ATTRIBUTE
              )
          ),
          required(
              message,
              SqsBrokerProducer.EVENT_TYPE_ATTRIBUTE
          ),
          timestamp(message),
          required(
              message,
              SqsBrokerProducer.EVENT_SOURCE_ATTRIBUTE
          ),
          optional(
              message,
              SqsBrokerProducer.CORRELATION_ID_ATTRIBUTE
          ),
          new SerializedPayload(
              message.getPayload(),
              required(
                  message,
                  SqsBrokerProducer.CONTENT_TYPE_ATTRIBUTE
              )
          )
      );
    } catch (InvalidSqsEventMessageException exception) {
      throw exception;
    } catch (IllegalArgumentException exception) {
      throw new InvalidSqsEventMessageException(
          "invalid generic event metadata",
          exception
      );
    }
  }

  private static Instant timestamp(Message<String> message) {
    try {
      return Instant.parse(
          required(
              message,
              SqsBrokerProducer.EVENT_TIMESTAMP_ATTRIBUTE
          )
      );
    } catch (DateTimeParseException exception) {
      throw new InvalidSqsEventMessageException(
          "invalid attribute '"
              + SqsBrokerProducer.EVENT_TIMESTAMP_ATTRIBUTE + "'",
          exception
      );
    }
  }

  private static String required(
      Message<String> message,
      String name
  ) {
    String value = optional(
        message,
        name
    );
    if (value == null || value.isBlank()) {
      throw new InvalidSqsEventMessageException("missing required attribute '" + name + "'");
    }
    return value;
  }

  private static String optional(
      Message<String> message,
      String name
  ) {
    Object value = message.getHeaders().get(name);
    return value == null ? null : value.toString();
  }
}
