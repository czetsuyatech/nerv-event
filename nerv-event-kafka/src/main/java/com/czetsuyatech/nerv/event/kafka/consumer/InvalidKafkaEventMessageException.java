package com.czetsuyatech.nerv.event.kafka.consumer;

import com.czetsuyatech.nerv.event.exception.InvalidEventMessageException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Identifies missing or invalid generic event metadata on a Kafka record.
 */
public class InvalidKafkaEventMessageException extends InvalidEventMessageException {

  public InvalidKafkaEventMessageException(
      ConsumerRecord<?, ?> record,
      String reason,
      Throwable cause
  )
  {
    super(
        "Invalid Kafka event message topic=" + record.topic()
            + " partition=" + record.partition()
            + " offset=" + record.offset()
            + " reason=" + reason,
        cause
    );
  }

  public InvalidKafkaEventMessageException(
      ConsumerRecord<?, ?> record,
      String reason
  )
  {
    this(
        record,
        reason,
        null
    );
  }
}
