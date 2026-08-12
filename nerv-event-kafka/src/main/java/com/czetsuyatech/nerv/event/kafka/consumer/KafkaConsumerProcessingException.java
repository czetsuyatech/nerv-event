package com.czetsuyatech.nerv.event.kafka.consumer;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Adds Kafka delivery diagnostics when generic consumer processing fails.
 */
public class KafkaConsumerProcessingException extends IllegalStateException {

  public KafkaConsumerProcessingException(
      ConsumerRecord<?, ?> record,
      ConsumerMessage message,
      RuntimeException cause
  )
  {
    this(
        record,
        message,
        "HANDLER_EXECUTION",
        cause
    );
  }

  public KafkaConsumerProcessingException(
      ConsumerRecord<?, ?> record,
      ConsumerMessage message,
      String stage,
      RuntimeException cause
  )
  {
    super(
        "Kafka inbox processing failed stage=" + stage
            + " eventId=" + message.eventId().value()
            + " eventType=" + message.eventType()
            + " topic=" + record.topic()
            + " partition=" + record.partition()
            + " offset=" + record.offset(),
        cause
    );
  }
}
