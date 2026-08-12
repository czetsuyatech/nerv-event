package com.czetsuyatech.nerv.event.kafka.consumer;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import com.czetsuyatech.nerv.event.exception.NervEventException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Identifies the distinct state where handling succeeded but Kafka acknowledgement failed.
 */
public class KafkaAcknowledgementException extends NervEventException {

  public KafkaAcknowledgementException(
      ConsumerRecord<?, ?> record,
      ConsumerMessage message,
      RuntimeException cause
  )
  {
    super(
        "Kafka acknowledgement failed after durable inbox outcome eventId=" + message.eventId().value()
            + " topic=" + record.topic()
            + " partition=" + record.partition()
            + " offset=" + record.offset(),
        cause
    );
  }
}
