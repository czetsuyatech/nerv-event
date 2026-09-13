package com.czetsuyatech.nerv.event.kafka.consumer;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import com.czetsuyatech.nerv.event.core.consumer.DefaultInboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.consumer.InboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.inbox.InboxCompletionException;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.kafka.producer.KafkaBrokerProducer;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics.ConsumerHandlerResult;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics.ConsumerOutcome;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * Connects Kafka delivery to durable inbox state before acknowledging a record.
 */
@Slf4j
public class KafkaConsumerAdapter implements AcknowledgingMessageListener<String, String> {

  @NonNull
  private final ConsumerDispatcher consumerDispatcher;

  @NonNull
  private final InboxService inboxService;

  @NonNull
  private final Clock clock;

  @NonNull
  private final String processingOwner;

  @NonNull
  private final Duration leaseDuration;

  @NonNull
  private final InboxRetryPolicy inboxRetryPolicy;

  @NonNull
  private final InboxFailureClassifier failureClassifier;

  @NonNull
  private final ConsumerMetrics consumerMetrics;

  public KafkaConsumerAdapter(
      ConsumerDispatcher consumerDispatcher,
      InboxService inboxService,
      Clock clock,
      String processingOwner,
      Duration leaseDuration,
      InboxRetryPolicy inboxRetryPolicy
  )
  {
    this(
        consumerDispatcher,
        inboxService,
        clock,
        processingOwner,
        leaseDuration,
        inboxRetryPolicy,
        new DefaultInboxFailureClassifier(),
        ConsumerMetrics.noop()
    );
  }

  public KafkaConsumerAdapter(
      ConsumerDispatcher consumerDispatcher,
      InboxService inboxService,
      Clock clock,
      String processingOwner,
      Duration leaseDuration,
      InboxRetryPolicy inboxRetryPolicy,
      InboxFailureClassifier failureClassifier
  )
  {
    this(
        consumerDispatcher,
        inboxService,
        clock,
        processingOwner,
        leaseDuration,
        inboxRetryPolicy,
        failureClassifier,
        ConsumerMetrics.noop()
    );
  }

  public KafkaConsumerAdapter(
      ConsumerDispatcher consumerDispatcher,
      InboxService inboxService,
      Clock clock,
      String processingOwner,
      Duration leaseDuration,
      InboxRetryPolicy inboxRetryPolicy,
      InboxFailureClassifier failureClassifier,
      ConsumerMetrics consumerMetrics
  )
  {
    this.consumerDispatcher = java.util.Objects.requireNonNull(
        consumerDispatcher,
        "consumerDispatcher must not be null"
    );
    this.inboxService = java.util.Objects.requireNonNull(
        inboxService,
        "inboxService must not be null"
    );
    this.clock = java.util.Objects.requireNonNull(
        clock,
        "clock must not be null"
    );
    this.processingOwner = java.util.Objects.requireNonNull(
        processingOwner,
        "processingOwner must not be null"
    );
    this.leaseDuration = java.util.Objects.requireNonNull(
        leaseDuration,
        "leaseDuration must not be null"
    );
    this.inboxRetryPolicy = java.util.Objects.requireNonNull(
        inboxRetryPolicy,
        "inboxRetryPolicy must not be null"
    );
    this.failureClassifier = java.util.Objects.requireNonNull(
        failureClassifier,
        "failureClassifier must not be null"
    );
    this.consumerMetrics = java.util.Objects.requireNonNull(
        consumerMetrics,
        "consumerMetrics must not be null"
    );
  }

  public KafkaConsumerAdapter forConsumer(
      String consumerName,
      Duration consumerLeaseDuration
  ) {
    if (consumerName == null || consumerName.isBlank()) {
      throw new IllegalArgumentException("consumerName must not be blank");
    }
    return new KafkaConsumerAdapter(
        consumerDispatcher,
        inboxService,
        clock,
        processingOwner + ":" + consumerName,
        consumerLeaseDuration,
        inboxRetryPolicy,
        failureClassifier,
        consumerMetrics
    );
  }

  @Override
  public void onMessage(
      ConsumerRecord<String, String> record,
      Acknowledgment acknowledgment
  ) {
    ConsumerMessage message;
    try {
      message = toConsumerMessage(record);
    } catch (InvalidKafkaEventMessageException exception) {
      log.warn(
          "Kafka event message rejected stage=REGISTER topic={} partition={} offset={} reason={}",
          record.topic(),
          record.partition(),
          record.offset(),
          exception.getMessage()
      );
      throw exception;
    }
    log.debug(
        "Kafka event received eventId={} eventType={} topic={} partition={} offset={} correlationId={} owner={}",
        message.eventId().value(),
        message.eventType(),
        record.topic(),
        record.partition(),
        record.offset(),
        message.correlationId(),
        processingOwner
    );
    observe(ConsumerMetrics::received);
    InboxRegistration registration = register(
        record,
        message
    );
    if (!registration.created() && acknowledgeDurableDuplicate(
        record,
        message,
        registration.event(),
        acknowledgment
    )) {
      return;
    }
    InboxEvent claimed = claim(
        record,
        message
    );
    log.debug(
        "Kafka Inbox event claimed eventId={} eventType={} owner={} attemptCount={}",
        message.eventId().value(),
        message.eventType(),
        processingOwner,
        claimed.attemptCount()
    );
    long handlerStartedAt = System.nanoTime();
    observe(ConsumerMetrics::handlerExecutionStarted);
    try {
      log.debug(
          "Kafka event handler starting eventId={} eventType={} topic={} partition={} offset={} owner={} attemptCount={}",
          message.eventId().value(),
          message.eventType(),
          record.topic(),
          record.partition(),
          record.offset(),
          processingOwner,
          claimed.attemptCount()
      );
      inboxService.process(
          message.eventId(),
          processingOwner,
          clock.instant(),
          () -> consumerDispatcher.dispatch(message)
      );
      observe(
          metrics -> metrics.handlerExecutionCompleted(
              ConsumerHandlerResult.SUCCESS,
              Duration.ofNanos(System.nanoTime() - handlerStartedAt)
          )
      );
    } catch (InboxCompletionException exception) {
      observe(
          metrics -> metrics.handlerExecutionCompleted(
              ConsumerHandlerResult.SUCCESS,
              Duration.ofNanos(System.nanoTime() - handlerStartedAt)
          )
      );
      observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
      throw new KafkaConsumerProcessingException(record, message, "MARK_PROCESSED", exception);
    } catch (RuntimeException exception) {
      observe(
          metrics -> metrics.handlerExecutionCompleted(
              ConsumerHandlerResult.FAILURE,
              Duration.ofNanos(System.nanoTime() - handlerStartedAt)
          )
      );
      persistHandlerFailureThenAcknowledge(
          record,
          message,
          claimed,
          acknowledgment,
          exception
      );
      return;
    }
    log.debug(
        "Kafka event handler succeeded eventId={} eventType={} topic={} partition={} offset={} owner={}",
        message.eventId().value(),
        message.eventType(),
        record.topic(),
        record.partition(),
        record.offset(),
        processingOwner
    );
    acknowledgeProcessed(
        record,
        message,
        claimed,
        acknowledgment
    );
  }

  private InboxRegistration register(
      ConsumerRecord<String, String> record,
      ConsumerMessage message
  ) {
    Instant receivedAt = clock.instant();
    InboxEvent event = new InboxEvent(
        message.eventId(),
        message.eventType(),
        message.timestamp(),
        message.source(),
        message.correlationId(),
        message.payload(),
        InboxStatus.RECEIVED,
        0,
        receivedAt,
        null,
        null,
        null,
        null,
        null,
        null
    );
    try {
      InboxRegistration registration = inboxService.register(event);
      log.debug(
          "Kafka Inbox event registered eventId={} eventType={} status={} consumer={}",
          message.eventId().value(),
          message.eventType(),
          registration.event().status(),
          processingOwner
      );
      return registration;
    } catch (RuntimeException exception) {
      observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
      log.error(
          "Kafka inbox registration failed stage=REGISTER eventId={} eventType={} topic={} partition={} offset={} owner={}",
          message.eventId().value(),
          message.eventType(),
          record.topic(),
          record.partition(),
          record.offset(),
          processingOwner,
          exception
      );
      throw new KafkaConsumerProcessingException(
          record,
          message,
          "REGISTER",
          exception
      );
    }
  }

  private boolean acknowledgeDurableDuplicate(
      ConsumerRecord<String, String> record,
      ConsumerMessage message,
      InboxEvent event,
      Acknowledgment acknowledgment
  ) {
    if (event.status() != InboxStatus.PROCESSED
        && event.status() != InboxStatus.RETRY_PENDING
        && event.status() != InboxStatus.FAILED) {
      return false;
    }
    log.debug(
        "Kafka duplicate resolved from durable inbox eventId={} eventType={} topic={} partition={} offset={} status={} owner={}",
        message.eventId().value(),
        message.eventType(),
        record.topic(),
        record.partition(),
        record.offset(),
        event.status(),
        processingOwner
    );
    observe(metrics -> metrics.outcome(ConsumerOutcome.DUPLICATE));
    acknowledge(
        record,
        message,
        acknowledgment,
        event.status()
    );
    return true;
  }

  private InboxEvent claim(
      ConsumerRecord<String, String> record,
      ConsumerMessage message
  ) {
    try {
      return inboxService.claim(
          message.eventId(),
          clock.instant(),
          processingOwner,
          leaseDuration
      )
          .orElseThrow(() -> new IllegalStateException("Inbox event has an active processing lease"));
    } catch (RuntimeException exception) {
      observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
      log.warn(
          "Kafka inbox claim unresolved stage=CLAIM eventId={} eventType={} topic={} partition={} offset={} owner={}",
          message.eventId().value(),
          message.eventType(),
          record.topic(),
          record.partition(),
          record.offset(),
          processingOwner,
          exception
      );
      throw new KafkaConsumerProcessingException(
          record,
          message,
          "CLAIM",
          exception
      );
    }
  }

  private void acknowledgeProcessed(
      ConsumerRecord<String, String> record,
      ConsumerMessage message,
      InboxEvent claimed,
      Acknowledgment acknowledgment
  ) {
    log.debug(
        "Inbox event processed eventId={} eventType={} attemptCount={}",
        message.eventId().value(),
        message.eventType(),
        claimed.attemptCount()
    );
    observe(metrics -> metrics.outcome(ConsumerOutcome.PROCESSED));
    acknowledge(
        record,
        message,
        acknowledgment,
        InboxStatus.PROCESSED
    );
  }

  private void persistHandlerFailureThenAcknowledge(
      ConsumerRecord<String, String> record,
      ConsumerMessage message,
      InboxEvent claimed,
      Acknowledgment acknowledgment,
      RuntimeException handlerFailure
  ) {
    int attemptCount = claimed.attemptCount() + 1;
    Instant failedAt = clock.instant();
    boolean retryable;
    try {
      retryable = failureClassifier.isRetryable(handlerFailure);
    } catch (RuntimeException exception) {
      observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
      throw exception;
    }
    if (!retryable) {
      log.warn(
          "Event processing failed with non-retryable error eventId={} eventType={} attemptCount={} failureType={}",
          message.eventId().value(),
          message.eventType(),
          attemptCount,
          handlerFailure.getClass().getSimpleName(),
          handlerFailure
      );
      markFailedThenAcknowledge(
          record,
          message,
          acknowledgment,
          handlerFailure,
          attemptCount,
          failedAt,
          false
      );
      return;
    }
    log.warn(
        "Event processing failed with retryable error eventId={} eventType={} attemptCount={} retryable=true",
        message.eventId().value(),
        message.eventType(),
        attemptCount,
        handlerFailure
    );
    boolean retryAllowed;
    try {
      retryAllowed = inboxRetryPolicy.canRetry(attemptCount);
    } catch (RuntimeException exception) {
      observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
      throw exception;
    }
    log.debug(
        "Inbox retry policy evaluated eventId={} eventType={} attemptCount={} retryable={}",
        message.eventId().value(),
        message.eventType(),
        attemptCount,
        retryAllowed
    );
    if (retryAllowed) {
      markRetryPendingThenAcknowledge(
          record,
          message,
          acknowledgment,
          handlerFailure,
          attemptCount,
          failedAt
      );
      return;
    }
    markFailedThenAcknowledge(
        record,
        message,
        acknowledgment,
        handlerFailure,
        attemptCount,
        failedAt,
        true
    );
  }

  private void markRetryPendingThenAcknowledge(
      ConsumerRecord<String, String> record,
      ConsumerMessage message,
      Acknowledgment acknowledgment,
      RuntimeException handlerFailure,
      int attemptCount,
      Instant failedAt
  ) {
    try {
      Instant availableAt = inboxRetryPolicy.nextAttemptAt(
          attemptCount,
          failedAt
      );
      inboxService.markRetryPending(
          message.eventId(),
          processingOwner,
          attemptCount,
          failedAt,
          availableAt,
          conciseError(handlerFailure)
      );
      log.debug(
          "Inbox event scheduled for retry eventId={} eventType={} attemptCount={} availableAt={}",
          message.eventId().value(),
          message.eventType(),
          attemptCount,
          availableAt
      );
      log.warn(
          "Inbox event processing failed; retry scheduled eventId={} eventType={} attemptCount={} availableAt={}",
          message.eventId().value(),
          message.eventType(),
          attemptCount,
          availableAt
      );
    } catch (RuntimeException persistenceFailure) {
      observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
      log.error(
          "Event processing failed and Inbox RETRY_PENDING state could not be persisted eventId={} eventType={} topic={} partition={} offset={} owner={} handlerErrorType={} persistenceErrorType={}",
          message.eventId().value(),
          message.eventType(),
          record.topic(),
          record.partition(),
          record.offset(),
          processingOwner,
          handlerFailure.getClass().getSimpleName(),
          persistenceFailure.getClass().getSimpleName(),
          persistenceFailure
      );
      handlerFailure.addSuppressed(persistenceFailure);
      throw new KafkaConsumerProcessingException(
          record,
          message,
          "MARK_RETRY_PENDING",
          handlerFailure
      );
    }
    observe(metrics -> metrics.outcome(ConsumerOutcome.RETRY_PENDING));
    observe(ConsumerMetrics::retryScheduled);
    acknowledge(
        record,
        message,
        acknowledgment,
        InboxStatus.RETRY_PENDING
    );
  }

  private void markFailedThenAcknowledge(
      ConsumerRecord<String, String> record,
      ConsumerMessage message,
      Acknowledgment acknowledgment,
      RuntimeException handlerFailure,
      int attemptCount,
      Instant failedAt,
      boolean retriesExhausted
  ) {
    try {
      inboxService.markFailed(
          message.eventId(),
          processingOwner,
          attemptCount,
          failedAt,
          conciseError(handlerFailure)
      );
      if (retriesExhausted) {
        log.warn(
            "Inbox automatic retries exhausted eventId={} eventType={} attemptCount={}",
            message.eventId().value(),
            message.eventType(),
            attemptCount
        );
      }
    } catch (RuntimeException persistenceFailure) {
      observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
      log.error(
          "Inbox FAILED state could not be persisted eventId={} eventType={} topic={} partition={} offset={} owner={} handlerErrorType={} persistenceErrorType={}",
          message.eventId().value(),
          message.eventType(),
          record.topic(),
          record.partition(),
          record.offset(),
          processingOwner,
          handlerFailure.getClass().getSimpleName(),
          persistenceFailure.getClass().getSimpleName(),
          persistenceFailure
      );
      handlerFailure.addSuppressed(persistenceFailure);
      throw new KafkaConsumerProcessingException(
          record,
          message,
          "MARK_FAILED",
          handlerFailure
      );
    }
    observe(metrics -> metrics.outcome(ConsumerOutcome.FAILED));
    acknowledge(
        record,
        message,
        acknowledgment,
        InboxStatus.FAILED
    );
  }

  ConsumerMessage toConsumerMessage(ConsumerRecord<String, String> record) {
    if (record.value() == null) {
      throw new InvalidKafkaEventMessageException(
          record,
          "record value must not be null"
      );
    }
    try {
      return new ConsumerMessage(
          new EventId(
              requiredHeader(
                  record,
                  KafkaBrokerProducer.EVENT_ID_HEADER
              )
          ),
          requiredHeader(
              record,
              KafkaBrokerProducer.EVENT_TYPE_HEADER
          ),
          parseTimestamp(record),
          requiredHeader(
              record,
              KafkaBrokerProducer.EVENT_SOURCE_HEADER
          ),
          optionalHeader(
              record,
              KafkaBrokerProducer.CORRELATION_ID_HEADER
          ),
          new SerializedPayload(
              record.value(),
              requiredHeader(
                  record,
                  KafkaBrokerProducer.CONTENT_TYPE_HEADER
              )
          )
      );
    } catch (InvalidKafkaEventMessageException exception) {
      throw exception;
    } catch (IllegalArgumentException exception) {
      throw new InvalidKafkaEventMessageException(
          record,
          "invalid generic event metadata",
          exception
      );
    }
  }

  private void acknowledge(
      ConsumerRecord<String, String> record,
      ConsumerMessage message,
      Acknowledgment acknowledgment,
      InboxStatus inboxStatus
  ) {
    try {
      acknowledgment.acknowledge();
      log.debug(
          "Kafka event acknowledged eventId={} eventType={} inboxStatus={} topic={} partition={} offset={}",
          message.eventId().value(),
          message.eventType(),
          inboxStatus,
          record.topic(),
          record.partition(),
          record.offset()
      );
    } catch (RuntimeException exception) {
      log.warn(
          "Kafka acknowledgement failed after durable Inbox outcome eventId={} eventType={} inboxStatus={} topic={} partition={} offset={} consumer={}",
          message.eventId().value(),
          message.eventType(),
          inboxStatus,
          record.topic(),
          record.partition(),
          record.offset(),
          processingOwner,
          exception
      );
      throw new KafkaAcknowledgementException(
          record,
          message,
          exception
      );
    }
  }

  private static String conciseError(RuntimeException exception) {
    String message = exception.getMessage();
    return exception.getClass().getSimpleName()
        + (message == null || message.isBlank() ? "" : ": " + message);
  }

  private void observe(ConsumerMetricsAction action) {
    try {
      action.record(consumerMetrics);
    } catch (RuntimeException exception) {
      log.debug("Consumer metric recording failed", exception);
    }
  }

  @FunctionalInterface
  private interface ConsumerMetricsAction {
    void record(ConsumerMetrics metrics);
  }

  private static Instant parseTimestamp(ConsumerRecord<String, String> record) {
    try {
      return Instant.parse(
          requiredHeader(
              record,
              KafkaBrokerProducer.EVENT_TIMESTAMP_HEADER
          )
      );
    } catch (DateTimeParseException exception) {
      throw new InvalidKafkaEventMessageException(
          record,
          "invalid header '"
              + KafkaBrokerProducer.EVENT_TIMESTAMP_HEADER + "'",
          exception
      );
    }
  }

  private static String requiredHeader(
      ConsumerRecord<String, String> record,
      String name
  ) {
    String value = optionalHeader(
        record,
        name
    );
    if (value == null || value.isBlank()) {
      throw new InvalidKafkaEventMessageException(
          record,
          "missing required header '" + name + "'"
      );
    }
    return value;
  }

  private static String optionalHeader(
      ConsumerRecord<String, String> record,
      String name
  ) {
    Header header = record.headers().lastHeader(name);
    return header == null
        ? null
        : new String(
            header.value(),
            StandardCharsets.UTF_8
        );
  }
}
