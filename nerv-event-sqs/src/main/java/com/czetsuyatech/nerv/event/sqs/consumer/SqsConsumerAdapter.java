package com.czetsuyatech.nerv.event.sqs.consumer;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import com.czetsuyatech.nerv.event.core.consumer.DefaultInboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.consumer.InboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.inbox.InboxCompletionException;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics.ConsumerHandlerResult;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics.ConsumerOutcome;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientId;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;

/**
 * Connects one SQS delivery to a durable Inbox outcome before deletion.
 */
@Slf4j
public final class SqsConsumerAdapter {
  private final ConsumerDispatcher dispatcher;
  private final InboxService repository;
  private final Clock clock;
  private final String processingOwner;
  private final Duration leaseDuration;
  private final InboxRetryPolicy retryPolicy;
  private final InboxFailureClassifier failureClassifier;
  private final SqsMessageMapper mapper;
  private final String consumerName;
  private final String queue;
  private final SqsClientId clientId;
  private final ConsumerMetrics consumerMetrics;

  public SqsConsumerAdapter(
      ConsumerDispatcher dispatcher,
      InboxService repository,
      Clock clock,
      String processingOwner,
      Duration leaseDuration,
      InboxRetryPolicy retryPolicy,
      SqsMessageMapper mapper
  )
  {
    this(
        dispatcher,
        repository,
        clock,
        processingOwner,
        leaseDuration,
        retryPolicy,
        mapper,
        new DefaultInboxFailureClassifier(),
        "unbound",
        "unbound",
        new SqsClientId("unbound"),
        ConsumerMetrics.noop()
    );
  }

  public SqsConsumerAdapter(
      ConsumerDispatcher dispatcher,
      InboxService repository,
      Clock clock,
      String processingOwner,
      Duration leaseDuration,
      InboxRetryPolicy retryPolicy,
      SqsMessageMapper mapper,
      InboxFailureClassifier failureClassifier
  )
  {
    this(
        dispatcher,
        repository,
        clock,
        processingOwner,
        leaseDuration,
        retryPolicy,
        mapper,
        failureClassifier,
        "unbound",
        "unbound",
        new SqsClientId("unbound"),
        ConsumerMetrics.noop()
    );
  }

  public SqsConsumerAdapter(
      ConsumerDispatcher dispatcher,
      InboxService repository,
      Clock clock,
      String processingOwner,
      Duration leaseDuration,
      InboxRetryPolicy retryPolicy,
      SqsMessageMapper mapper,
      InboxFailureClassifier failureClassifier,
      ConsumerMetrics consumerMetrics
  )
  {
    this(
        dispatcher,
        repository,
        clock,
        processingOwner,
        leaseDuration,
        retryPolicy,
        mapper,
        failureClassifier,
        "unbound",
        "unbound",
        new SqsClientId("unbound"),
        consumerMetrics
    );
  }

  private SqsConsumerAdapter(
      ConsumerDispatcher dispatcher,
      InboxService repository,
      Clock clock,
      String processingOwner,
      Duration leaseDuration,
      InboxRetryPolicy retryPolicy,
      SqsMessageMapper mapper,
      InboxFailureClassifier failureClassifier,
      String consumerName,
      String queue,
      SqsClientId clientId,
      ConsumerMetrics consumerMetrics
  )
  {
    this.dispatcher = Objects.requireNonNull(dispatcher);
    this.repository = Objects.requireNonNull(repository);
    this.clock = Objects.requireNonNull(clock);
    this.processingOwner = requireText(
        processingOwner,
        "processingOwner"
    );
    this.leaseDuration = Objects.requireNonNull(leaseDuration);
    this.retryPolicy = Objects.requireNonNull(retryPolicy);
    this.mapper = Objects.requireNonNull(mapper);
    this.failureClassifier = Objects.requireNonNull(failureClassifier);
    this.consumerName = requireText(
        consumerName,
        "consumerName"
    );
    this.queue = requireText(
        queue,
        "queue"
    );
    this.clientId = Objects.requireNonNull(clientId);
    this.consumerMetrics = Objects.requireNonNull(consumerMetrics);
  }

  public SqsConsumerAdapter forConsumer(
      String name,
      String queue,
      SqsClientId clientId
  ) {
    return new SqsConsumerAdapter(
        dispatcher,
        repository,
        clock,
        processingOwner + ":sqs-" + requireText(
            name,
            "name"
        ),
        leaseDuration,
        retryPolicy,
        mapper,
        failureClassifier,
        name,
        queue,
        clientId,
        consumerMetrics
    );
  }

  public void onMessage(Message<String> sqsMessage) {
    process(
        sqsMessage,
        () -> Acknowledgement.acknowledgeAsync(sqsMessage).join()
    );
  }

  void process(
      Message<String> sqsMessage,
      Runnable acknowledgement
  ) {
    String sqsMessageId = messageId(sqsMessage);
    ConsumerMessage message;
    try {
      message = mapper.toConsumerMessage(sqsMessage);
    } catch (InvalidSqsEventMessageException exception) {
      log.warn(
          "SQS event message rejected consumer={} queue={} messageId={} reason={}",
          consumerName,
          queue,
          sqsMessageId,
          exception.getMessage()
      );
      throw exception;
    }
    log.debug(
        "SQS event received eventId={} eventType={} queue={} messageId={} consumer={} clientId={}",
        message.eventId().value(),
        message.eventType(),
        queue,
        sqsMessageId,
        consumerName,
        clientId.value()
    );
    observe(ConsumerMetrics::received);
    InboxRegistration registration = register(
        message,
        sqsMessageId
    );
    if (!registration.created() && acknowledgeDurableDuplicate(
        message,
        registration.event(),
        acknowledgement,
        sqsMessageId
    )) {
      return;
    }
    InboxEvent claimed = claim(
        message,
        sqsMessageId
    );
    log.debug(
        "SQS Inbox event claimed eventId={} eventType={} owner={} attemptCount={}",
        message.eventId().value(),
        message.eventType(),
        processingOwner,
        claimed.attemptCount()
    );
    long handlerStartedAt = System.nanoTime();
    observe(ConsumerMetrics::handlerExecutionStarted);
    try {
      repository.process(
          message.eventId(),
          processingOwner,
          clock.instant(),
          () -> dispatcher.dispatch(message)
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
      throw new SqsConsumerProcessingException("MARK_PROCESSED", exception);
    } catch (RuntimeException handlerFailure) {
      observe(
          metrics -> metrics.handlerExecutionCompleted(
              ConsumerHandlerResult.FAILURE,
              Duration.ofNanos(System.nanoTime() - handlerStartedAt)
          )
      );
      persistFailureThenAcknowledge(
          message,
          claimed,
          acknowledgement,
          sqsMessageId,
          handlerFailure
      );
      return;
    }
    acknowledgeProcessed(
        message,
        claimed,
        acknowledgement,
        sqsMessageId
    );
  }

  private InboxRegistration register(
      ConsumerMessage message,
      String sqsMessageId
  ) {
    InboxEvent event = new InboxEvent(
        message.eventId(),
        message.eventType(),
        message.timestamp(),
        message.source(),
        message.correlationId(),
        message.payload(),
        InboxStatus.RECEIVED,
        0,
        clock.instant(),
        null,
        null,
        null,
        null,
        null,
        null
    );
    try {
      InboxRegistration registration = repository.register(event);
      log.debug(
          "SQS Inbox event registered eventId={} eventType={} status={} consumer={}",
          message.eventId().value(),
          message.eventType(),
          registration.event().status(),
          consumerName
      );
      return registration;
    } catch (RuntimeException exception) {
      observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
      log.error(
          "SQS inbox registration failed stage=REGISTER eventId={} eventType={} queue={} "
              + "messageId={} consumer={} clientId={}",
          message.eventId().value(),
          message.eventType(),
          queue,
          sqsMessageId,
          consumerName,
          clientId.value(),
          exception
      );
      throw new SqsConsumerProcessingException(
          "REGISTER",
          exception
      );
    }
  }

  private boolean acknowledgeDurableDuplicate(
      ConsumerMessage message,
      InboxEvent existing,
      Runnable acknowledgement,
      String sqsMessageId
  ) {
    if (existing.status() != InboxStatus.PROCESSED
        && existing.status() != InboxStatus.RETRY_PENDING
        && existing.status() != InboxStatus.FAILED) {
      return false;
    }
    log.debug(
        "SQS duplicate resolved from durable Inbox eventId={} eventType={} status={} consumer={}",
        message.eventId().value(),
        message.eventType(),
        existing.status(),
        consumerName
    );
    observe(metrics -> metrics.outcome(ConsumerOutcome.DUPLICATE));
    acknowledge(
        message,
        acknowledgement,
        existing.status(),
        sqsMessageId
    );
    return true;
  }

  private InboxEvent claim(
      ConsumerMessage message,
      String sqsMessageId
  ) {
    try {
      return repository.claim(
          message.eventId(),
          clock.instant(),
          processingOwner,
          leaseDuration
      )
          .orElseThrow(() -> new IllegalStateException("Inbox event has an active processing lease"));
    } catch (RuntimeException exception) {
      observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
      log.warn(
          "SQS inbox claim unresolved stage=CLAIM eventId={} eventType={} queue={} messageId={} "
              + "consumer={} clientId={} owner={}",
          message.eventId().value(),
          message.eventType(),
          queue,
          sqsMessageId,
          consumerName,
          clientId.value(),
          processingOwner,
          exception
      );
      throw new SqsConsumerProcessingException(
          "CLAIM",
          exception
      );
    }
  }

  private void acknowledgeProcessed(
      ConsumerMessage message,
      InboxEvent claimed,
      Runnable acknowledgement,
      String sqsMessageId
  ) {
    log.debug(
        "SQS Inbox event processed eventId={} eventType={} attemptCount={}",
        message.eventId().value(),
        message.eventType(),
        claimed.attemptCount()
    );
    observe(metrics -> metrics.outcome(ConsumerOutcome.PROCESSED));
    acknowledge(
        message,
        acknowledgement,
        InboxStatus.PROCESSED,
        sqsMessageId
    );
  }

  private void persistFailureThenAcknowledge(
      ConsumerMessage message,
      InboxEvent claimed,
      Runnable acknowledgement,
      String sqsMessageId,
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
          "Event processing failed with non-retryable error eventId={} eventType={} attemptCount={} "
              + "failureType={}",
          message.eventId().value(),
          message.eventType(),
          attemptCount,
          handlerFailure.getClass().getSimpleName(),
          handlerFailure
      );
      markFailedThenAcknowledge(
          message,
          acknowledgement,
          sqsMessageId,
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
      retryAllowed = retryPolicy.canRetry(attemptCount);
    } catch (RuntimeException exception) {
      observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
      throw exception;
    }
    if (retryAllowed) {
      try {
        Instant availableAt = retryPolicy.nextAttemptAt(
            attemptCount,
            failedAt
        );
        repository.markRetryPending(
            message.eventId(),
            processingOwner,
            attemptCount,
            failedAt,
            availableAt,
            conciseError(handlerFailure)
        );
        log.warn(
            "SQS Inbox processing failed; retry scheduled eventId={} eventType={} attemptCount={} "
                + "availableAt={} consumer={}",
            message.eventId().value(),
            message.eventType(),
            attemptCount,
            availableAt,
            consumerName
        );
      } catch (RuntimeException persistenceFailure) {
        persistenceFailure(
            handlerFailure,
            persistenceFailure,
            message,
            sqsMessageId,
            "MARK_RETRY_PENDING"
        );
      }
      observe(metrics -> metrics.outcome(ConsumerOutcome.RETRY_PENDING));
      observe(ConsumerMetrics::retryScheduled);
      acknowledge(
          message,
          acknowledgement,
          InboxStatus.RETRY_PENDING,
          sqsMessageId
      );
      return;
    }
    markFailedThenAcknowledge(
        message,
        acknowledgement,
        sqsMessageId,
        handlerFailure,
        attemptCount,
        failedAt,
        true
    );
  }

  private void markFailedThenAcknowledge(
      ConsumerMessage message,
      Runnable acknowledgement,
      String sqsMessageId,
      RuntimeException handlerFailure,
      int attemptCount,
      Instant failedAt,
      boolean retriesExhausted
  ) {
    try {
      repository.markFailed(
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
      persistenceFailure(
          handlerFailure,
          persistenceFailure,
          message,
          sqsMessageId,
          "MARK_FAILED"
      );
    }
    observe(metrics -> metrics.outcome(ConsumerOutcome.FAILED));
    acknowledge(
        message,
        acknowledgement,
        InboxStatus.FAILED,
        sqsMessageId
    );
  }

  private void persistenceFailure(
      RuntimeException handlerFailure,
      RuntimeException persistenceFailure,
      ConsumerMessage message,
      String sqsMessageId,
      String stage
  ) {
    observe(metrics -> metrics.outcome(ConsumerOutcome.UNRESOLVED));
    log.error(
        "SQS handler failed and durable Inbox transition failed stage={} eventId={} eventType={} "
            + "queue={} messageId={} consumer={} clientId={} owner={} handlerErrorType={} persistenceErrorType={}",
        stage,
        message.eventId().value(),
        message.eventType(),
        queue,
        sqsMessageId,
        consumerName,
        clientId.value(),
        processingOwner,
        handlerFailure.getClass().getSimpleName(),
        persistenceFailure.getClass().getSimpleName(),
        persistenceFailure
    );
    handlerFailure.addSuppressed(persistenceFailure);
    throw new SqsConsumerProcessingException(
        stage,
        handlerFailure
    );
  }

  private void acknowledge(
      ConsumerMessage message,
      Runnable acknowledgement,
      InboxStatus status,
      String sqsMessageId
  ) {
    try {
      acknowledgement.run();
      log.debug(
          "SQS event acknowledged eventId={} eventType={} inboxStatus={} queue={} messageId={} "
              + "consumer={} clientId={}",
          message.eventId().value(),
          message.eventType(),
          status,
          queue,
          sqsMessageId,
          consumerName,
          clientId.value()
      );
    } catch (RuntimeException exception) {
      log.warn(
          "SQS acknowledgement failed after durable Inbox outcome eventId={} eventType={} "
              + "inboxStatus={} queue={} messageId={} consumer={} clientId={}",
          message.eventId().value(),
          message.eventType(),
          status,
          queue,
          sqsMessageId,
          consumerName,
          clientId.value(),
          exception
      );
      throw new SqsAcknowledgementException(exception);
    }
  }

  private static String messageId(Message<?> message) {
    Object id = message == null ? null : message.getHeaders().get(SqsHeaders.SQS_RAW_MESSAGE_ID_HEADER);
    return id == null ? "unknown" : id.toString();
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

  private static String requireText(
      String value,
      String name
  ) {
    Objects.requireNonNull(
        value,
        name + " must not be null"
    );
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
