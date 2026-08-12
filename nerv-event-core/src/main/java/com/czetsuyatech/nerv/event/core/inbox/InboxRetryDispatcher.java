package com.czetsuyatech.nerv.event.core.inbox;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import com.czetsuyatech.nerv.event.core.consumer.DefaultInboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.consumer.InboxFailureClassifier;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * Executes one synchronous, broker-neutral batch of due inbox retries.
 * </p>
 *
 * <p>
 * Persistence owns claiming, leases, and each state transition. This class deliberately does not hold a transaction
 * around the batch or the application handler invocation.
 * </p>
 */
@Slf4j
public final class InboxRetryDispatcher {

  @NonNull
  private final InboxService inboxService;

  @NonNull
  private final ConsumerDispatcher consumerDispatcher;

  @NonNull
  private final InboxRetryPolicy inboxRetryPolicy;

  @NonNull
  private final Clock clock;

  @Getter
  @NonNull
  private final String owner;

  @NonNull
  private final Duration leaseDuration;

  @NonNull
  private final InboxFailureClassifier failureClassifier;

  public InboxRetryDispatcher(
      InboxService inboxService,
      ConsumerDispatcher consumerDispatcher,
      InboxRetryPolicy inboxRetryPolicy,
      Clock clock,
      String owner,
      Duration leaseDuration
  )
  {
    this(
        inboxService,
        consumerDispatcher,
        inboxRetryPolicy,
        clock,
        owner,
        leaseDuration,
        new DefaultInboxFailureClassifier()
    );
  }

  public InboxRetryDispatcher(
      InboxService inboxService,
      ConsumerDispatcher consumerDispatcher,
      InboxRetryPolicy inboxRetryPolicy,
      Clock clock,
      String owner,
      Duration leaseDuration,
      InboxFailureClassifier failureClassifier
  )
  {
    this.inboxService = Objects.requireNonNull(
        inboxService,
        "inboxService must not be null"
    );
    this.consumerDispatcher = Objects.requireNonNull(
        consumerDispatcher,
        "consumerDispatcher must not be null"
    );
    this.inboxRetryPolicy = Objects.requireNonNull(
        inboxRetryPolicy,
        "inboxRetryPolicy must not be null"
    );
    this.clock = Objects.requireNonNull(
        clock,
        "clock must not be null"
    );
    this.owner = Objects.requireNonNull(
        owner,
        "owner must not be null"
    );
    this.leaseDuration = Objects.requireNonNull(
        leaseDuration,
        "leaseDuration must not be null"
    );
    this.failureClassifier = Objects.requireNonNull(
        failureClassifier,
        "failureClassifier must not be null"
    );
  }

  public InboxRetryResult dispatch(int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be greater than zero");
    }
    Instant claimedAt = clock.instant();
    List<InboxEvent> claimedEvents = inboxService.claimPendingRetries(
        batchSize,
        claimedAt,
        owner,
        leaseDuration
    );
    if (claimedEvents.isEmpty()) {
      return InboxRetryResult.empty();
    }

    InboxRetryResultAccumulator result = new InboxRetryResultAccumulator(claimedEvents.size());
    for (InboxEvent event : claimedEvents) {
      result.record(dispatch(event));
    }
    return result.toResult();
  }

  private InboxRetryOutcome dispatch(InboxEvent event) {
    log.debug(
        "Inbox retry event claimed eventId={} eventType={} owner={} attemptCount={}",
        event.eventId().value(),
        event.eventType(),
        owner,
        event.attemptCount()
    );
    try {
      consumerDispatcher.dispatch(toConsumerMessage(event));
    } catch (Exception exception) {
      return handleHandlerFailure(
          event,
          exception
      );
    }

    try {
      inboxService.markProcessed(
          event.eventId(),
          owner,
          clock.instant()
      );
      log.debug(
          "Inbox retry event processed eventId={} eventType={} attemptCount={}",
          event.eventId().value(),
          event.eventType(),
          event.attemptCount()
      );
      return InboxRetryOutcome.PROCESSED;
    } catch (Exception exception) {
      log.error(
          "Unable to mark inbox retry event processed; outcome is unresolved eventId={} eventType={} errorType={}",
          event.eventId().value(),
          event.eventType(),
          exception.getClass().getSimpleName(),
          exception
      );
      return InboxRetryOutcome.UNRESOLVED;
    }
  }

  private InboxRetryOutcome handleHandlerFailure(
      InboxEvent event,
      Exception exception
  ) {
    Instant failedAt = clock.instant();
    int attemptCount;
    try {
      attemptCount = Math.incrementExact(event.attemptCount());
    } catch (ArithmeticException overflowException) {
      return markFailed(
          event,
          Integer.MAX_VALUE,
          failedAt,
          overflowException,
          false
      );
    }

    if (!failureClassifier.isRetryable(exception)) {
      log.warn(
          "Event processing failed with non-retryable error eventId={} eventType={} attemptCount={} failureType={}",
          event.eventId().value(),
          event.eventType(),
          attemptCount,
          exception.getClass().getSimpleName(),
          exception
      );
      return markFailed(
          event,
          attemptCount,
          failedAt,
          exception,
          false
      );
    }
    log.warn(
        "Event processing failed with retryable error eventId={} eventType={} attemptCount={} retryable=true",
        event.eventId().value(),
        event.eventType(),
        attemptCount,
        exception
    );
    try {
      if (inboxRetryPolicy.canRetry(attemptCount)) {
        Instant availableAt = Objects.requireNonNull(
            inboxRetryPolicy.nextAttemptAt(
                attemptCount,
                failedAt
            ),
            "inboxRetryPolicy returned null next attempt time"
        );
        try {
          inboxService.markRetryPending(
              event.eventId(),
              owner,
              attemptCount,
              failedAt,
              availableAt,
              failureReason(exception)
          );
          log.debug(
              "Inbox retry scheduled eventId={} eventType={} attemptCount={} availableAt={}",
              event.eventId().value(),
              event.eventType(),
              attemptCount,
              availableAt
          );
          return InboxRetryOutcome.RETRY_PENDING;
        } catch (Exception transitionException) {
          return unresolved(
              event,
              "schedule",
              transitionException
          );
        }
      }
    } catch (Exception policyException) {
      return unresolved(
          event,
          "determine retry eligibility",
          policyException
      );
    }
    return markFailed(
        event,
        attemptCount,
        failedAt,
        exception,
        true
    );
  }

  private InboxRetryOutcome markFailed(
      InboxEvent event,
      int attemptCount,
      Instant failedAt,
      Exception exception,
      boolean retriesExhausted
  ) {
    try {
      inboxService.markFailed(
          event.eventId(),
          owner,
          attemptCount,
          failedAt,
          failureReason(exception)
      );
      if (retriesExhausted) {
        log.warn(
            "Inbox automatic retries exhausted eventId={} eventType={} attemptCount={}",
            event.eventId().value(),
            event.eventType(),
            attemptCount,
            exception
        );
      }
      return InboxRetryOutcome.FAILED;
    } catch (Exception transitionException) {
      return unresolved(
          event,
          "mark failed",
          transitionException
      );
    }
  }

  private InboxRetryOutcome unresolved(
      InboxEvent event,
      String transition,
      Exception exception
  ) {
    log.error(
        "Unable to {} inbox retry event; outcome is unresolved eventId={} eventType={} errorType={}",
        transition,
        event.eventId().value(),
        event.eventType(),
        exception.getClass().getSimpleName(),
        exception
    );
    return InboxRetryOutcome.UNRESOLVED;
  }

  private static ConsumerMessage toConsumerMessage(InboxEvent event) {
    return new ConsumerMessage(
        event.eventId(),
        event.eventType(),
        event.timestamp(),
        event.source(),
        event.correlationId(),
        event.payload()
    );
  }

  private static String failureReason(Exception exception) {
    String message = exception.getMessage();
    return message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
  }

  private enum InboxRetryOutcome {
    PROCESSED, RETRY_PENDING, FAILED, UNRESOLVED
  }

  private static final class InboxRetryResultAccumulator {
    private final int claimed;
    private int processed;
    private int retryPending;
    private int failed;
    private int unresolved;

    private InboxRetryResultAccumulator(int claimed) {
      this.claimed = claimed;
    }

    private void record(InboxRetryOutcome outcome) {
      switch (outcome) {
        case PROCESSED -> processed++;
        case RETRY_PENDING -> retryPending++;
        case FAILED -> failed++;
        case UNRESOLVED -> unresolved++;
      }
    }

    private InboxRetryResult toResult() {
      return new InboxRetryResult(
          claimed,
          processed,
          retryPending,
          failed,
          unresolved
      );
    }
  }
}
