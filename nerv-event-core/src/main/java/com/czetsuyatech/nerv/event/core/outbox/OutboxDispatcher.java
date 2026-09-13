package com.czetsuyatech.nerv.event.core.outbox;

import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducerRegistry;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.retry.RetryPolicy;
import com.czetsuyatech.nerv.event.core.routing.DestinationResolver;
import com.czetsuyatech.nerv.event.core.routing.DestinationRoute;
import com.czetsuyatech.nerv.event.core.serialization.EventSerializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Executes one synchronous outbox delivery batch. Scheduling belongs outside core.
 */
@Slf4j
@RequiredArgsConstructor
public final class OutboxDispatcher {

  @NonNull
  private final OutboxService outboxService;

  @NonNull
  private final DestinationResolver destinationResolver;

  @NonNull
  private final EventSerializer eventSerializer;

  @NonNull
  private final BrokerProducerRegistry brokerProducerRegistry;

  @NonNull
  private final RetryPolicy retryPolicy;

  @NonNull
  private final Clock clock;

  public DispatchResult dispatch(int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be greater than zero");
    }
    Instant dispatchTime = clock.instant();
    log.debug(
        "Outbox dispatch batch claiming events batchSize={} eligibleAt={}",
        batchSize,
        dispatchTime
    );
    List<OutboxEvent> claimedEvents = outboxService.claimPending(
        dispatchTime,
        batchSize
    );
    log.debug(
        "Outbox dispatch batch claimed events claimed={}",
        claimedEvents.size()
    );
    if (claimedEvents.isEmpty()) {
      return DispatchResult.empty();
    }
    DispatchResultAccumulator result = new DispatchResultAccumulator(claimedEvents.size());
    for (OutboxEvent event : claimedEvents) {
      result.record(dispatch(event));
    }
    return result.toResult();
  }

  private DispatchOutcome dispatch(OutboxEvent outboxEvent) {
    try {
      log.debug(
          "Dispatching outbox event outboxId={} eventId={} eventType={} destination={} correlationId={} attemptCount={}",
          outboxEvent.id().value(),
          outboxEvent.event().id().value(),
          outboxEvent.event().type(),
          outboxEvent.destination().name(),
          outboxEvent.event().correlationId(),
          outboxEvent.attemptCount()
      );
      DestinationRoute route = destinationResolver.resolve(outboxEvent.destination());
      log.debug(
          "Resolved destination route destination={} broker={} target={} eventId={}",
          outboxEvent.destination().name(),
          route.brokerId().value(),
          route.physicalTarget(),
          outboxEvent.event().id().value()
      );
      SerializedPayload serializedEvent = eventSerializer.serialize(outboxEvent.event());
      BrokerProducer producer = brokerProducerRegistry.producerFor(route.brokerId());
      log.debug(
          "Selected broker producer broker={} eventId={}",
          route.brokerId().value(),
          outboxEvent.event().id().value()
      );
      log.debug(
          "Publishing outbox event outboxId={} eventId={} broker={} target={}",
          outboxEvent.id().value(),
          outboxEvent.event().id().value(),
          route.brokerId().value(),
          route.physicalTarget()
      );
      BrokerPublishResult result = producer.publish(
          new BrokerMessage(
              outboxEvent.event().id(),
              outboxEvent.event().type(),
              outboxEvent.event().timestamp(),
              outboxEvent.event().source(),
              outboxEvent.event().correlationId(),
              outboxEvent.orderingKey(),
              route.physicalTarget(),
              serializedEvent
          )
      );
      try {
        boolean transitioned = outboxService.markPublished(
            outboxEvent.id(),
            outboxEvent.claimVersion(),
            result
        );
        if (!transitioned) {
          logFenced(outboxEvent, "PUBLISHED");
          return DispatchOutcome.UNRESOLVED;
        }
      } catch (Exception exception) {
        log.error(
            "Unable to confirm published outbox event outboxId={} eventId={} errorType={}",
            outboxEvent.id().value(),
            outboxEvent.event().id().value(),
            exception.getClass().getSimpleName(),
            exception
        );
        return DispatchOutcome.UNRESOLVED;
      }
      log.debug(
          "Outbox event published outboxId={} eventId={} eventType={} destination={} correlationId={} broker={} target={}",
          outboxEvent.id().value(),
          outboxEvent.event().id().value(),
          outboxEvent.event().type(),
          outboxEvent.destination().name(),
          outboxEvent.event().correlationId(),
          route.brokerId().value(),
          route.physicalTarget()
      );
      return DispatchOutcome.PUBLISHED;

    } catch (Exception exception) {
      return handleFailure(
          outboxEvent,
          clock.instant(),
          exception
      );
    }
  }

  private DispatchOutcome handleFailure(
      OutboxEvent event,
      Instant failedAt,
      Exception exception
  ) {
    String failureReason = failureReason(exception);
    int failedAttemptCount;
    try {
      failedAttemptCount = Math.incrementExact(event.attemptCount());
    } catch (ArithmeticException arithmeticException) {
      try {
        boolean transitioned = outboxService.markFailed(
            event.id(),
            event.claimVersion(),
            Integer.MAX_VALUE,
            failureReason
        );
        if (!transitioned) {
          logFenced(event, "FAILED");
          return DispatchOutcome.UNRESOLVED;
        }
      } catch (Exception transitionException) {
        log.error(
            "Unable to mark outbox event failed after attempt count overflow outboxId={} eventId={} errorType={}",
            event.id().value(),
            event.event().id().value(),
            transitionException.getClass().getSimpleName(),
            transitionException
        );
        return DispatchOutcome.UNRESOLVED;
      }
      log.error(
          "Outbox event permanently failed because the attempt count cannot be incremented outboxId={} eventId={} eventType={} destination={} correlationId={} attempts={}",
          event.id().value(),
          event.event().id().value(),
          event.event().type(),
          event.destination().name(),
          event.event().correlationId(),
          event.attemptCount(),
          arithmeticException
      );
      return DispatchOutcome.FAILED;
    }
    final boolean retryAllowed;
    try {
      retryAllowed = retryPolicy.allowsRetry(failedAttemptCount);
    } catch (Exception policyException) {
      log.error(
          "Unable to determine retry eligibility for outbox event outboxId={} eventId={} errorType={}",
          event.id().value(),
          event.event().id().value(),
          policyException.getClass().getSimpleName(),
          policyException
      );
      return DispatchOutcome.UNRESOLVED;
    }
    if (retryAllowed) {
      try {
        Instant nextAttemptAt = Objects.requireNonNull(
            retryPolicy.nextEligibleAt(
                failedAttemptCount,
                failedAt
            ),
            "retryPolicy returned null next eligible time"
        );
        boolean transitioned = outboxService.reschedule(
            event.id(),
            event.claimVersion(),
            failedAttemptCount,
            nextAttemptAt,
            failureReason
        );
        if (!transitioned) {
          logFenced(event, "PENDING");
          return DispatchOutcome.UNRESOLVED;
        }
        log.warn(
            "Outbox event delivery failed; retry scheduled outboxId={} eventId={} eventType={} destination={} correlationId={} attemptCount={} nextAttemptAt={} errorType={}",
            event.id().value(),
            event.event().id().value(),
            event.event().type(),
            event.destination().name(),
            event.event().correlationId(),
            failedAttemptCount,
            nextAttemptAt,
            exception.getClass().getSimpleName()
        );
        return DispatchOutcome.RETRIED;
      } catch (Exception transitionException) {
        log.error(
            "Unable to reschedule outbox event outboxId={} eventId={} errorType={}",
            event.id().value(),
            event.event().id().value(),
            transitionException.getClass().getSimpleName(),
            transitionException
        );
        return DispatchOutcome.UNRESOLVED;
      }
    } else {
      try {
        boolean transitioned = outboxService.markFailed(
            event.id(),
            event.claimVersion(),
            failedAttemptCount,
            failureReason
        );
        if (!transitioned) {
          logFenced(event, "FAILED");
          return DispatchOutcome.UNRESOLVED;
        }
      } catch (Exception transitionException) {
        log.error(
            "Unable to mark outbox event failed outboxId={} eventId={} errorType={}",
            event.id().value(),
            event.event().id().value(),
            transitionException.getClass().getSimpleName(),
            transitionException
        );
        return DispatchOutcome.UNRESOLVED;
      }
      log.error(
          "Outbox event permanently failed outboxId={} eventId={} eventType={} destination={} correlationId={} attempts={}",
          event.id().value(),
          event.event().id().value(),
          event.event().type(),
          event.destination().name(),
          event.event().correlationId(),
          failedAttemptCount,
          exception
      );
      return DispatchOutcome.FAILED;
    }
  }

  private static String failureReason(Exception exception) {
    String message = exception.getMessage();
    return message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
  }

  private static void logFenced(OutboxEvent event, String transition) {
    log.warn(
        "Outbox transition fenced outboxId={} eventId={} owner={} claimVersion={} transition={}",
        event.id().value(),
        event.event().id().value(),
        event.lockedBy(),
        event.claimVersion(),
        transition
    );
  }

  private enum DispatchOutcome {
    PUBLISHED, RETRIED, FAILED, UNRESOLVED
  }

  private static final class DispatchResultAccumulator {

    private final int claimed;
    private int published;
    private int retried;
    private int failed;
    private int unresolved;

    private DispatchResultAccumulator(int claimed) {
      this.claimed = claimed;
    }

    private void record(DispatchOutcome outcome) {
      switch (outcome) {
        case PUBLISHED -> published++;
        case RETRIED -> retried++;
        case FAILED -> failed++;
        case UNRESOLVED -> unresolved++;
      }
    }

    private DispatchResult toResult() {
      return new DispatchResult(
          claimed,
          published,
          retried,
          failed,
          unresolved
      );
    }
  }
}
