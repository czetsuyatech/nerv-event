package com.czetsuyatech.nerv.event.sqs.consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.DefaultInboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.exception.EventRetryableException;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics.ConsumerHandlerResult;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics.ConsumerOutcome;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientId;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class SqsConsumerAdapterTest {
  private final ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
  private final InboxService repository = mock(InboxService.class);
  private final InboxRetryPolicy retryPolicy = mock(InboxRetryPolicy.class);
  private final Runnable acknowledgement = mock(Runnable.class);
  private final Instant now = Instant.parse("2026-08-18T01:00:00Z");
  private final SqsConsumerAdapter adapter = new SqsConsumerAdapter(
      dispatcher,
      repository,
      Clock.fixed(
          now,
          ZoneOffset.UTC
      ),
      "orders-api",
      Duration.ofSeconds(30),
      retryPolicy,
      new SqsMessageMapper()
  ).forConsumer(
      "order-events",
      "orders",
      new SqsClientId("account-a")
  );

  @BeforeEach
  void newMessage() {
    InboxEvent received = event(
        InboxStatus.RECEIVED,
        0
    );
    InboxEvent processing = event(
        InboxStatus.PROCESSING,
        0
    );
    when(repository.register(any())).thenReturn(
        new InboxRegistration(
            true,
            received
        )
    );
    when(
        repository.claim(
            any(),
            any(),
            anyString(),
            any()
        )
    ).thenReturn(Optional.of(processing));
    doCallRealMethod().when(repository).process(any(), anyString(), any(), any());
  }

  @Test
  void successRegistersClaimsDispatchesPersistsThenAcknowledgesInOrder() {
    adapter.process(
        SqsMessageMapperTest.validMessage(true),
        acknowledgement
    );
    InOrder order = inOrder(
        repository,
        dispatcher,
        acknowledgement
    );
    order.verify(repository).register(any());
    order.verify(repository)
        .claim(
            any(),
            any(),
            anyString(),
            any()
        );
    order.verify(dispatcher).dispatch(any());
    order.verify(repository)
        .markProcessed(
            new EventId("event-1"),
            "orders-api:sqs-order-events",
            now
        );
    order.verify(acknowledgement).run();
  }

  @Test
  void recordsTheConfirmedConsumerOutcome() {
    ConsumerMetrics metrics = mock(ConsumerMetrics.class);
    SqsConsumerAdapter instrumentedAdapter = new SqsConsumerAdapter(
        dispatcher,
        repository,
        Clock.fixed(
            now,
            ZoneOffset.UTC
        ),
        "orders-api",
        Duration.ofSeconds(30),
        retryPolicy,
        new SqsMessageMapper(),
        new DefaultInboxFailureClassifier(),
        metrics
    ).forConsumer(
        "order-events",
        "orders",
        new SqsClientId("account-a")
    );

    instrumentedAdapter.process(
        SqsMessageMapperTest.validMessage(true),
        acknowledgement
    );

    verify(metrics).received();
    verify(metrics).handlerExecutionStarted();
    verify(metrics).handlerExecutionCompleted(
        eq(ConsumerHandlerResult.SUCCESS),
        any()
    );
    verify(metrics).outcome(ConsumerOutcome.PROCESSED);
  }

  @Test
  void retryPolicySchedulesDurableRetryBeforeAcknowledgement() {
    doThrow(new EventRetryableException("temporary")).when(dispatcher).dispatch(any());
    when(retryPolicy.canRetry(1)).thenReturn(true);
    when(
        retryPolicy.nextAttemptAt(
            1,
            now
        )
    ).thenReturn(now.plusSeconds(5));
    adapter.process(
        SqsMessageMapperTest.validMessage(false),
        acknowledgement
    );
    InOrder order = inOrder(
        dispatcher,
        retryPolicy,
        repository,
        acknowledgement
    );
    order.verify(dispatcher).dispatch(any());
    order.verify(retryPolicy).canRetry(1);
    order.verify(retryPolicy)
        .nextAttemptAt(
            1,
            now
        );
    order.verify(repository)
        .markRetryPending(
            new EventId("event-1"),
            "orders-api:sqs-order-events",
            1,
            now,
            now.plusSeconds(5),
            "EventRetryableException: temporary"
        );
    order.verify(acknowledgement).run();
  }

  @Test
  void exhaustedFailureIsPersistedBeforeAcknowledgement() {
    doThrow(new IllegalStateException("permanent")).when(dispatcher).dispatch(any());
    when(retryPolicy.canRetry(1)).thenReturn(false);
    adapter.process(
        SqsMessageMapperTest.validMessage(false),
        acknowledgement
    );
    InOrder order = inOrder(
        dispatcher,
        repository,
        acknowledgement
    );
    order.verify(dispatcher).dispatch(any());
    order.verify(repository)
        .markFailed(
            new EventId("event-1"),
            "orders-api:sqs-order-events",
            1,
            now,
            "IllegalStateException: permanent"
        );
    order.verify(acknowledgement).run();
    verify(
        retryPolicy,
        never()
    ).nextAttemptAt(
        anyInt(),
        any()
    );
  }

  @Test
  void durableDuplicatesAreAcknowledgedWithoutHandlerOrClaim() {
    for (InboxStatus status : new InboxStatus[]{InboxStatus.PROCESSED,
        InboxStatus.RETRY_PENDING, InboxStatus.FAILED}) {
      InboxEvent existing = event(
          status,
          1
      );
      when(repository.register(any())).thenReturn(
          new InboxRegistration(
              false,
              existing
          )
      );
      adapter.process(
          SqsMessageMapperTest.validMessage(false),
          acknowledgement
      );
    }
    verify(
        dispatcher,
        never()
    ).dispatch(any());
    verify(
        repository,
        never()
    ).claim(
        any(),
        any(),
        anyString(),
        any()
    );
    verify(
        acknowledgement,
        org.mockito.Mockito.times(3)
    ).run();
  }

  @Test
  void activeProcessingIsNotAcknowledged() {
    when(repository.register(any())).thenReturn(
        new InboxRegistration(
            false,
            event(
                InboxStatus.PROCESSING,
                0
            )
        )
    );
    when(
        repository.claim(
            any(),
            any(),
            anyString(),
            any()
        )
    ).thenReturn(Optional.empty());
    assertThatThrownBy(
        () -> adapter.process(
            SqsMessageMapperTest.validMessage(false),
            acknowledgement
        )
    )
        .isInstanceOf(SqsConsumerProcessingException.class)
        .extracting(exception -> ((SqsConsumerProcessingException) exception).stage())
        .isEqualTo("CLAIM");
    verify(
        dispatcher,
        never()
    ).dispatch(any());
    verify(
        acknowledgement,
        never()
    ).run();
  }

  @Test
  void expiredProcessingCanBeReclaimedAndProcessed() {
    when(repository.register(any())).thenReturn(
        new InboxRegistration(
            false,
            event(
                InboxStatus.PROCESSING,
                0
            )
        )
    );
    adapter.process(
        SqsMessageMapperTest.validMessage(false),
        acknowledgement
    );
    verify(dispatcher).dispatch(any());
    verify(repository).markProcessed(
        any(),
        anyString(),
        any()
    );
    verify(acknowledgement).run();
  }

  @Test
  void durabilityFailuresNeverAcknowledge() {
    doThrow(new IllegalStateException("database")).when(repository)
        .markProcessed(
            any(),
            anyString(),
            any()
        );
    assertThatThrownBy(
        () -> adapter.process(
            SqsMessageMapperTest.validMessage(false),
            acknowledgement
        )
    )
        .isInstanceOf(SqsConsumerProcessingException.class);
    verify(
        acknowledgement,
        never()
    ).run();
  }

  @Test
  void registrationAndClaimFailuresNeverAcknowledge() {
    doThrow(new IllegalStateException("database")).when(repository).register(any());
    assertThatThrownBy(
        () -> adapter.process(
            SqsMessageMapperTest.validMessage(false),
            acknowledgement
        )
    )
        .isInstanceOf(SqsConsumerProcessingException.class);
    verify(
        acknowledgement,
        never()
    ).run();
  }

  @Test
  void claimFailureNeverAcknowledges() {
    when(
        repository.claim(
            any(),
            any(),
            anyString(),
            any()
        )
    )
        .thenThrow(new IllegalStateException("database"));
    assertThatThrownBy(
        () -> adapter.process(
            SqsMessageMapperTest.validMessage(false),
            acknowledgement
        )
    )
        .isInstanceOf(SqsConsumerProcessingException.class);
    verify(
        dispatcher,
        never()
    ).dispatch(any());
    verify(
        acknowledgement,
        never()
    ).run();
  }

  @Test
  void retryAndFailedPersistenceFailuresNeverAcknowledge() {
    doThrow(new EventRetryableException("handler")).when(dispatcher).dispatch(any());
    when(retryPolicy.canRetry(1)).thenReturn(true);
    when(
        retryPolicy.nextAttemptAt(
            1,
            now
        )
    ).thenReturn(now.plusSeconds(1));
    doThrow(new IllegalStateException("database")).when(repository)
        .markRetryPending(
            any(),
            anyString(),
            anyInt(),
            any(),
            any(),
            anyString()
        );
    assertThatThrownBy(
        () -> adapter.process(
            SqsMessageMapperTest.validMessage(false),
            acknowledgement
        )
    )
        .isInstanceOf(SqsConsumerProcessingException.class);
    verify(
        acknowledgement,
        never()
    ).run();
  }

  @Test
  void terminalFailedPersistenceFailureNeverAcknowledges() {
    doThrow(new IllegalStateException("handler")).when(dispatcher).dispatch(any());
    when(retryPolicy.canRetry(1)).thenReturn(false);
    doThrow(new IllegalStateException("database")).when(repository)
        .markFailed(
            any(),
            anyString(),
            anyInt(),
            any(),
            anyString()
        );
    assertThatThrownBy(
        () -> adapter.process(
            SqsMessageMapperTest.validMessage(false),
            acknowledgement
        )
    )
        .isInstanceOf(SqsConsumerProcessingException.class);
    verify(
        acknowledgement,
        never()
    ).run();
  }

  @Test
  void acknowledgementFailureDoesNotRevertDurableOutcome() {
    doThrow(new IllegalStateException("delete failed")).when(acknowledgement).run();
    assertThatThrownBy(
        () -> adapter.process(
            SqsMessageMapperTest.validMessage(false),
            acknowledgement
        )
    )
        .isInstanceOf(SqsAcknowledgementException.class);
    verify(repository).markProcessed(
        any(),
        anyString(),
        any()
    );
    verify(
        repository,
        never()
    ).markFailed(
        any(),
        anyString(),
        anyInt(),
        any(),
        anyString()
    );
  }

  @Test
  void invalidMetadataDoesNotRegisterDispatchOrAcknowledge() {
    assertThatThrownBy(
        () -> adapter.process(
            org.springframework.messaging.support.MessageBuilder
                .withPayload("secret-body")
                .build(),
            acknowledgement
        )
    )
        .isInstanceOf(InvalidSqsEventMessageException.class);
    verify(
        repository,
        never()
    ).register(any());
    verify(
        dispatcher,
        never()
    ).dispatch(any());
    verify(
        acknowledgement,
        never()
    ).run();
  }

  private InboxEvent event(
      InboxStatus status,
      int attemptCount
  ) {
    Instant availableAt = status == InboxStatus.RETRY_PENDING ? now.plusSeconds(5) : null;
    return new InboxEvent(
        new EventId("event-1"),
        "OrderCreated",
        Instant.parse("2026-08-18T00:00:00Z"),
        "orders-service",
        null,
        new SerializedPayload(
            "{\"order\":42}",
            "application/json"
        ),
        status,
        attemptCount,
        now.minusSeconds(1),
        availableAt,
        status == InboxStatus.PROCESSING ? now : null,
        status == InboxStatus.PROCESSING ? "other" : null,
        status == InboxStatus.PROCESSED ? now : null,
        status == InboxStatus.FAILED ? now : null,
        null
    );
  }
}
