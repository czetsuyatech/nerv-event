package com.czetsuyatech.nerv.event.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.czetsuyatech.nerv.event.consumer.EventHandler;
import com.czetsuyatech.nerv.event.consumer.EventHandlerInterceptor;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import com.czetsuyatech.nerv.event.core.consumer.DefaultInboxFailureClassifier;
import com.czetsuyatech.nerv.event.core.consumer.EventHandlerRegistry;
import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.exception.EventRetryableException;
import com.czetsuyatech.nerv.event.kafka.producer.KafkaBrokerProducer;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics.ConsumerHandlerResult;
import com.czetsuyatech.nerv.event.observability.ConsumerMetrics.ConsumerOutcome;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;

class KafkaConsumerAdapterTest {

  private static final Instant NOW = Instant.parse("2026-08-17T00:00:00Z");
  private static final Clock CLOCK = Clock.fixed(
      NOW,
      ZoneOffset.UTC
  );

  private static final Duration LEASE = Duration.ofSeconds(30);

  @Test
  void mapsKafkaValueAndGenericHeadersToConsumerMessage() {
    ConsumerMessage message = adapter(
        mock(ConsumerDispatcher.class),
        mock(InboxService.class)
    )
        .toConsumerMessage(record("correlation-1"));

    assertThat(message.eventId().value()).isEqualTo("event-1");
    assertThat(message.eventType()).isEqualTo("order.created");
    assertThat(message.source()).isEqualTo("orders");
    assertThat(message.timestamp()).isEqualTo(Instant.parse("2026-08-15T00:00:00Z"));
    assertThat(message.correlationId()).isEqualTo("correlation-1");
    assertThat(message.payload().value()).isEqualTo("{\"orderId\":42}");
    assertThat(message.payload().contentType()).isEqualTo("application/json");
  }

  @Test
  void newEventIsRegisteredClaimedProcessedThenAcknowledged() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    InboxService inbox = readyInbox();
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    adapter(
        dispatcher,
        inbox
    ).onMessage(
        record("correlation-1"),
        acknowledgment
    );

    InOrder order = inOrder(
        inbox,
        dispatcher,
        acknowledgment
    );
    order.verify(inbox).register(any(InboxEvent.class));
    order.verify(inbox)
        .claim(
            eq(new EventId("event-1")),
            eq(NOW),
            eq("orders-api:order-events"),
            eq(LEASE)
        );
    order.verify(dispatcher).dispatch(any(ConsumerMessage.class));
    order.verify(inbox)
        .markProcessed(
            new EventId("event-1"),
            "orders-api:order-events",
            NOW
        );
    order.verify(acknowledgment).acknowledge();
    verify(
        inbox,
        never()
    ).markRetryPending(
        any(),
        any(),
        org.mockito.ArgumentMatchers.anyInt(),
        any(),
        any(),
        any()
    );
    verify(
        inbox,
        never()
    ).markFailed(
        any(),
        any(),
        org.mockito.ArgumentMatchers.anyInt(),
        any(),
        any()
    );
  }

  @Test
  void runsTheInterceptorAroundTheHandlerAfterInboxClaimAndBeforeProcessedOutcome() {
    List<String> calls = new ArrayList<>();
    EventHandler<String> handler = handler(calls);
    EventHandlerInterceptor interceptor = (event, chain) -> {
      calls.add("before");
      try {
        chain.proceed();
      } finally {
        calls.add("after");
      }
    };
    InboxService inbox = readyInbox();

    adapter(
        dispatcher(
            handler,
            List.of(interceptor)
        ),
        inbox
    ).onMessage(
        record(null),
        mock(Acknowledgment.class)
    );

    assertThat(calls).containsExactly(
        "before",
        "handler",
        "after"
    );
    verify(inbox).markProcessed(
        new EventId("event-1"),
        "orders-api:order-events",
        NOW
    );
  }

  @Test
  void routesRetryableInterceptorFailuresThroughTheExistingInboxRetryPath() {
    AtomicInteger handlerInvocations = new AtomicInteger();
    EventHandlerInterceptor interceptor = (event, chain) -> {
      throw new EventRetryableException("context unavailable");
    };
    InboxService inbox = readyInbox();

    adapter(
        dispatcher(
            handler(handlerInvocations),
            List.of(interceptor)
        ),
        inbox,
        retryPolicy(true)
    ).onMessage(
        record(null),
        mock(Acknowledgment.class)
    );

    verify(inbox).markRetryPending(
        new EventId("event-1"),
        "orders-api:order-events",
        1,
        NOW,
        NOW.plusSeconds(1),
        "EventRetryableException: context unavailable"
    );
    assertThat(handlerInvocations).hasValue(0);
  }

  @Test
  void recordsOneProcessedOutcomeAfterTheInboxTransitionIsDurable() {
    RecordingConsumerMetrics metrics = new RecordingConsumerMetrics();

    adapter(
        mock(ConsumerDispatcher.class),
        readyInbox(),
        retryPolicy(true),
        metrics
    ).onMessage(
        record(null),
        mock(Acknowledgment.class)
    );

    assertThat(metrics.received).isEqualTo(1);
    assertThat(metrics.outcomes).containsExactly(ConsumerOutcome.PROCESSED);
    assertThat(metrics.handlerResults).containsExactly(ConsumerHandlerResult.SUCCESS);
  }

  @Test
  void recordsRetryPendingOnlyAfterItIsDurablyScheduled() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    doThrow(new EventRetryableException("temporary")).when(dispatcher).dispatch(any());
    RecordingConsumerMetrics metrics = new RecordingConsumerMetrics();

    adapter(
        dispatcher,
        readyInbox(),
        retryPolicy(true),
        metrics
    ).onMessage(
        record(null),
        mock(Acknowledgment.class)
    );

    assertThat(metrics.outcomes).containsExactly(ConsumerOutcome.RETRY_PENDING);
    assertThat(metrics.handlerResults).containsExactly(ConsumerHandlerResult.FAILURE);
    assertThat(metrics.retries).isEqualTo(1);
  }

  @Test
  void recordsAConfirmedTerminalFailure() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    doThrow(new IllegalArgumentException("invalid")).when(dispatcher).dispatch(any());
    RecordingConsumerMetrics metrics = new RecordingConsumerMetrics();

    adapter(
        dispatcher,
        readyInbox(),
        retryPolicy(true),
        metrics
    ).onMessage(
        record(null),
        mock(Acknowledgment.class)
    );

    assertThat(metrics.outcomes).containsExactly(ConsumerOutcome.FAILED);
    assertThat(metrics.handlerResults).containsExactly(ConsumerHandlerResult.FAILURE);
  }

  @Test
  void recordsDurableDuplicateWithoutExecutingTheHandler() {
    InboxService inbox = mock(InboxService.class);
    when(inbox.register(any())).thenReturn(
        new InboxRegistration(
            false,
            inboxEvent(InboxStatus.PROCESSED)
        )
    );
    RecordingConsumerMetrics metrics = new RecordingConsumerMetrics();
    List<String> calls = new ArrayList<>();
    ConsumerDispatcher dispatcher = dispatcher(
        handler(calls),
        List.of((event, chain) -> {
          calls.add("interceptor");
          chain.proceed();
        })
    );

    adapter(
        dispatcher,
        inbox,
        retryPolicy(true),
        metrics
    ).onMessage(
        record(null),
        mock(Acknowledgment.class)
    );

    assertThat(metrics.received).isEqualTo(1);
    assertThat(metrics.outcomes).containsExactly(ConsumerOutcome.DUPLICATE);
    assertThat(metrics.handlerResults).isEmpty();
    assertThat(calls).isEmpty();
  }

  @Test
  void recordsUnresolvedWhenAHandlerSuccessCannotBePersisted() {
    InboxService inbox = readyInbox();
    doThrow(new IllegalStateException("database unavailable")).when(inbox)
        .markProcessed(any(), any(), any());
    RecordingConsumerMetrics metrics = new RecordingConsumerMetrics();

    assertThatThrownBy(
        () -> adapter(
            mock(ConsumerDispatcher.class),
            inbox,
            retryPolicy(true),
            metrics
        ).onMessage(
            record(null),
            mock(Acknowledgment.class)
        )
    ).isInstanceOf(KafkaConsumerProcessingException.class);

    assertThat(metrics.outcomes).containsExactly(ConsumerOutcome.UNRESOLVED);
    assertThat(metrics.handlerResults).containsExactly(ConsumerHandlerResult.SUCCESS);
  }

  @Test
  void ignoresMetricRecordingFailures() {
    ConsumerMetrics failingMetrics = new ConsumerMetrics() {
      public void received() {
        throw new IllegalStateException("metrics unavailable");
      }

      public void outcome(ConsumerOutcome outcome) {
      }

      public void handlerExecutionStarted() {
      }

      public void handlerExecutionCompleted(
          ConsumerHandlerResult result,
          Duration duration
      ) {
      }

      public void retryScheduled() {
      }
    };
    InboxService inbox = readyInbox();

    adapter(
        mock(ConsumerDispatcher.class),
        inbox,
        retryPolicy(true),
        failingMetrics
    ).onMessage(
        record(null),
        mock(Acknowledgment.class)
    );

    verify(inbox).markProcessed(any(), any(), any());
  }

  @Test
  void handlerFailureBelowTheLimitIsDurablyMarkedRetryPendingBeforeAcknowledgement() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    EventRetryableException handlerFailure = new EventRetryableException("business failure");
    doThrow(handlerFailure).when(dispatcher).dispatch(any());
    InboxService inbox = readyInbox();
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    InboxRetryPolicy retryPolicy = retryPolicy(true);
    adapter(
        dispatcher,
        inbox,
        retryPolicy
    ).onMessage(
        record(null),
        acknowledgment
    );

    InOrder order = inOrder(
        inbox,
        dispatcher,
        acknowledgment
    );
    order.verify(dispatcher).dispatch(any(ConsumerMessage.class));
    order.verify(inbox)
        .markRetryPending(
            new EventId("event-1"),
            "orders-api:order-events",
            1,
            NOW,
            NOW.plusSeconds(1),
            "EventRetryableException: business failure"
        );
    order.verify(acknowledgment).acknowledge();
    verify(retryPolicy).canRetry(1);
    verify(retryPolicy).nextAttemptAt(
        1,
        NOW
    );
  }

  @Test
  void handlerFailureAtTheLimitIsDurablyMarkedFailedBeforeAcknowledgement() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    doThrow(new EventRetryableException("business failure")).when(dispatcher).dispatch(any());
    InboxService inbox = readyInbox();
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    adapter(
        dispatcher,
        inbox,
        retryPolicy(false)
    ).onMessage(
        record(null),
        acknowledgment
    );

    InOrder order = inOrder(
        inbox,
        dispatcher,
        acknowledgment
    );
    order.verify(dispatcher).dispatch(any(ConsumerMessage.class));
    order.verify(inbox)
        .markFailed(
            new EventId("event-1"),
            "orders-api:order-events",
            1,
            NOW,
            "EventRetryableException: business failure"
        );
    order.verify(acknowledgment).acknowledge();
  }

  @Test
  void nonRetryableHandlerFailureIsFailedWithoutConsultingTheRetryPolicy() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    doThrow(new IllegalArgumentException("invalid order")).when(dispatcher).dispatch(any());
    InboxService inbox = readyInbox();
    InboxRetryPolicy retryPolicy = retryPolicy(true);

    adapter(
        dispatcher,
        inbox,
        retryPolicy
    ).onMessage(
        record(null),
        mock(Acknowledgment.class)
    );

    verify(inbox).markFailed(
        new EventId("event-1"),
        "orders-api:order-events",
        1,
        NOW,
        "IllegalArgumentException: invalid order"
    );
    verify(
        retryPolicy,
        never()
    ).canRetry(anyInt());
    verify(
        retryPolicy,
        never()
    ).nextAttemptAt(
        anyInt(),
        any()
    );
  }

  @Test
  void reclaimedProcessingAttemptUsesTheNextActualHandlerAttemptNumber() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    doThrow(new EventRetryableException("business failure")).when(dispatcher).dispatch(any());
    InboxService inbox = mock(InboxService.class);
    InboxEvent reclaimed = inboxEvent(
        InboxStatus.PROCESSING,
        1
    );
    when(inbox.register(any())).thenReturn(
        new InboxRegistration(
            false,
            reclaimed
        )
    );
    when(
        inbox.claim(
            any(),
            any(),
            any(),
            any()
        )
    ).thenReturn(Optional.of(reclaimed));
    InboxRetryPolicy retryPolicy = retryPolicy(true);

    adapter(
        dispatcher,
        inbox,
        retryPolicy
    ).onMessage(
        record(null),
        mock(Acknowledgment.class)
    );

    verify(retryPolicy).canRetry(2);
    verify(retryPolicy).nextAttemptAt(
        2,
        NOW
    );
    verify(inbox).markRetryPending(
        new EventId("event-1"),
        "orders-api:order-events",
        2,
        NOW,
        NOW.plusSeconds(1),
        "EventRetryableException: business failure"
    );
  }

  @Test
  void processedTransitionFailureDoesNotAcknowledge() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    InboxService inbox = readyInbox();
    doThrow(new IllegalStateException("database unavailable")).when(inbox)
        .markProcessed(
            any(),
            any(),
            any()
        );
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    assertThatThrownBy(
        () -> adapter(
            dispatcher,
            inbox
        ).onMessage(
            record(null),
            acknowledgment
        )
    )
        .isInstanceOf(KafkaConsumerProcessingException.class)
        .hasMessageContaining("stage=MARK_PROCESSED");
    verify(
        acknowledgment,
        never()
    ).acknowledge();
  }

  @Test
  void registrationFailureDoesNotAcknowledgeOrDispatch() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    InboxService inbox = mock(InboxService.class);
    doThrow(new IllegalStateException("database unavailable")).when(inbox).register(any());
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    assertThatThrownBy(
        () -> adapter(
            dispatcher,
            inbox
        ).onMessage(
            record(null),
            acknowledgment
        )
    )
        .isInstanceOf(KafkaConsumerProcessingException.class)
        .hasMessageContaining("stage=REGISTER");
    verify(
        dispatcher,
        never()
    ).dispatch(any());
    verify(
        acknowledgment,
        never()
    ).acknowledge();
  }

  @Test
  void claimFailureDoesNotAcknowledgeOrDispatch() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    InboxService inbox = mock(InboxService.class);
    when(inbox.register(any())).thenAnswer(
        invocation -> new InboxRegistration(
            true,
            invocation.getArgument(0)
        )
    );
    doThrow(new IllegalStateException("database unavailable")).when(inbox)
        .claim(
            any(),
            any(),
            any(),
            any()
        );
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    assertThatThrownBy(
        () -> adapter(
            dispatcher,
            inbox
        ).onMessage(
            record(null),
            acknowledgment
        )
    )
        .isInstanceOf(KafkaConsumerProcessingException.class)
        .hasMessageContaining("stage=CLAIM");
    verify(
        dispatcher,
        never()
    ).dispatch(any());
    verify(
        acknowledgment,
        never()
    ).acknowledge();
  }

  @Test
  void retryPendingTransitionFailureDoesNotAcknowledge() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    doThrow(new EventRetryableException("business failure")).when(dispatcher).dispatch(any());
    InboxService inbox = readyInbox();
    doThrow(new IllegalStateException("database unavailable")).when(inbox)
        .markRetryPending(
            any(),
            any(),
            org.mockito.ArgumentMatchers.anyInt(),
            any(),
            any(),
            any()
        );
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    assertThatThrownBy(
        () -> adapter(
            dispatcher,
            inbox,
            retryPolicy(true)
        ).onMessage(
            record(null),
            acknowledgment
        )
    )
        .isInstanceOf(KafkaConsumerProcessingException.class)
        .hasMessageContaining("stage=MARK_RETRY_PENDING");
    verify(
        acknowledgment,
        never()
    ).acknowledge();
  }

  @Test
  void failedTransitionFailureDoesNotAcknowledge() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    doThrow(new IllegalStateException("business failure")).when(dispatcher).dispatch(any());
    InboxService inbox = readyInbox();
    doThrow(new IllegalStateException("database unavailable")).when(inbox)
        .markFailed(
            any(),
            any(),
            org.mockito.ArgumentMatchers.anyInt(),
            any(),
            any()
        );
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    assertThatThrownBy(
        () -> adapter(
            dispatcher,
            inbox,
            retryPolicy(false)
        ).onMessage(
            record(null),
            acknowledgment
        )
    )
        .isInstanceOf(KafkaConsumerProcessingException.class)
        .hasMessageContaining("stage=MARK_FAILED");
    verify(
        acknowledgment,
        never()
    ).acknowledge();
  }

  @Test
  void processedRetryPendingAndFailedDuplicatesAreAcknowledgedWithoutHandling() {
    assertCompletedDuplicateIsAcknowledged(InboxStatus.PROCESSED);
    assertCompletedDuplicateIsAcknowledged(InboxStatus.RETRY_PENDING);
    assertCompletedDuplicateIsAcknowledged(InboxStatus.FAILED);
  }

  @Test
  void activeProcessingDuplicateIsNotAcknowledged() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    InboxService inbox = mock(InboxService.class);
    InboxEvent processing = inboxEvent(InboxStatus.PROCESSING);
    when(inbox.register(any())).thenReturn(
        new InboxRegistration(
            false,
            processing
        )
    );
    when(
        inbox.claim(
            any(),
            any(),
            any(),
            any()
        )
    ).thenReturn(Optional.empty());
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    assertThatThrownBy(
        () -> adapter(
            dispatcher,
            inbox
        ).onMessage(
            record(null),
            acknowledgment
        )
    )
        .isInstanceOf(KafkaConsumerProcessingException.class)
        .hasMessageContaining("stage=CLAIM");
    verify(
        dispatcher,
        never()
    ).dispatch(any());
    verify(
        acknowledgment,
        never()
    ).acknowledge();
  }

  @Test
  void malformedRecordIsNotAcknowledgedOrRegistered() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    InboxService inbox = mock(InboxService.class);
    ConsumerRecord<String, String> record = record(null);
    record.headers().remove(KafkaBrokerProducer.EVENT_TYPE_HEADER);
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    assertThatThrownBy(
        () -> adapter(
            dispatcher,
            inbox
        ).onMessage(
            record,
            acknowledgment
        )
    )
        .isInstanceOf(InvalidKafkaEventMessageException.class);
    verify(
        inbox,
        never()
    ).register(any());
    verify(
        dispatcher,
        never()
    ).dispatch(any());
    verify(
        acknowledgment,
        never()
    ).acknowledge();
  }

  @Test
  void acknowledgementFailureDoesNotRevertDurableProcessedState() {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    InboxService inbox = readyInbox();
    Acknowledgment acknowledgment = mock(Acknowledgment.class);
    doThrow(new IllegalStateException("commit failed")).when(acknowledgment).acknowledge();

    assertThatThrownBy(
        () -> adapter(
            dispatcher,
            inbox
        ).onMessage(
            record(null),
            acknowledgment
        )
    )
        .isInstanceOf(KafkaAcknowledgementException.class);
    verify(inbox).markProcessed(
        new EventId("event-1"),
        "orders-api:order-events",
        NOW
    );
  }

  @Test
  void acknowledgementFailureDoesNotChangeDurableDuplicateState() {
    assertDuplicateAcknowledgementFailure(InboxStatus.RETRY_PENDING);
    assertDuplicateAcknowledgementFailure(InboxStatus.FAILED);
  }

  private static void assertCompletedDuplicateIsAcknowledged(InboxStatus status) {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    InboxService inbox = mock(InboxService.class);
    when(inbox.register(any())).thenReturn(
        new InboxRegistration(
            false,
            inboxEvent(status)
        )
    );
    Acknowledgment acknowledgment = mock(Acknowledgment.class);

    adapter(
        dispatcher,
        inbox
    ).onMessage(
        record(null),
        acknowledgment
    );

    verify(
        dispatcher,
        never()
    ).dispatch(any());
    verify(
        inbox,
        never()
    ).claim(
        any(),
        any(),
        any(),
        any()
    );
    verify(acknowledgment).acknowledge();
  }

  private static void assertDuplicateAcknowledgementFailure(InboxStatus status) {
    ConsumerDispatcher dispatcher = mock(ConsumerDispatcher.class);
    InboxService inbox = mock(InboxService.class);
    when(inbox.register(any())).thenReturn(
        new InboxRegistration(
            false,
            inboxEvent(status)
        )
    );
    Acknowledgment acknowledgment = mock(Acknowledgment.class);
    doThrow(new IllegalStateException("commit failed")).when(acknowledgment).acknowledge();

    assertThatThrownBy(
        () -> adapter(
            dispatcher,
            inbox
        ).onMessage(
            record(null),
            acknowledgment
        )
    )
        .isInstanceOf(KafkaAcknowledgementException.class);
    verify(
        dispatcher,
        never()
    ).dispatch(any());
    verify(
        inbox,
        never()
    ).claim(
        any(),
        any(),
        any(),
        any()
    );
  }

  private static InboxService readyInbox() {
    InboxService inbox = mock(InboxService.class);
    InboxEvent processing = inboxEvent(InboxStatus.PROCESSING);
    when(inbox.register(any())).thenAnswer(
        invocation -> new InboxRegistration(
            true,
            invocation.getArgument(0)
        )
    );
    when(
        inbox.claim(
            any(),
            any(),
            any(),
            any()
        )
    ).thenReturn(Optional.of(processing));
    return inbox;
  }

  private static ConsumerDispatcher dispatcher(
      EventHandler<String> handler,
      List<EventHandlerInterceptor> interceptors
  ) {
    return new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler)),
        new EventDeserializer() {
          @Override
          public <T> T deserialize(
              SerializedPayload payload,
              Class<T> payloadType
          ) {
            return payloadType.cast("payload");
          }
        },
        interceptors
    );
  }

  private static EventHandler<String> handler(List<String> calls) {
    return new EventHandler<>() {
      @Override
      public String eventType() {
        return "order.created";
      }

      @Override
      public Class<String> payloadType() {
        return String.class;
      }

      @Override
      public void handle(EventMessage<String> event) {
        calls.add("handler");
      }
    };
  }

  private static EventHandler<String> handler(AtomicInteger invocations) {
    return new EventHandler<>() {
      @Override
      public String eventType() {
        return "order.created";
      }

      @Override
      public Class<String> payloadType() {
        return String.class;
      }

      @Override
      public void handle(EventMessage<String> event) {
        invocations.incrementAndGet();
      }
    };
  }

  private static KafkaConsumerAdapter adapter(
      ConsumerDispatcher dispatcher,
      InboxService inbox
  ) {
    return adapter(
        dispatcher,
        inbox,
        retryPolicy(true)
    );
  }

  private static KafkaConsumerAdapter adapter(
      ConsumerDispatcher dispatcher,
      InboxService inbox,
      InboxRetryPolicy retryPolicy
  ) {
    return adapter(
        dispatcher,
        inbox,
        retryPolicy,
        ConsumerMetrics.noop()
    );
  }

  private static KafkaConsumerAdapter adapter(
      ConsumerDispatcher dispatcher,
      InboxService inbox,
      InboxRetryPolicy retryPolicy,
      ConsumerMetrics metrics
  ) {
    return new KafkaConsumerAdapter(
        dispatcher,
        inbox,
        CLOCK,
        "orders-api",
        LEASE,
        retryPolicy,
        new DefaultInboxFailureClassifier(),
        metrics
    )
        .forConsumer(
            "order-events",
            LEASE
        );
  }

  private static InboxRetryPolicy retryPolicy(boolean retryable) {
    InboxRetryPolicy retryPolicy = mock(InboxRetryPolicy.class);
    when(retryPolicy.canRetry(org.mockito.ArgumentMatchers.anyInt())).thenReturn(retryable);
    when(
        retryPolicy.nextAttemptAt(
            org.mockito.ArgumentMatchers.anyInt(),
            any()
        )
    )
        .thenAnswer(invocation -> ((Instant) invocation.getArgument(1)).plusSeconds(1));
    return retryPolicy;
  }

  private static InboxEvent inboxEvent(InboxStatus status) {
    return inboxEvent(
        status,
        0
    );
  }

  private static InboxEvent inboxEvent(
      InboxStatus status,
      int attemptCount
  ) {
    return new InboxEvent(
        new EventId("event-1"),
        "order.created",
        Instant.parse("2026-08-15T00:00:00Z"),
        "orders",
        null,
        new SerializedPayload(
            "{\"orderId\":42}",
            "application/json"
        ),
        status,
        attemptCount,
        NOW,
        status == InboxStatus.RETRY_PENDING ? NOW : null,
        status == InboxStatus.PROCESSING ? NOW : null,
        status == InboxStatus.PROCESSING ? "other-owner" : null,
        status == InboxStatus.PROCESSED ? NOW : null,
        status == InboxStatus.RETRY_PENDING || status == InboxStatus.FAILED ? NOW : null,
        status == InboxStatus.RETRY_PENDING || status == InboxStatus.FAILED ? "failed" : null
    );
  }

  private static final class RecordingConsumerMetrics implements ConsumerMetrics {
    private int received;
    private int retries;
    private final List<ConsumerOutcome> outcomes = new ArrayList<>();
    private final List<ConsumerHandlerResult> handlerResults = new ArrayList<>();

    public void received() {
      received++;
    }

    public void outcome(ConsumerOutcome outcome) {
      outcomes.add(outcome);
    }

    public void handlerExecutionStarted() {
    }

    public void handlerExecutionCompleted(
        ConsumerHandlerResult result,
        Duration duration
    ) {
      handlerResults.add(result);
    }

    public void retryScheduled() {
      retries++;
    }
  }

  private static ConsumerRecord<String, String> record(String correlationId) {
    ConsumerRecord<String, String> record = new ConsumerRecord<>(
        "orders-topic",
        2,
        42L,
        null,
        "{\"orderId\":42}"
    );
    addHeader(
        record,
        KafkaBrokerProducer.EVENT_ID_HEADER,
        "event-1"
    );
    addHeader(
        record,
        KafkaBrokerProducer.EVENT_TYPE_HEADER,
        "order.created"
    );
    addHeader(
        record,
        KafkaBrokerProducer.EVENT_SOURCE_HEADER,
        "orders"
    );
    addHeader(
        record,
        KafkaBrokerProducer.EVENT_TIMESTAMP_HEADER,
        "2026-08-15T00:00:00Z"
    );
    addHeader(
        record,
        KafkaBrokerProducer.CONTENT_TYPE_HEADER,
        "application/json"
    );
    if (correlationId != null) {
      addHeader(
          record,
          KafkaBrokerProducer.CORRELATION_ID_HEADER,
          correlationId
      );
    }
    return record;
  }

  private static void addHeader(
      ConsumerRecord<String, String> record,
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
