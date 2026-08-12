package com.czetsuyatech.nerv.event.sqs.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientId;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistration;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistry;
import com.czetsuyatech.nerv.event.sqs.routing.SqsDestination;
import com.czetsuyatech.nerv.event.sqs.routing.SqsDestinationResolver;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

class SqsBrokerProducerTest {
  @Test
  void publishesBodyAndGenericAttributesAndReturnsMessageId() {
    SqsAsyncClient client = successfulClient("sqs-message-1");
    SqsBrokerProducer producer = producer(
        Map.of(
            "orders",
            destination(
                "account-a",
                "https://sqs.test/orders"
            )
        ),
        List.of(
            registration(
                "account-a",
                client
            )
        ),
        Duration.ofSeconds(1)
    );

    assertThat(producer.brokerId()).isEqualTo(SqsBrokerProducer.BROKER_ID);
    assertThat(
        producer.publish(
            message(
                "orders",
                "corr-1"
            )
        ).brokerReference()
    ).isEqualTo("sqs-message-1");
    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(client).sendMessage(captor.capture());
    SendMessageRequest request = captor.getValue();
    assertThat(request.messageBody()).isEqualTo("{\"order\":42}");
    assertThat(request.queueUrl()).isEqualTo("https://sqs.test/orders");
    assertThat(request.messageAttributes()).containsOnlyKeys(
        "nerv-event-id",
        "nerv-event-type",
        "nerv-event-source",
        "nerv-event-timestamp",
        "nerv-event-content-type",
        "nerv-event-correlation-id"
    );
    assertThat(request.messageAttributes()).allSatisfy(
        (
            name,
            value) -> assertThat(value.dataType()).isEqualTo("String")
    );
    assertThat(request.messageAttributes().get("nerv-event-correlation-id").stringValue()).isEqualTo("corr-1");
  }

  @Test
  void omitsMissingCorrelationId() {
    SqsAsyncClient client = successfulClient("id");
    producer(
        Map.of(
            "orders",
            destination(
                "a",
                "https://sqs.test/orders"
            )
        ),
        List.of(
            registration(
                "a",
                client
            )
        ),
        Duration.ofSeconds(1)
    ).publish(
        message(
            "orders",
            null
        )
    );
    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(client).sendMessage(captor.capture());
    assertThat(captor.getValue().messageAttributes()).doesNotContainKey("nerv-event-correlation-id");
  }

  @Test
  void resolvesAConfiguredQueueNameBeforeSending() {
    SqsAsyncClient client = successfulClient("id");
    when(client.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(
        CompletableFuture.completedFuture(
            GetQueueUrlResponse.builder().queueUrl("https://sqs.test/orders-prod").build()
        )
    );
    producer(
        Map.of(
            "orders",
            destination(
                "a",
                "orders-prod"
            )
        ),
        List.of(
            registration(
                "a",
                client
            )
        ),
        Duration.ofSeconds(1)
    ).publish(
        message(
            "orders",
            null
        )
    );
    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(client).sendMessage(captor.capture());
    assertThat(captor.getValue().queueUrl()).isEqualTo("https://sqs.test/orders-prod");
  }

  @Test
  void routesDifferentTargetsToDifferentAccountClients() {
    SqsAsyncClient accountA = successfulClient("orders-id");
    SqsAsyncClient accountB = successfulClient("notifications-id");
    SqsBrokerProducer producer = producer(
        Map.of(
            "orders",
            destination(
                "account-a",
                "https://sqs.test/orders"
            ),
            "notifications",
            destination(
                "account-b",
                "https://sqs.test/notifications"
            )
        ),
        List.of(
            registration(
                "account-a",
                accountA
            ),
            registration(
                "account-b",
                accountB
            )
        ),
        Duration.ofSeconds(1)
    );

    producer.publish(
        message(
            "orders",
            null
        )
    );
    verify(accountA).sendMessage(any(SendMessageRequest.class));
    verify(
        accountB,
        never()
    ).sendMessage(any(SendMessageRequest.class));
    producer.publish(
        message(
            "notifications",
            null
        )
    );
    verify(accountB).sendMessage(any(SendMessageRequest.class));
  }

  @Test
  void propagatesClientFailureWithRoutingContextAndOriginalCause() {
    RuntimeException cause = new RuntimeException("credentials unavailable");
    SqsAsyncClient client = mock(SqsAsyncClient.class);
    when(client.sendMessage(any(SendMessageRequest.class)))
        .thenReturn(CompletableFuture.failedFuture(cause));
    SqsBrokerProducer producer = producer(
        Map.of(
            "orders",
            destination(
                "account-a",
                "https://sqs.test/orders"
            )
        ),
        List.of(
            registration(
                "account-a",
                client
            )
        ),
        Duration.ofSeconds(1)
    );
    assertThatThrownBy(
        () -> producer.publish(
            message(
                "orders",
                null
            )
        )
    )
        .isInstanceOf(SqsPublishException.class)
        .hasMessageContaining("eventId=event-1")
        .hasMessageContaining("queue=https://sqs.test/orders")
        .hasMessageContaining("clientId=account-a")
        .hasCause(cause);
  }

  @Test
  void timeoutReportsAmbiguousOutcomeAndRoutingContext() {
    SqsAsyncClient client = mock(SqsAsyncClient.class);
    when(client.sendMessage(any(SendMessageRequest.class))).thenReturn(new CompletableFuture<>());
    SqsBrokerProducer producer = producer(
        Map.of(
            "orders",
            destination(
                "account-a",
                "https://sqs.test/orders"
            )
        ),
        List.of(
            registration(
                "account-a",
                client
            )
        ),
        Duration.ofMillis(5)
    );
    assertThatThrownBy(
        () -> producer.publish(
            message(
                "orders",
                null
            )
        )
    )
        .isInstanceOf(SqsPublishException.class)
        .hasMessageContaining("timed out after PT0.005S")
        .hasMessageContaining("delivery may have succeeded and is ambiguous")
        .hasMessageContaining("eventId=event-1")
        .hasMessageContaining("clientId=account-a");
  }

  private static SqsAsyncClient successfulClient(String messageId) {
    SqsAsyncClient client = mock(SqsAsyncClient.class);
    when(client.sendMessage(any(SendMessageRequest.class))).thenReturn(
        CompletableFuture.completedFuture(
            SendMessageResponse.builder().messageId(messageId).build()
        )
    );
    return client;
  }

  private static BrokerMessage message(
      String target,
      String correlationId
  ) {
    return new BrokerMessage(
        new EventId("event-1"),
        "OrderCreated",
        Instant.parse("2026-08-18T00:00:00Z"),
        "orders-service",
        correlationId,
        target,
        new SerializedPayload(
            "{\"order\":42}",
            "application/json"
        )
    );
  }

  private static SqsDestination destination(
      String client,
      String queue
  ) {
    return new SqsDestination(
        new SqsClientId(client),
        queue
    );
  }

  private static SqsClientRegistration registration(
      String id,
      SqsAsyncClient client
  ) {
    return SqsClientRegistration.applicationProvided(
        new SqsClientId(id),
        client
    );
  }

  private static SqsBrokerProducer producer(
      Map<String, SqsDestination> destinations,
      List<SqsClientRegistration> clients,
      Duration timeout
  ) {
    return new SqsBrokerProducer(
        new SqsDestinationResolver(destinations),
        new SqsClientRegistry(clients),
        timeout
    );
  }
}
