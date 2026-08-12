package com.czetsuyatech.nerv.event.sqs;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.consumer.EventHandler;
import com.czetsuyatech.nerv.event.exception.EventRetryableException;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.EventHandlerRegistry;
import com.czetsuyatech.nerv.event.core.inbox.ExponentialInboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import com.czetsuyatech.nerv.event.sqs.autoconfigure.NervEventSqsProperties;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientId;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistration;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistry;
import com.czetsuyatech.nerv.event.sqs.consumer.SpringSqsConsumerContainerFactory;
import com.czetsuyatech.nerv.event.sqs.consumer.SqsConsumerAdapter;
import com.czetsuyatech.nerv.event.sqs.consumer.SqsMessageMapper;
import com.czetsuyatech.nerv.event.sqs.producer.SqsBrokerProducer;
import com.czetsuyatech.nerv.event.sqs.routing.SqsDestination;
import com.czetsuyatech.nerv.event.sqs.routing.SqsDestinationResolver;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

@Testcontainers(disabledWithoutDocker = true)
class SqsLocalStackIT {
  @Container
  static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
      DockerImageName.parse("localstack/localstack:4.7.0")
  ).withServices("sqs");

  private MessageListenerContainer<String> listenerContainer;
  private SqsAsyncClient client;

  @AfterEach
  void stopResources() {
    if (listenerContainer != null) {
      listenerContainer.stop();
    }
    if (client != null) {
      client.close();
    }
  }

  @Test
  void producerToSqsToInboxToHandlerProcessesAndDeletesMessage() throws Exception {
    CountDownLatch handled = new CountDownLatch(1);
    AtomicReference<String> receivedBody = new AtomicReference<>();
    InMemoryInboxService inbox = new InMemoryInboxService();
    String queueUrl = createQueue("nerv-orders-success");
    EventHandler<String> handler = handler(event -> {
      receivedBody.set(event.payload());
      handled.countDown();
    });

    startConsumer(
        queueUrl,
        inbox,
        handler,
        2
    );
    String messageId = publish(
        queueUrl,
        "event-success",
        "{\"order\":42}"
    );

    assertThat(messageId).isNotBlank();
    assertThat(
        handled.await(
            20,
            TimeUnit.SECONDS
        )
    ).isTrue();
    awaitStatus(
        inbox,
        new EventId("event-success"),
        InboxStatus.PROCESSED
    );
    assertThat(receivedBody).hasValue("{\"order\":42}");
    assertDeletedAfterVisibility(queueUrl);
  }

  @Test
  void handlerFailureBecomesRetryPendingAndOriginalMessageIsDeleted() throws Exception {
    CountDownLatch attempted = new CountDownLatch(1);
    InMemoryInboxService inbox = new InMemoryInboxService();
    String queueUrl = createQueue("nerv-orders-failure");
    EventHandler<String> handler = handler(event -> {
      attempted.countDown();
      throw new EventRetryableException("temporary downstream failure");
    });

    startConsumer(
        queueUrl,
        inbox,
        handler,
        2
    );
    publish(
        queueUrl,
        "event-failure",
        "{\"order\":99}"
    );

    assertThat(
        attempted.await(
            20,
            TimeUnit.SECONDS
        )
    ).isTrue();
    awaitStatus(
        inbox,
        new EventId("event-failure"),
        InboxStatus.RETRY_PENDING
    );
    assertThat(inbox.find(new EventId("event-failure"))).get()
        .extracting(InboxEvent::attemptCount)
        .isEqualTo(1);
    assertDeletedAfterVisibility(queueUrl);
  }

  private void startConsumer(
      String queueUrl,
      InMemoryInboxService inbox,
      EventHandler<String> handler,
      int maxAttempts
  ) {
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler)),
        stringDeserializer()
    );
    SqsConsumerAdapter adapter = new SqsConsumerAdapter(
        dispatcher,
        inbox,
        Clock.systemUTC(),
        "localstack-test",
        Duration.ofSeconds(5),
        new ExponentialInboxRetryPolicy(
            maxAttempts,
            Duration.ofSeconds(30),
            2,
            Duration.ofMinutes(1)
        ),
        new SqsMessageMapper()
    )
        .forConsumer(
            "orders",
            queueUrl,
            new SqsClientId("localstack")
        );
    NervEventSqsProperties.Consumer properties = new NervEventSqsProperties.Consumer();
    properties.setEnabled(true);
    properties.setClient("localstack");
    properties.setQueue(queueUrl);
    properties.setMaxConcurrentMessages(1);
    properties.setMaxMessagesPerPoll(1);
    properties.setPollTimeout(Duration.ofSeconds(1));
    properties.setVisibilityTimeout(Duration.ofSeconds(2));
    listenerContainer = new SpringSqsConsumerContainerFactory()
        .create(
            "orders",
            properties,
            client(),
            adapter
        );
    listenerContainer.start();
  }

  private String publish(
      String queueUrl,
      String eventId,
      String body
  ) {
    SqsClientRegistry registry = new SqsClientRegistry(
        List.of(
            SqsClientRegistration.applicationProvided(
                new SqsClientId("localstack"),
                client()
            )
        )
    );
    SqsBrokerProducer producer = new SqsBrokerProducer(
        new SqsDestinationResolver(
            Map.of(
                "orders",
                new SqsDestination(
                    new SqsClientId("localstack"),
                    queueUrl
                )
            )
        ),
        registry,
        Duration.ofSeconds(10)
    );
    return producer.publish(
        new BrokerMessage(
            new EventId(eventId),
            "OrderCreated",
            Instant.now(),
            "integration-test",
            "corr-1",
            "orders",
            new SerializedPayload(
                body,
                "application/json"
            )
        )
    ).brokerReference();
  }

  private String createQueue(String name) {
    return client().createQueue(CreateQueueRequest.builder().queueName(name).build()).join().queueUrl();
  }

  private SqsAsyncClient client() {
    if (client == null) {
      client = SqsAsyncClient.builder()
          .endpointOverride(LOCALSTACK.getEndpoint())
          .region(Region.of(LOCALSTACK.getRegion()))
          .credentialsProvider(
              StaticCredentialsProvider.create(
                  AwsBasicCredentials.create(
                      LOCALSTACK.getAccessKey(),
                      LOCALSTACK.getSecretKey()
                  )
              )
          )
          .build();
    }
    return client;
  }

  private void assertDeletedAfterVisibility(String queueUrl) throws InterruptedException {
    Thread.sleep(2500);
    assertThat(
        client().receiveMessage(
            ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(1)
                .maxNumberOfMessages(1)
                .build()
        ).join().messages()
    ).isEmpty();
  }

  private static void awaitStatus(
      InMemoryInboxService inbox,
      EventId eventId,
      InboxStatus expected
  ) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      if (inbox.find(eventId).map(InboxEvent::status).orElse(null) == expected) {
        return;
      }
      Thread.sleep(25);
    }
    assertThat(inbox.find(eventId)).get().extracting(InboxEvent::status).isEqualTo(expected);
  }

  private static EventHandler<String> handler(java.util.function.Consumer<EventMessage<String>> action) {
    return new EventHandler<>() {
      public String eventType() {
        return "OrderCreated";
      }

      public Class<String> payloadType() {
        return String.class;
      }

      public void handle(EventMessage<String> event) {
        action.accept(event);
      }
    };
  }

  private static EventDeserializer stringDeserializer() {
    return new EventDeserializer() {
      @Override
      public <T> T deserialize(
          SerializedPayload payload,
          Class<T> payloadType
      ) {
        return payloadType.cast(payload.value());
      }
    };
  }

  private static final class InMemoryInboxService implements InboxService {
    private InboxEvent event;

    @Override
    public synchronized InboxRegistration register(InboxEvent candidate) {
      if (event == null) {
        event = candidate;
        return new InboxRegistration(
            true,
            event
        );
      }
      return new InboxRegistration(
          false,
          event
      );
    }

    @Override
    public synchronized Optional<InboxEvent> find(EventId eventId) {
      return event != null && event.eventId().equals(eventId) ? Optional.of(event) : Optional.empty();
    }

    @Override
    public synchronized Optional<InboxEvent> claim(
        EventId eventId,
        Instant now,
        String owner,
        Duration leaseDuration
    ) {
      if (event == null || !event.eventId().equals(eventId)) {
        return Optional.empty();
      }
      boolean claimable = event.status() == InboxStatus.RECEIVED
          || event.status() == InboxStatus.RETRY_PENDING && !event.availableAt().isAfter(now)
          || event.status() == InboxStatus.PROCESSING
              && event.processingAt().plus(leaseDuration).isBefore(now);
      if (!claimable) {
        return Optional.empty();
      }
      event = copy(
          InboxStatus.PROCESSING,
          event.attemptCount(),
          null,
          now,
          owner,
          null,
          null,
          null
      );
      return Optional.of(event);
    }

    @Override
    public List<InboxEvent> claimPendingRetries(
        int limit,
        Instant now,
        String owner,
        Duration leaseDuration
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void markProcessed(
        EventId eventId,
        String owner,
        Instant processedAt
    ) {
      requireOwner(
          eventId,
          owner
      );
      event = copy(
          InboxStatus.PROCESSED,
          event.attemptCount(),
          null,
          event.processingAt(),
          owner,
          processedAt,
          null,
          null
      );
    }

    @Override
    public synchronized void markRetryPending(
        EventId eventId,
        String owner,
        int attemptCount,
        Instant failedAt,
        Instant availableAt,
        String error
    ) {
      requireOwner(
          eventId,
          owner
      );
      event = copy(
          InboxStatus.RETRY_PENDING,
          attemptCount,
          availableAt,
          event.processingAt(),
          owner,
          null,
          failedAt,
          error
      );
    }

    @Override
    public synchronized void markFailed(
        EventId eventId,
        String owner,
        int attemptCount,
        Instant failedAt,
        String error
    ) {
      requireOwner(
          eventId,
          owner
      );
      event = copy(
          InboxStatus.FAILED,
          attemptCount,
          null,
          event.processingAt(),
          owner,
          null,
          failedAt,
          error
      );
    }

    private void requireOwner(
        EventId eventId,
        String owner
    ) {
      if (event == null || !event.eventId().equals(eventId) || event.status() != InboxStatus.PROCESSING
          || !owner.equals(event.processingBy())) {
        throw new IllegalStateException("event is not owned");
      }
    }

    private InboxEvent copy(
        InboxStatus status,
        int attempts,
        Instant availableAt,
        Instant processingAt,
        String processingBy,
        Instant processedAt,
        Instant failedAt,
        String error
    ) {
      return new InboxEvent(
          event.eventId(),
          event.eventType(),
          event.timestamp(),
          event.source(),
          event.correlationId(),
          event.payload(),
          status,
          attempts,
          event.receivedAt(),
          availableAt,
          processingAt,
          processingBy,
          processedAt,
          failedAt,
          error
      );
    }
  }
}
