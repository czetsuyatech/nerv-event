package com.czetsuyatech.nerv.event.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.consumer.EventHandler;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.exception.EventRetryableException;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.EventHandlerRegistry;
import com.czetsuyatech.nerv.event.core.inbox.ExponentialInboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.kafka.autoconfigure.NervEventKafkaProperties;
import com.czetsuyatech.nerv.event.kafka.consumer.KafkaConsumerAdapter;
import com.czetsuyatech.nerv.event.kafka.consumer.SpringKafkaConsumerContainerFactory;
import com.czetsuyatech.nerv.event.kafka.producer.KafkaBrokerProducer;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class KafkaBrokerIntegrationIT {
  @Container
  static final KafkaContainer KAFKA = new KafkaContainer(
      DockerImageName.parse("apache/kafka-native:3.9.1")
  );

  private MessageListenerContainer listenerContainer;
  private DefaultKafkaProducerFactory<String, String> producerFactory;

  @AfterEach
  void stopResources() {
    if (listenerContainer != null) {
      listenerContainer.stop();
    }
    if (producerFactory != null) {
      producerFactory.destroy();
    }
  }

  @Test
  void producerToKafkaToInboxToHandlerProcessesAndCommitsOffset() throws Exception {
    String topic = "nerv-orders-success";
    String group = "nerv-orders-success-group";
    createTopic(topic);
    CountDownLatch handled = new CountDownLatch(1);
    AtomicReference<String> receivedBody = new AtomicReference<>();
    InMemoryInboxService inbox = new InMemoryInboxService();
    EventHandler<String> handler = handler(event -> {
      receivedBody.set(event.payload());
      handled.countDown();
    });

    startConsumer(
        topic,
        group,
        inbox,
        handler,
        2
    );
    publish(
        topic,
        "event-success",
        "{\"order\":42}"
    );

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
    awaitCommittedOffset(
        group,
        topic,
        1L
    );
    assertThat(receivedBody).hasValue("{\"order\":42}");
  }

  @Test
  void handlerFailureBecomesRetryPendingAndCommitsOriginalOffset() throws Exception {
    String topic = "nerv-orders-failure";
    String group = "nerv-orders-failure-group";
    createTopic(topic);
    CountDownLatch attempted = new CountDownLatch(1);
    InMemoryInboxService inbox = new InMemoryInboxService();
    EventHandler<String> handler = handler(event -> {
      attempted.countDown();
      throw new EventRetryableException("temporary downstream failure");
    });

    startConsumer(
        topic,
        group,
        inbox,
        handler,
        2
    );
    publish(
        topic,
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
    awaitCommittedOffset(
        group,
        topic,
        1L
    );
    assertThat(inbox.find(new EventId("event-failure"))).get()
        .extracting(InboxEvent::attemptCount)
        .isEqualTo(1);
  }

  private void startConsumer(
      String topic,
      String group,
      InMemoryInboxService inbox,
      EventHandler<String> handler,
      int maxAttempts
  ) {
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler)),
        stringDeserializer(),
        List.of()
    );
    KafkaConsumerAdapter adapter = new KafkaConsumerAdapter(
        dispatcher,
        inbox,
        Clock.systemUTC(),
        "kafka-integration",
        Duration.ofSeconds(5),
        new ExponentialInboxRetryPolicy(
            maxAttempts,
            Duration.ofSeconds(30),
            2,
            Duration.ofMinutes(1)
        )
    ).forConsumer(
        "orders",
        Duration.ofSeconds(5)
    );
    NervEventKafkaProperties.Consumer consumer = new NervEventKafkaProperties.Consumer();
    consumer.setEnabled(true);
    consumer.setTopic(topic);
    consumer.setGroupId(group);
    consumer.setConcurrency(1);
    consumer.setLeaseDuration(Duration.ofSeconds(5));
    listenerContainer = new SpringKafkaConsumerContainerFactory(
        new DefaultKafkaConsumerFactory<>(consumerProperties(group))
    )
        .create(
            "orders",
            consumer,
            adapter
        );
    listenerContainer.start();
  }

  private String publish(
      String topic,
      String eventId,
      String body
  ) {
    producerFactory = new DefaultKafkaProducerFactory<>(producerProperties());
    KafkaBrokerProducer producer = new KafkaBrokerProducer(
        new KafkaTemplate<>(producerFactory),
        Duration.ofSeconds(10)
    );
    return producer.publish(
        new BrokerMessage(
            new EventId(eventId),
            "OrderCreated",
            Instant.now(),
            "integration-test",
            "corr-1",
            topic,
            new SerializedPayload(
                body,
                "application/json"
            )
        )
    ).brokerReference();
  }

  private static void createTopic(String topic) throws Exception {
    try (AdminClient admin = AdminClient.create(adminProperties())) {
      admin.createTopics(
          List.of(
              new NewTopic(
                  topic,
                  1,
                  (short) 1
              )
          )
      )
          .all()
          .get(
              10,
              TimeUnit.SECONDS
          );
    }
  }

  private static void awaitCommittedOffset(
      String group,
      String topic,
      long expected
  )
      throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    try (AdminClient admin = AdminClient.create(adminProperties())) {
      while (System.nanoTime() < deadline) {
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        var offset = offsets.get(
            new TopicPartition(
                topic,
                0
            )
        );
        if (offset != null && offset.offset() == expected) {
          return;
        }
        Thread.sleep(50);
      }
      var offset = admin.listConsumerGroupOffsets(group)
          .partitionsToOffsetAndMetadata()
          .get()
          .get(
              new TopicPartition(
                  topic,
                  0
              )
          );
      assertThat(offset).isNotNull();
      assertThat(offset.offset()).isEqualTo(expected);
    }
  }

  private static Map<String, Object> producerProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA.getBootstrapServers()
    );
    properties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class
    );
    properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class
    );
    properties.put(
        ProducerConfig.ACKS_CONFIG,
        "all"
    );
    return properties;
  }

  private static Map<String, Object> consumerProperties(String group) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA.getBootstrapServers()
    );
    properties.put(
        ConsumerConfig.GROUP_ID_CONFIG,
        group
    );
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class
    );
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class
    );
    properties.put(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "earliest"
    );
    properties.put(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        false
    );
    return properties;
  }

  private static Properties adminProperties() {
    Properties properties = new Properties();
    properties.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA.getBootstrapServers()
    );
    return properties;
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

    public synchronized Optional<InboxEvent> find(EventId id) {
      return event != null && event.eventId().equals(id) ? Optional.of(event) : Optional.empty();
    }

    public synchronized Optional<InboxEvent> claim(
        EventId id,
        Instant now,
        String owner,
        Duration lease
    ) {
      if (event == null || !event.eventId().equals(id))
        return Optional.empty();
      boolean claimable = event.status() == InboxStatus.RECEIVED
          || event.status() == InboxStatus.RETRY_PENDING && !event.availableAt().isAfter(now)
          || event.status() == InboxStatus.PROCESSING && event.processingAt().plus(lease).isBefore(now);
      if (!claimable)
        return Optional.empty();
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

    public List<InboxEvent> claimPendingRetries(
        int limit,
        Instant now,
        String owner,
        Duration lease
    ) {
      throw new UnsupportedOperationException();
    }

    public synchronized void markProcessed(
        EventId id,
        String owner,
        Instant at
    ) {
      requireOwner(
          id,
          owner
      );
      event = copy(
          InboxStatus.PROCESSED,
          event.attemptCount(),
          null,
          event.processingAt(),
          owner,
          at,
          null,
          null
      );
    }

    public synchronized void markRetryPending(
        EventId id,
        String owner,
        int attempts,
        Instant failedAt,
        Instant availableAt,
        String error
    ) {
      requireOwner(
          id,
          owner
      );
      event = copy(
          InboxStatus.RETRY_PENDING,
          attempts,
          availableAt,
          event.processingAt(),
          owner,
          null,
          failedAt,
          error
      );
    }

    public synchronized void markFailed(
        EventId id,
        String owner,
        int attempts,
        Instant failedAt,
        String error
    ) {
      requireOwner(
          id,
          owner
      );
      event = copy(
          InboxStatus.FAILED,
          attempts,
          null,
          event.processingAt(),
          owner,
          null,
          failedAt,
          error
      );
    }

    private void requireOwner(
        EventId id,
        String owner
    ) {
      if (event == null || !event.eventId().equals(id) || event.status() != InboxStatus.PROCESSING
          || !owner.equals(event.processingBy()))
        throw new IllegalStateException("event is not owned");
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
