package com.czetsuyatech.nerv.event.persistence.jpa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.exception.EventStateTransitionException;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryDispatcher;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryResult;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import com.czetsuyatech.nerv.event.core.consumer.EventHandlerRegistry;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.exception.EventRetryableException;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.application.mapper.InboxEventMapper;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.InboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.service.InboxRegistrationWriter;
import com.czetsuyatech.nerv.event.persistence.service.impl.InboxServiceImpl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

class InboxServiceImplTest {

  private static final Instant NOW = Instant.parse("2026-08-17T12:00:00Z");
  private static final Duration LEASE = Duration.ofMinutes(1);
  private static AnnotationConfigApplicationContext applicationContext;
  private static InboxService inboxService;
  private static EntityManager entityManager;
  private static TransactionalStore store;

  @BeforeAll
  static void createApplicationContext() {
    applicationContext = new AnnotationConfigApplicationContext(TestConfiguration.class);
    inboxService = applicationContext.getBean(InboxService.class);
    entityManager = applicationContext.getBean(EntityManager.class);
    store = applicationContext.getBean(TransactionalStore.class);
  }

  @AfterAll
  static void closeApplicationContext() {
    applicationContext.close();
  }

  @BeforeEach
  void clearInbox() {
    store.clear();
  }

  @Test
  void registerStoresReadablePayloadAndMetadata() {
    InboxEvent event = receivedEvent("{\"orderId\":\"123\"}");

    InboxRegistration registration = inboxService.register(event);

    assertThat(registration.created()).isTrue();
    InboxEventEntity stored = entityManager.find(
        InboxEventEntity.class,
        event.eventId().value()
    );
    assertThat(stored.getPayload()).isEqualTo("{\"orderId\":\"123\"}");
    assertThat(stored.getContentType()).isEqualTo("application/json");
    assertThat(stored.getEventType()).isEqualTo(event.eventType());
    assertThat(stored.getSource()).isEqualTo(event.source());
    assertThat(stored.getCorrelationId()).isEqualTo(event.correlationId());
    assertThat(stored.getStatus()).isEqualTo(InboxStatus.RECEIVED);
  }

  @Test
  void duplicateRegistrationReturnsExistingEvent() {
    InboxEvent original = receivedEvent("{\"first\":true}");
    inboxService.register(original);
    InboxEvent duplicate = new InboxEvent(
        original.eventId(),
        "different",
        NOW,
        "other-source",
        null,
        new SerializedPayload(
            "{\"second\":true}",
            "application/json"
        ),
        InboxStatus.RECEIVED,
        0,
        NOW,
        null,
        null,
        null,
        null,
        null,
        null
    );

    InboxRegistration registration = inboxService.register(duplicate);

    assertThat(registration.created()).isFalse();
    assertThat(registration.event()).isEqualTo(original);
    assertThat(count()).isEqualTo(1);
  }

  @Test
  void concurrentDuplicateRegistrationCreatesOneRow() throws Exception {
    InboxEvent event = receivedEvent("{\"concurrent\":true}");
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      List<Future<InboxRegistration>> results = executor.invokeAll(
          List.of(
              () -> inboxService.register(event),
              () -> inboxService.register(event)
          )
      );

      assertThat(results.stream().map(this::get).filter(InboxRegistration::created)).hasSize(1);
      assertThat(count()).isEqualTo(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void receivedEventCanBeClaimedOnlyOnceWhileLeaseIsActive() {
    InboxEvent event = receivedEvent("{}");
    inboxService.register(event);

    assertThat(
        inboxService.claim(
            event.eventId(),
            NOW,
            "worker-a",
            LEASE
        )
    ).isPresent();
    assertThat(
        inboxService.claim(
            event.eventId(),
            NOW,
            "worker-b",
            LEASE
        )
    ).isEmpty();
    assertThat(inboxService.find(event.eventId())).get().satisfies(claimed -> {
      assertThat(claimed.status()).isEqualTo(InboxStatus.PROCESSING);
      assertThat(claimed.processingAt()).isEqualTo(NOW);
      assertThat(claimed.processingBy()).isEqualTo("worker-a");
    });
  }

  @Test
  void expiredLeaseCanBeReclaimed() {
    InboxEvent event = receivedEvent("{}");
    inboxService.register(event);
    inboxService.claim(
        event.eventId(),
        NOW,
        "worker-a",
        LEASE
    );

    assertThat(
        inboxService.claim(
            event.eventId(),
            NOW.plus(LEASE),
            "worker-b",
            LEASE
        )
    ).isPresent();
    assertThat(inboxService.find(event.eventId())).get()
        .extracting(InboxEvent::processingBy)
        .isEqualTo("worker-b");
  }

  @Test
  void concurrentClaimsHaveOneWinner() throws Exception {
    InboxEvent event = receivedEvent("{}");
    inboxService.register(event);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      List<Future<Boolean>> results = executor.invokeAll(
          List.of(
              claim(
                  event.eventId(),
                  "worker-a"
              ),
              claim(
                  event.eventId(),
                  "worker-b"
              )
          )
      );

      assertThat(results.stream().map(this::get).filter(Boolean::booleanValue)).hasSize(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void ownerCanMarkClaimedEventProcessedAndWrongOwnerIsRejected() {
    InboxEvent event = receivedEvent("{}");
    inboxService.register(event);
    inboxService.claim(
        event.eventId(),
        NOW,
        "worker-a",
        LEASE
    );

    assertThatThrownBy(
        () -> inboxService.markProcessed(
            event.eventId(),
            "worker-b",
            NOW
        )
    )
        .isInstanceOf(EventStateTransitionException.class);
    inboxService.markProcessed(
        event.eventId(),
        "worker-a",
        NOW
    );

    assertThat(inboxService.find(event.eventId())).get().satisfies(processed -> {
      assertThat(processed.status()).isEqualTo(InboxStatus.PROCESSED);
      assertThat(processed.attemptCount()).isEqualTo(1);
      assertThat(processed.processedAt()).isEqualTo(NOW);
      assertThat(processed.processingAt()).isNull();
      assertThat(processed.processingBy()).isNull();
    });
  }

  @Test
  void processingEventCanBeMarkedRetryPendingOnlyByItsOwner() {
    InboxEvent event = receivedEvent("{}");
    inboxService.register(event);
    inboxService.claim(
        event.eventId(),
        NOW,
        "worker-a",
        LEASE
    );
    String error = "x".repeat(InboxServiceImpl.MAX_LAST_ERROR_LENGTH + 1);
    Instant availableAt = NOW.plusSeconds(10);

    assertThatThrownBy(
        () -> inboxService.markRetryPending(
            event.eventId(),
            "worker-b",
            2,
            NOW,
            availableAt,
            error
        )
    ).isInstanceOf(EventStateTransitionException.class);
    inboxService.markRetryPending(
        event.eventId(),
        "worker-a",
        2,
        NOW,
        availableAt,
        error
    );

    assertThat(inboxService.find(event.eventId())).get().satisfies(retryPending -> {
      assertThat(retryPending.status()).isEqualTo(InboxStatus.RETRY_PENDING);
      assertThat(retryPending.availableAt()).isEqualTo(availableAt);
      assertThat(retryPending.attemptCount()).isEqualTo(2);
      assertThat(retryPending.failedAt()).isEqualTo(NOW);
      assertThat(retryPending.processingAt()).isNull();
      assertThat(retryPending.processingBy()).isNull();
      assertThat(retryPending.processedAt()).isNull();
      assertThat(retryPending.lastError()).hasSize(InboxServiceImpl.MAX_LAST_ERROR_LENGTH);
    });
  }

  @Test
  void dueRetryPendingEventsCanBeClaimedButFutureAndTerminalEventsCannot() {
    InboxEvent due = receivedEvent("{}");
    InboxEvent future = receivedEvent("{}");
    InboxEvent terminal = receivedEvent("{}");
    InboxEvent processed = receivedEvent("{}");
    inboxService.register(due);
    inboxService.register(future);
    inboxService.register(terminal);
    inboxService.register(processed);
    retryPending(
        due,
        NOW
    );
    retryPending(
        future,
        NOW.plusSeconds(1)
    );
    claimAndFail(terminal);
    claimAndProcess(processed);

    assertThat(
        inboxService.claimPendingRetries(
            10,
            NOW,
            "worker-b",
            LEASE
        )
    )
        .singleElement()
        .satisfies(claimed -> {
          assertThat(claimed.eventId()).isEqualTo(due.eventId());
          assertThat(claimed.status()).isEqualTo(InboxStatus.PROCESSING);
          assertThat(claimed.availableAt()).isNull();
          assertThat(claimed.processingAt()).isEqualTo(NOW);
          assertThat(claimed.processingBy()).isEqualTo("worker-b");
        });
    assertThat(
        inboxService.claimPendingRetries(
            10,
            NOW,
            "worker-c",
            LEASE
        )
    ).isEmpty();
  }

  @Test
  void processingEventCanBeMarkedFailedOnlyByItsOwner() {
    InboxEvent event = receivedEvent("{}");
    inboxService.register(event);
    inboxService.claim(
        event.eventId(),
        NOW,
        "worker-a",
        LEASE
    );

    assertThatThrownBy(
        () -> inboxService.markFailed(
            event.eventId(),
            "worker-b",
            3,
            NOW,
            "failed"
        )
    ).isInstanceOf(EventStateTransitionException.class);
    inboxService.markFailed(
        event.eventId(),
        "worker-a",
        3,
        NOW,
        "failed"
    );

    assertThat(inboxService.find(event.eventId())).get().satisfies(failed -> {
      assertThat(failed.status()).isEqualTo(InboxStatus.FAILED);
      assertThat(failed.availableAt()).isNull();
      assertThat(failed.attemptCount()).isEqualTo(3);
      assertThat(failed.failedAt()).isEqualTo(NOW);
      assertThat(failed.processingAt()).isNull();
      assertThat(failed.processingBy()).isNull();
      assertThat(failed.processedAt()).isNull();
    });
  }

  @Test
  void processedEventClearsRetryAndFailureDetails() {
    InboxEvent event = receivedEvent("{}");
    inboxService.register(event);
    retryPending(
        event,
        NOW
    );
    inboxService.claimPendingRetries(
        1,
        NOW,
        "worker-b",
        LEASE
    );

    inboxService.markProcessed(
        event.eventId(),
        "worker-b",
        NOW.plusSeconds(1)
    );

    assertThat(inboxService.find(event.eventId())).get().satisfies(processed -> {
      assertThat(processed.status()).isEqualTo(InboxStatus.PROCESSED);
      assertThat(processed.attemptCount()).isEqualTo(2);
      assertThat(processed.processedAt()).isEqualTo(NOW.plusSeconds(1));
      assertThat(processed.processingAt()).isNull();
      assertThat(processed.processingBy()).isNull();
      assertThat(processed.availableAt()).isNull();
      assertThat(processed.failedAt()).isNull();
      assertThat(processed.lastError()).isNull();
    });
  }

  @Test
  void retryDispatcherPersistsSuccessRetryAndTerminalFailureWithoutChangingPayload() {
    InboxEvent success = receivedEvent("{\"orderId\":\"success\"}");
    inboxService.register(success);
    retryPending(
        success,
        NOW
    );

    InboxRetryDispatcher successDispatcher = retryDispatcher(message -> {
    },
        policy(true)
    );
    InboxRetryResult successResult = successDispatcher.dispatch(1);
    assertThat(successResult).isEqualTo(
        new InboxRetryResult(
            1,
            1,
            0,
            0,
            0
        )
    );
    assertThat(inboxService.find(success.eventId())).get().satisfies(event -> {
      assertThat(event.status()).isEqualTo(InboxStatus.PROCESSED);
      assertThat(event.payload()).isEqualTo(success.payload());
    });

    InboxEvent retry = receivedEvent("{\"orderId\":\"retry\"}");
    inboxService.register(retry);
    retryPending(
        retry,
        NOW
    );
    InboxRetryDispatcher retryDispatcher = retryDispatcher(message -> {
      throw new EventRetryableException("handler unavailable");
    },
        policy(true)
    );
    InboxRetryResult retryResult = retryDispatcher.dispatch(1);
    assertThat(retryResult).isEqualTo(
        new InboxRetryResult(
            1,
            0,
            1,
            0,
            0
        )
    );
    assertThat(inboxService.find(retry.eventId())).get().satisfies(event -> {
      assertThat(event.status()).isEqualTo(InboxStatus.RETRY_PENDING);
      assertThat(event.attemptCount()).isEqualTo(2);
      assertThat(event.payload()).isEqualTo(retry.payload());
    });

    InboxEvent failed = receivedEvent("{\"orderId\":\"failed\"}");
    inboxService.register(failed);
    retryPending(
        failed,
        NOW
    );
    InboxRetryDispatcher failedDispatcher = retryDispatcher(message -> {
      throw new IllegalStateException("handler unavailable");
    },
        policy(false)
    );
    InboxRetryResult failedResult = failedDispatcher.dispatch(1);
    assertThat(failedResult).isEqualTo(
        new InboxRetryResult(
            1,
            0,
            0,
            1,
            0
        )
    );
    assertThat(inboxService.find(failed.eventId())).get().satisfies(event -> {
      assertThat(event.status()).isEqualTo(InboxStatus.FAILED);
      assertThat(event.attemptCount()).isEqualTo(2);
      assertThat(event.payload()).isEqualTo(failed.payload());
    });
  }

  /**
   * <p>
   * H2 lock coverage only; it does not prove PostgreSQL locking semantics.
   * </p>
   *
   * <p>
   * TODO: verify these competing retry claims against PostgreSQL through Testcontainers.
   * </p>
   */
  @Test
  void concurrentPendingRetryClaimsHaveOneWinner() throws Exception {
    InboxEvent event = receivedEvent("{}");
    inboxService.register(event);
    retryPending(
        event,
        NOW
    );
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      List<Future<Integer>> results = executor.invokeAll(
          List.of(
              () -> inboxService.claimPendingRetries(
                  1,
                  NOW,
                  "worker-b",
                  LEASE
              ).size(),
              () -> inboxService.claimPendingRetries(
                  1,
                  NOW,
                  "worker-c",
                  LEASE
              ).size()
          )
      );

      assertThat(results.stream().map(this::get).filter(result -> result == 1)).hasSize(1);
    } finally {
      executor.shutdownNow();
    }
  }

  private Callable<Boolean> claim(
      EventId eventId,
      String owner
  ) {
    return () -> inboxService.claim(
        eventId,
        NOW,
        owner,
        LEASE
    ).isPresent();
  }

  private void retryPending(
      InboxEvent event,
      Instant availableAt
  ) {
    inboxService.claim(
        event.eventId(),
        NOW,
        "worker-a",
        LEASE
    );
    inboxService.markRetryPending(
        event.eventId(),
        "worker-a",
        1,
        NOW,
        availableAt,
        "failed"
    );
  }

  private void claimAndFail(InboxEvent event) {
    inboxService.claim(
        event.eventId(),
        NOW,
        "worker-a",
        LEASE
    );
    inboxService.markFailed(
        event.eventId(),
        "worker-a",
        1,
        NOW,
        "failed"
    );
  }

  private void claimAndProcess(InboxEvent event) {
    inboxService.claim(
        event.eventId(),
        NOW,
        "worker-a",
        LEASE
    );
    inboxService.markProcessed(
        event.eventId(),
        "worker-a",
        NOW
    );
  }

  private InboxEvent receivedEvent(String payload) {
    return new InboxEvent(
        new EventId(UUID.randomUUID().toString()),
        "order.created",
        NOW,
        "orders",
        "correlation-1",
        new SerializedPayload(
            payload,
            "application/json"
        ),
        InboxStatus.RECEIVED,
        0,
        NOW,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  private InboxRetryDispatcher retryDispatcher(
      ConsumerAction action,
      InboxRetryPolicy policy
  ) {
    return new InboxRetryDispatcher(
        inboxService,
        new TestConsumerDispatcher(action),
        policy,
        Clock.fixed(
            NOW,
            ZoneOffset.UTC
        ),
        "worker-retry",
        LEASE
    );
  }

  private static InboxRetryPolicy policy(boolean retryAllowed) {
    return new InboxRetryPolicy() {
      @Override
      public boolean canRetry(int attemptCount) {
        return retryAllowed;
      }

      @Override
      public Instant nextAttemptAt(
          int attemptCount,
          Instant failedAt
      ) {
        return failedAt.plusSeconds(30);
      }
    };
  }

  @FunctionalInterface
  private interface ConsumerAction {
    void accept(ConsumerMessage message);
  }

  private static final class TestConsumerDispatcher extends ConsumerDispatcher {
    private final ConsumerAction action;

    private TestConsumerDispatcher(ConsumerAction action) {
      super(
          new EventHandlerRegistry(List.of()),
          new NoOpEventDeserializer(),
          List.of()
      );
      this.action = action;
    }

    @Override
    public void dispatch(ConsumerMessage message) {
      action.accept(message);
    }
  }

  private static final class NoOpEventDeserializer implements EventDeserializer {
    @Override
    public <T> T deserialize(
        SerializedPayload payload,
        Class<T> payloadType
    ) {
      throw new UnsupportedOperationException();
    }
  }

  private long count() {
    return entityManager.createQuery(
        "select count(event) from InboxEventEntity event",
        Long.class
    )
        .getSingleResult();
  }

  private <T> T get(Future<T> future) {
    try {
      return future.get();
    } catch (Exception exception) {
      throw new AssertionError(
          "Concurrent operation failed",
          exception
      );
    }
  }

  @Configuration(proxyBeanMethods = false)
  @ComponentScan(basePackageClasses = InboxEventMapper.class)
  @EnableTransactionManagement
  @EnableJpaRepositories(basePackageClasses = InboxEventRepository.class)
  static class TestConfiguration {

    @Bean(destroyMethod = "close")
    EntityManagerFactory entityManagerFactory() {
      return Persistence.createEntityManagerFactory("nerv-event-test");
    }

    @Bean
    PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
      return new JpaTransactionManager(entityManagerFactory);
    }

    @Bean
    @Primary
    EntityManager entityManager(EntityManagerFactory entityManagerFactory) {
      return SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory);
    }

    @Bean
    InboxRegistrationWriter inboxRegistrationWriter(InboxEventRepository entityRepository) {
      return new InboxRegistrationWriter(entityRepository);
    }

    @Bean
    InboxService inboxRepository(
        InboxEventRepository entityRepository,
        InboxRegistrationWriter registrationWriter,
        InboxEventMapper mapper
    ) {
      return new InboxServiceImpl(
          entityRepository,
          registrationWriter,
          mapper
      );
    }

    @Bean
    TransactionalStore transactionalStore(EntityManager entityManager) {
      return new TransactionalStore(entityManager);
    }
  }

  static class TransactionalStore {

    private final EntityManager entityManager;

    TransactionalStore(EntityManager entityManager) {
      this.entityManager = entityManager;
    }

    @Transactional
    public void clear() {
      entityManager.createQuery("delete from InboxEventEntity").executeUpdate();
    }
  }
}
