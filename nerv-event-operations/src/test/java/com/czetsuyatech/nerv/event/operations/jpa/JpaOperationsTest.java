package com.czetsuyatech.nerv.event.operations.jpa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.application.dto.InboxEventView;
import com.czetsuyatech.nerv.event.application.dto.InboxQuery;
import com.czetsuyatech.nerv.event.application.dto.OutboxEventView;
import com.czetsuyatech.nerv.event.application.dto.OutboxQuery;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.exception.ManualRetryRejectedException;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.repository.InboxOperationRepository;
import com.czetsuyatech.nerv.event.persistence.repository.OutboxOperationRepository;
import com.czetsuyatech.nerv.event.persistence.repository.OperationsMetrics;
import com.czetsuyatech.nerv.event.services.InboxOperationService;
import com.czetsuyatech.nerv.event.services.OutboxOperationService;
import com.czetsuyatech.nerv.event.services.impl.InboxOperationServiceImpl;
import com.czetsuyatech.nerv.event.services.impl.OutboxOperationServiceImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

class JpaOperationsTest {

  private static final Instant NOW = Instant.parse("2026-08-18T12:00:00Z");
  private static AnnotationConfigApplicationContext context;
  private static OutboxOperationService outboxOperationService;
  private static InboxOperationService inboxOperationService;
  private static Store store;
  private static SimpleMeterRegistry registry;

  @BeforeAll
  static void setUpContext() {
    context = new AnnotationConfigApplicationContext(TestConfiguration.class);
    outboxOperationService = context.getBean(OutboxOperationService.class);
    inboxOperationService = context.getBean(InboxOperationService.class);
    store = context.getBean(Store.class);
    registry = context.getBean(SimpleMeterRegistry.class);
  }

  @AfterAll
  static void closeContext() {
    context.close();
  }

  @BeforeEach
  void clear() {
    store.clear();
    registry.clear();
  }

  @Test
  void findsExactOutboxWithPayloadAndReturnsEmptyForUnknownRow() {
    store.save(
        outbox(
            "outbox-1",
            "event-1",
            OutboxStatus.FAILED,
            "orders",
            "order.created"
        )
    );

    assertThat(outboxOperationService.find(new OutboxId("outbox-1")))
        .get()
        .extracting(
            OutboxEventView::payload,
            OutboxEventView::lastError
        )
        .containsExactly(
            "{\"id\":1}",
            "broker unavailable"
        );
    assertThat(outboxOperationService.find(new OutboxId("missing"))).isEmpty();
  }

  @Test
  void searchesOutboxByFailedDestinationTypeAndPaginatesWithoutPayload() {
    store.save(
        outbox(
            "outbox-1",
            "event-1",
            OutboxStatus.FAILED,
            "orders",
            "order.created"
        )
    );
    store.save(
        outbox(
            "outbox-2",
            "event-2",
            OutboxStatus.PENDING,
            "billing",
            "invoice.created"
        )
    );
    store.save(
        outbox(
            "outbox-3",
            "event-3",
            OutboxStatus.FAILED,
            "orders",
            "order.created"
        )
    );

    OutboxQuery failedOrders = new OutboxQuery(
        OutboxStatus.FAILED,
        null,
        "order.created",
        "orders",
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        1
    );
    var firstPage = outboxOperationService.search(failedOrders);
    var secondPage = outboxOperationService.search(
        new OutboxQuery(
            OutboxStatus.FAILED,
            null,
            "order.created",
            "orders",
            null,
            null,
            null,
            null,
            null,
            null,
            1,
            1
        )
    );

    assertThat(firstPage.totalElements()).isEqualTo(2);
    assertThat(firstPage.totalPages()).isEqualTo(2);
    assertThat(firstPage.events()).allSatisfy(event -> assertThat(event.payload()).isNull());
    assertThat(firstPage.events().getFirst().outboxId()).isNotEqualTo(secondPage.events().getFirst().outboxId());
  }

  @Test
  void retriesFailedOutboxInTheSameRowAndPreservesRecoveryHistory() {
    OutboxEventEntity original = outbox(
        "outbox-1",
        "event-1",
        OutboxStatus.FAILED,
        "orders",
        "order.created"
    );
    store.save(original);

    OutboxEventView retried = outboxOperationService.retryFailed(new OutboxId("outbox-1"));

    assertThat(retried.status()).isEqualTo(OutboxStatus.PENDING);
    assertThat(retried.availableAt()).isEqualTo(NOW);
    assertThat(retried.outboxId().value()).isEqualTo("outbox-1");
    assertThat(retried.eventId().value()).isEqualTo("event-1");
    assertThat(retried.payload()).isEqualTo("{\"id\":1}");
    assertThat(retried.attemptCount()).isEqualTo(5);
    assertThat(retried.lastError()).isEqualTo("broker unavailable");
    assertThat(store.outboxCount()).isOne();
    assertThat(
        registry.counter(
            "nerv.event.operations.retry",
            "direction",
            "outbox",
            "result",
            "success"
        )
            .count()
    ).isEqualTo(1.0);
  }

  @Test
  void rejectsNonFailedOutboxRetryAndRecordsBoundedMetric() {
    store.save(
        outbox(
            "outbox-1",
            "event-1",
            OutboxStatus.PUBLISHED,
            "orders",
            "order.created"
        )
    );

    assertThatThrownBy(() -> outboxOperationService.retryFailed(new OutboxId("outbox-1")))
        .isInstanceOf(ManualRetryRejectedException.class)
        .hasMessageContaining("only FAILED is retryable");
    assertThat(
        registry.counter(
            "nerv.event.operations.retry",
            "direction",
            "outbox",
            "result",
            "rejected"
        )
            .count()
    ).isEqualTo(1.0);
  }

  @Test
  void rejectsEveryOtherOutboxState() {
    for (OutboxStatus status : new OutboxStatus[]{
        OutboxStatus.PENDING, OutboxStatus.PROCESSING, OutboxStatus.PUBLISHED
    }) {
      store.clear();
      store.save(
          outbox(
              "outbox-1",
              "event-1",
              status,
              "orders",
              "order.created"
          )
      );
      assertThatThrownBy(() -> outboxOperationService.retryFailed(new OutboxId("outbox-1")))
          .isInstanceOf(ManualRetryRejectedException.class);
    }
  }

  @Test
  void concurrentOutboxRetryHasExactlyOneWinner() throws Exception {
    store.save(
        outbox(
            "outbox-1",
            "event-1",
            OutboxStatus.FAILED,
            "orders",
            "order.created"
        )
    );
    try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
      CountDownLatch start = new CountDownLatch(1);
      Future<Boolean> first = pool.submit(() -> retryOutboxAfter(start));
      Future<Boolean> second = pool.submit(() -> retryOutboxAfter(start));
      start.countDown();

      assertThat((first.get() ? 1 : 0) + (second.get() ? 1 : 0)).isEqualTo(1);
    }
  }

  @Test
  void findsAndSearchesInboxByFailedRetryPendingTypeAndDateRange() {
    store.save(
        inbox(
            "event-1",
            InboxStatus.FAILED,
            "order.created",
            NOW.minusSeconds(120)
        )
    );
    store.save(
        inbox(
            "event-2",
            InboxStatus.RETRY_PENDING,
            "order.created",
            NOW.minusSeconds(60)
        )
    );
    store.save(
        inbox(
            "event-3",
            InboxStatus.PROCESSED,
            "invoice.created",
            NOW.minusSeconds(10)
        )
    );

    assertThat(inboxOperationService.find(new EventId("event-1"))).get()
        .extracting(
            InboxEventView::payload,
            InboxEventView::lastError
        )
        .containsExactly(
            "{\"id\":1}",
            "handler failed"
        );
    var result = inboxOperationService.search(
        new InboxQuery(
            InboxStatus.RETRY_PENDING,
            null,
            "order.created",
            "app",
            null,
            NOW.minusSeconds(90),
            NOW,
            null,
            null,
            0,
            10
        )
    );

    assertThat(result.events()).singleElement().satisfies(event -> {
      assertThat(event.eventId().value()).isEqualTo("event-2");
      assertThat(event.payload()).isNull();
    });
    assertThat(
        inboxOperationService.search(
            new InboxQuery(
                InboxStatus.FAILED,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                0,
                1
            )
        ).events()
    ).singleElement()
        .extracting(event -> event.eventId().value())
        .isEqualTo("event-1");
  }

  @Test
  void retriesFailedInboxWithoutInvokingAHandlerAndPreservesTheRow() {
    store.save(
        inbox(
            "event-1",
            InboxStatus.FAILED,
            "order.created",
            NOW.minusSeconds(10)
        )
    );

    InboxEventView retried = inboxOperationService.retryFailed(new EventId("event-1"));

    assertThat(retried.status()).isEqualTo(InboxStatus.RETRY_PENDING);
    assertThat(retried.availableAt()).isEqualTo(NOW);
    assertThat(retried.eventId().value()).isEqualTo("event-1");
    assertThat(retried.payload()).isEqualTo("{\"id\":1}");
    assertThat(retried.attemptCount()).isEqualTo(5);
    assertThat(retried.lastError()).isEqualTo("handler failed");
    assertThat(store.inboxCount()).isOne();
    assertThat(
        registry.counter(
            "nerv.event.operations.retry",
            "direction",
            "inbox",
            "result",
            "success"
        )
            .count()
    ).isEqualTo(1.0);
  }

  @Test
  void rejectsNonFailedInboxRetry() {
    store.save(
        inbox(
            "event-1",
            InboxStatus.PROCESSED,
            "order.created",
            NOW
        )
    );

    assertThatThrownBy(() -> inboxOperationService.retryFailed(new EventId("event-1")))
        .isInstanceOf(ManualRetryRejectedException.class)
        .hasMessageContaining("only FAILED is retryable");
  }

  @Test
  void rejectsEveryOtherInboxState() {
    for (InboxStatus status : new InboxStatus[]{
        InboxStatus.RECEIVED, InboxStatus.PROCESSING, InboxStatus.RETRY_PENDING, InboxStatus.PROCESSED
    }) {
      store.clear();
      store.save(
          inbox(
              "event-1",
              status,
              "order.created",
              NOW
          )
      );
      assertThatThrownBy(() -> inboxOperationService.retryFailed(new EventId("event-1")))
          .isInstanceOf(ManualRetryRejectedException.class);
    }
  }

  @Test
  void concurrentInboxRetryHasExactlyOneWinner() throws Exception {
    store.save(
        inbox(
            "event-1",
            InboxStatus.FAILED,
            "order.created",
            NOW
        )
    );
    try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
      CountDownLatch start = new CountDownLatch(1);
      Future<Boolean> first = pool.submit(() -> retryInboxAfter(start));
      Future<Boolean> second = pool.submit(() -> retryInboxAfter(start));
      start.countDown();

      assertThat((first.get() ? 1 : 0) + (second.get() ? 1 : 0)).isEqualTo(1);
    }
  }

  private static boolean retryOutboxAfter(CountDownLatch start) throws Exception {
    start.await();
    try {
      outboxOperationService.retryFailed(new OutboxId("outbox-1"));
      return true;
    } catch (ManualRetryRejectedException ignored) {
      return false;
    }
  }

  private static boolean retryInboxAfter(CountDownLatch start) throws Exception {
    start.await();
    try {
      inboxOperationService.retryFailed(new EventId("event-1"));
      return true;
    } catch (ManualRetryRejectedException ignored) {
      return false;
    }
  }

  private static OutboxEventEntity outbox(
      String id,
      String eventId,
      OutboxStatus status,
      String destination,
      String eventType
  ) {
    OutboxEventEntity event = new OutboxEventEntity();
    event.setId(id);
    event.setEventId(eventId);
    event.setEventType(eventType);
    event.setEventTimestamp(NOW.minusSeconds(300));
    event.setSource("app");
    event.setCorrelationId("correlation-1");
    event.setDestination(destination);
    event.setPayload("{\"id\":1}");
    event.setStatus(status);
    event.setAttemptCount(5);
    event.setAvailableAt(status == OutboxStatus.PENDING ? NOW : NOW.minusSeconds(30));
    event.setLockedAt(status == OutboxStatus.PROCESSING ? NOW.minusSeconds(1) : null);
    event.setLockedBy(status == OutboxStatus.PROCESSING ? "worker" : null);
    event.setLastError("broker unavailable");
    event.setCreatedAt(NOW.minusSeconds(600));
    event.setUpdatedAt(NOW.minusSeconds(Integer.parseInt(id.substring(id.length() - 1))));
    event.setPublishedAt(status == OutboxStatus.PUBLISHED ? NOW.minusSeconds(5) : null);
    return event;
  }

  private static InboxEventEntity inbox(
      String eventId,
      InboxStatus status,
      String eventType,
      Instant receivedAt
  ) {
    InboxEventEntity event = new InboxEventEntity();
    event.setEventId(eventId);
    event.setEventType(eventType);
    event.setEventTimestamp(NOW.minusSeconds(300));
    event.setSource("app");
    event.setCorrelationId("correlation-1");
    event.setPayload("{\"id\":1}");
    event.setContentType("application/json");
    event.setStatus(status);
    event.setAttemptCount(5);
    event.setReceivedAt(receivedAt);
    event.setAvailableAt(status == InboxStatus.RETRY_PENDING ? NOW.minusSeconds(10) : null);
    event.setProcessingAt(status == InboxStatus.PROCESSING ? NOW.minusSeconds(1) : null);
    event.setProcessingBy(status == InboxStatus.PROCESSING ? "worker" : null);
    event.setProcessedAt(status == InboxStatus.PROCESSED ? NOW.minusSeconds(5) : null);
    event.setFailedAt(status == InboxStatus.FAILED ? NOW.minusSeconds(5) : null);
    event.setLastError("handler failed");
    event.setCreatedAt(receivedAt);
    event.setUpdatedAt(receivedAt.plusSeconds(1));
    return event;
  }

  @Configuration(proxyBeanMethods = false)
  @EnableTransactionManagement
  @EnableJpaRepositories(basePackageClasses = OutboxOperationRepository.class)
  static class TestConfiguration {

    @Bean(destroyMethod = "close")
    EntityManagerFactory entityManagerFactory() {
      return Persistence.createEntityManagerFactory("nerv-event-operations-test");
    }

    @Bean
    PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
      return new JpaTransactionManager(entityManagerFactory);
    }

    @Bean
    EntityManager entityManager(EntityManagerFactory entityManagerFactory) {
      return SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory);
    }

    @Bean
    Clock clock() {
      return Clock.fixed(
          NOW,
          ZoneOffset.UTC
      );
    }

    @Bean
    SimpleMeterRegistry meterRegistry() {
      return new SimpleMeterRegistry();
    }

    @Bean
    OutboxOperationService outboxOperations(
        OutboxOperationRepository repository,
        Clock clock,
        SimpleMeterRegistry meterRegistry
    ) {
      return new OutboxOperationServiceImpl(
          repository,
          clock,
          OperationsMetrics.of(meterRegistry)
      );
    }

    @Bean
    InboxOperationService inboxOperations(
        InboxOperationRepository repository,
        Clock clock,
        SimpleMeterRegistry meterRegistry
    ) {
      return new InboxOperationServiceImpl(
          repository,
          clock,
          OperationsMetrics.of(meterRegistry)
      );
    }

    @Bean
    Store store(EntityManager entityManager) {
      return new Store(entityManager);
    }
  }

  static class Store {

    private final EntityManager entityManager;

    Store(EntityManager entityManager) {
      this.entityManager = entityManager;
    }

    @Transactional
    public void save(Object event) {
      entityManager.persist(event);
    }

    @Transactional
    public void clear() {
      entityManager.createQuery("delete from OutboxEventEntity").executeUpdate();
      entityManager.createQuery("delete from InboxEventEntity").executeUpdate();
    }

    @Transactional(readOnly = true)
    public long outboxCount() {
      return entityManager.createQuery(
          "select count(event) from OutboxEventEntity event",
          Long.class
      )
          .getSingleResult();
    }

    @Transactional(readOnly = true)
    public long inboxCount() {
      return entityManager.createQuery(
          "select count(event) from InboxEventEntity event",
          Long.class
      )
          .getSingleResult();
    }
  }
}
