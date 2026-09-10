package com.czetsuyatech.nerv.event.persistence.jpa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducerRegistry;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.outbox.OutboxDispatcher;
import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.core.retry.RetryPolicy;
import com.czetsuyatech.nerv.event.core.routing.DestinationResolver;
import com.czetsuyatech.nerv.event.core.routing.DestinationRoute;
import com.czetsuyatech.nerv.event.core.serialization.EventSerializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import com.czetsuyatech.nerv.event.persistence.application.mapper.OutboxEventMapper;
import com.czetsuyatech.nerv.event.persistence.application.mapper.OutboxEventMapperImpl;
import com.czetsuyatech.nerv.event.persistence.service.impl.JpaPessimisticOutboxClaimStrategy;
import com.czetsuyatech.nerv.event.persistence.service.impl.OutboxClaimStrategy;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.application.dto.JacksonOutboxPayloadCodec;
import com.czetsuyatech.nerv.event.persistence.application.dto.OutboxPayloadCodec;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.service.impl.OutboxServiceImpl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.sql.Clob;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import tools.jackson.databind.ObjectMapper;

class OutboxServiceImplTest {

  private static final Instant NOW = Instant.parse("2026-08-16T12:00:00Z");
  private static final Clock CLOCK = Clock.fixed(
      NOW,
      ZoneOffset.UTC
  );

  private static final OutboxPayloadCodec PAYLOAD_CODEC = new JacksonOutboxPayloadCodec(new ObjectMapper());
  private static AnnotationConfigApplicationContext applicationContext;
  private static EntityManagerFactory entityManagerFactory;
  private static OutboxService workerARepository;
  private static OutboxService workerBRepository;
  private static TransactionalCaller transactionalCaller;

  @BeforeAll
  static void createApplicationContext() {
    applicationContext = new AnnotationConfigApplicationContext(TestConfiguration.class);
    entityManagerFactory = applicationContext.getBean(EntityManagerFactory.class);
    workerARepository = applicationContext.getBean(
        "workerARepository",
        OutboxService.class
    );
    workerBRepository = applicationContext.getBean(
        "workerBRepository",
        OutboxService.class
    );
    transactionalCaller = applicationContext.getBean(TransactionalCaller.class);
  }

  @AfterAll
  static void closeApplicationContext() {
    applicationContext.close();
  }

  @BeforeEach
  void clearOutboxEvents() {
    transactionalCaller.clearOutboxEvents();
  }

  @Test
  void saveWithoutAnExistingTransactionFails() {
    assertThatThrownBy(() -> workerARepository.save(pendingEvent(NOW)))
        .isInstanceOf(IllegalTransactionStateException.class);
  }

  @Test
  void saveParticipatesInTheCallerTransactionAndCommitsWithIt() {
    OutboxEvent event = pendingEvent(NOW);

    transactionalCaller.saveAndCommit(event);

    OutboxEventEntity stored = find(event.id());
    assertThat(stored).isNotNull();
    assertThat(stored.getStatus()).isEqualTo(OutboxStatus.PENDING);
  }

  @Test
  void callerRollbackAlsoRollsBackTheOutboxInsert() {
    OutboxEvent event = pendingEvent(NOW);

    assertThatThrownBy(() -> transactionalCaller.saveAndRollback(event))
        .isInstanceOf(RollbackRequestedException.class);

    assertThat(find(event.id())).isNull();
  }

  @Test
  void normalSaveDelegatesToTheSpringDataRepository() {
    AtomicReference<OutboxEventEntity> savedEntity = new AtomicReference<>();
    OutboxEventRepository recordingRepository = (OutboxEventRepository) java.lang.reflect.Proxy
        .newProxyInstance(
            OutboxEventRepository.class.getClassLoader(),
            new Class<?>[]{OutboxEventRepository.class},
            (
                proxy,
                method,
                arguments) -> {
              if (method.getName().equals("save")) {
                OutboxEventEntity entity = (OutboxEventEntity) arguments[0];
                savedEntity.set(entity);
                return entity;
              }
              throw new UnsupportedOperationException(method.getName());
            }
        );
    OutboxServiceImpl repository = new OutboxServiceImpl(
        recordingRepository,
        mapper(),
        PAYLOAD_CODEC,
        (
            claimedAt,
            batchSize,
            owner,
            leaseDuration) -> List.of(),
        "worker-a",
        Duration.ofMinutes(1),
        CLOCK
    );
    OutboxEvent event = pendingEvent(NOW);

    repository.save(event);

    assertThat(savedEntity.get()).isNotNull();
    assertThat(savedEntity.get().getId()).isEqualTo(event.id().value());
  }

  @Test
  void claimPendingEventMovesItToProcessingAndSetsTheLease() {
    OutboxEvent event = pendingEvent(NOW);
    save(event);

    List<OutboxEvent> claimedEvents = workerARepository.claimPending(
        NOW,
        10
    );

    assertThat(claimedEvents).containsExactly(
        eventWithStatus(
            event,
            OutboxStatus.PROCESSING
        )
    );
    OutboxEventEntity stored = find(event.id());
    assertThat(stored.getStatus()).isEqualTo(OutboxStatus.PROCESSING);
    assertThat(stored.getLockedAt()).isEqualTo(NOW);
    assertThat(stored.getLockedBy()).isEqualTo("worker-a");
    assertThat(stored.getClaimVersion()).isEqualTo(1);
    assertThat(claimedEvents.getFirst().claimVersion()).isEqualTo(1);
  }

  @Test
  void claimSkipsFutureAvailableEvent() {
    OutboxEvent event = pendingEvent(NOW.plusSeconds(1));
    save(event);

    List<OutboxEvent> claimedEvents = workerARepository.claimPending(
        NOW,
        10
    );

    assertThat(claimedEvents).isEmpty();
    assertThat(find(event.id()).getStatus()).isEqualTo(OutboxStatus.PENDING);
  }

  @Test
  void claimReclaimsExpiredProcessingLease() {
    OutboxEvent event = pendingEvent(NOW.minusSeconds(120));
    save(event);
    transactionalCaller.setProcessingLease(
        event.id(),
        NOW.minus(Duration.ofMinutes(2)),
        "crashed-worker"
    );

    List<OutboxEvent> claimedEvents = workerARepository.claimPending(
        NOW,
        10
    );

    assertThat(claimedEvents).hasSize(1);
    assertThat(find(event.id()).getLockedBy()).isEqualTo("worker-a");
    assertThat(find(event.id()).getClaimVersion()).isEqualTo(1);
  }

  @Test
  void claimDoesNotReclaimAnActiveProcessingLease() {
    OutboxEvent event = pendingEvent(NOW.minusSeconds(120));
    save(event);
    transactionalCaller.setProcessingLease(
        event.id(),
        NOW.minusSeconds(30),
        "active-worker"
    );

    List<OutboxEvent> claimedEvents = workerARepository.claimPending(
        NOW,
        10
    );

    assertThat(claimedEvents).isEmpty();
    assertThat(find(event.id()).getLockedBy()).isEqualTo("active-worker");
  }

  @Test
  void staleWorkerCannotMutateAReclaimedEventAndCurrentWorkerCanComplete() {
    OutboxEvent event = pendingEvent(NOW);
    save(event);
    OutboxEvent claimA = workerARepository.claimPending(NOW, 1).getFirst();
    OutboxEvent claimB = workerBRepository.claimPending(NOW.plus(Duration.ofMinutes(1)).plusSeconds(1), 1).getFirst();

    assertThat(claimA.claimVersion()).isEqualTo(1);
    assertThat(claimB.claimVersion()).isEqualTo(2);
    assertThat(
        workerARepository.markPublished(
            claimA.id(),
            claimA.claimVersion(),
            new BrokerPublishResult("stale-ack")
        )
    ).isFalse();
    assertThat(
        workerARepository.reschedule(
            claimA.id(),
            claimA.claimVersion(),
            1,
            NOW.plusSeconds(30),
            "stale retry"
        )
    ).isFalse();
    assertThat(
        workerARepository.markFailed(
            claimA.id(),
            claimA.claimVersion(),
            1,
            "stale failure"
        )
    ).isFalse();

    OutboxEventEntity stillOwnedByB = find(event.id());
    assertThat(stillOwnedByB.getStatus()).isEqualTo(OutboxStatus.PROCESSING);
    assertThat(stillOwnedByB.getLockedBy()).isEqualTo("worker-b");
    assertThat(stillOwnedByB.getClaimVersion()).isEqualTo(2);
    assertThat(stillOwnedByB.getAttemptCount()).isZero();
    assertThat(stillOwnedByB.getLastError()).isNull();

    assertThat(
        workerBRepository.markPublished(
            claimB.id(),
            claimB.claimVersion(),
            new BrokerPublishResult("current-ack")
        )
    ).isTrue();
    assertThat(find(event.id()).getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
  }

  @Test
  void concurrentClaimAttemptsDoNotReturnTheSameEvent() throws Exception {
    OutboxEvent event = pendingEvent(NOW);
    save(event);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);

    Future<List<OutboxEvent>> first = executor.submit(
        () -> claimConcurrently(
            workerARepository,
            ready,
            start
        )
    );
    Future<List<OutboxEvent>> second = executor.submit(
        () -> claimConcurrently(
            workerBRepository,
            ready,
            start
        )
    );
    assertThat(
        ready.await(
            5,
            TimeUnit.SECONDS
        )
    ).isTrue();
    start.countDown();

    List<OutboxEvent> firstClaim = first.get(
        10,
        TimeUnit.SECONDS
    );
    List<OutboxEvent> secondClaim = second.get(
        10,
        TimeUnit.SECONDS
    );
    executor.shutdownNow();

    assertThat(firstClaim.size() + secondClaim.size()).isEqualTo(1);
    assertThat(
        Stream.concat(
            firstClaim.stream(),
            secondClaim.stream()
        )
            .map(OutboxEvent::id)
            .toList()
    ).containsExactly(event.id());
    assertThat(find(event.id()).getLockedBy()).isIn(
        "worker-a",
        "worker-b"
    );
  }

  @Test
  void claimTransactionEndsBeforeDispatcherPublishesToTheBroker() {
    OutboxEvent event = pendingEvent(NOW);
    save(event);
    AtomicBoolean brokerCalled = new AtomicBoolean();
    AtomicBoolean transactionWasActiveAtBrokerCall = new AtomicBoolean();
    BrokerProducer producer = new BrokerProducer() {
      @Override
      public BrokerId brokerId() {
        return new BrokerId("test-broker");
      }

      @Override
      public BrokerPublishResult publish(BrokerMessage message) {
        brokerCalled.set(true);
        transactionWasActiveAtBrokerCall.set(
            TransactionSynchronizationManager.isActualTransactionActive()
        );
        return new BrokerPublishResult("ack-1");
      }
    };
    DestinationResolver destinationResolver = ignored -> new DestinationRoute(
        new BrokerId("test-broker"),
        "orders-topic"
    );
    EventSerializer eventSerializer = ignored -> new SerializedPayload(
        "serialized",
        "application/json"
    );
    OutboxDispatcher dispatcher = new OutboxDispatcher(
        workerARepository,
        destinationResolver,
        eventSerializer,
        new BrokerProducerRegistry(List.of(producer)),
        noRetryPolicy(),
        CLOCK
    );

    dispatcher.dispatch(1);

    assertThat(brokerCalled.get()).isTrue();
    assertThat(transactionWasActiveAtBrokerCall.get()).isFalse();
    assertThat(find(event.id()).getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
  }

  @Test
  void markPublishedUsesAnIndependentPersistenceTransaction() {
    OutboxEvent event = claimedEvent();

    workerARepository.markPublished(
        event.id(),
        event.claimVersion(),
        new BrokerPublishResult("ack-1")
    );

    OutboxEventEntity stored = find(event.id());
    assertThat(stored.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
    assertThat(stored.getPublishedAt()).isEqualTo(NOW);
    assertThat(stored.getLockedAt()).isNull();
    assertThat(stored.getLockedBy()).isNull();
    assertThat(stored.getLastError()).isNull();
  }

  @Test
  void rescheduleUsesAnIndependentPersistenceTransaction() {
    OutboxEvent event = claimedEvent();
    Instant nextAttempt = NOW.plusSeconds(30);
    String failureReason = "x".repeat(OutboxServiceImpl.MAX_LAST_ERROR_LENGTH + 1);

    workerARepository.reschedule(
        event.id(),
        event.claimVersion(),
        2,
        nextAttempt,
        failureReason
    );

    OutboxEventEntity stored = find(event.id());
    assertThat(stored.getStatus()).isEqualTo(OutboxStatus.PENDING);
    assertThat(stored.getAttemptCount()).isEqualTo(2);
    assertThat(stored.getAvailableAt()).isEqualTo(nextAttempt);
    assertThat(stored.getLockedAt()).isNull();
    assertThat(stored.getLockedBy()).isNull();
    assertThat(stored.getLastError()).hasSize(OutboxServiceImpl.MAX_LAST_ERROR_LENGTH);
  }

  @Test
  void markFailedUsesAnIndependentPersistenceTransaction() {
    OutboxEvent event = claimedEvent();

    workerARepository.markFailed(
        event.id(),
        event.claimVersion(),
        3,
        "delivery rejected"
    );

    OutboxEventEntity stored = find(event.id());
    assertThat(stored.getStatus()).isEqualTo(OutboxStatus.FAILED);
    assertThat(stored.getAttemptCount()).isEqualTo(3);
    assertThat(stored.getLockedAt()).isNull();
    assertThat(stored.getLockedBy()).isNull();
    assertThat(stored.getLastError()).isEqualTo("delivery rejected");
  }

  @Test
  void mapsPayloadAndPersistsOnlyTheLogicalDestination() {
    OutboxEvent event = pendingEvent(NOW);
    OutboxEventEntity mapped = mapper().toJpa(
        event
    );

    assertThat(mapped.getDestination()).isEqualTo("orders.created");
    assertThat(mapped.getPayload()).isNull();
    assertThat(
        mapper().toCore(
            mapped,
            mapper().toEventMessage(
                mapped,
                "payload"
            )
        )
    ).isEqualTo(event);

    save(event);
    OutboxEventEntity stored = find(event.id());
    assertThat(stored.getDestination()).isEqualTo("orders.created");
    assertThat(stored.getPayload()).isEqualTo("\"payload\"");
  }

  @Test
  void persistsPayloadAsQueryableReadableJsonText() throws SQLException {
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    payload.put(
        "orderId",
        "123"
    );
    payload.put(
        "customerId",
        "456"
    );
    payload.put(
        "amount",
        new BigDecimal("1200.50")
    );
    OutboxEvent event = pendingEvent(
        NOW,
        payload
    );

    save(event);

    assertThat(rawPayload(event.id()))
        .isEqualTo("{\"orderId\":\"123\",\"customerId\":\"456\",\"amount\":1200.50}");
  }

  private List<OutboxEvent> claimConcurrently(
      OutboxService repository,
      CountDownLatch ready,
      CountDownLatch start
  ) throws InterruptedException {
    ready.countDown();
    if (!start.await(
        5,
        TimeUnit.SECONDS
    )) {
      throw new IllegalStateException("Concurrent claim start timed out");
    }
    return repository.claimPending(
        NOW,
        1
    );
  }

  private OutboxEvent claimedEvent() {
    OutboxEvent event = pendingEvent(NOW);
    save(event);
    return workerARepository.claimPending(
        NOW,
        1
    ).getFirst();
  }

  private void save(OutboxEvent event) {
    transactionalCaller.saveAndCommit(event);
  }

  private OutboxEventEntity find(OutboxId id) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      return entityManager.find(
          OutboxEventEntity.class,
          id.value()
      );
    } finally {
      entityManager.close();
    }
  }

  private String rawPayload(OutboxId id) throws SQLException {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      Object payload = entityManager.createNativeQuery("select payload from nerv_outbox_event where id = :id")
          .setParameter(
              "id",
              id.value()
          )
          .getSingleResult();
      if (payload instanceof Clob clob) {
        return clob.getSubString(
            1,
            Math.toIntExact(clob.length())
        );
      }
      return (String) payload;
    } finally {
      entityManager.close();
    }
  }

  private OutboxEventMapper mapper() {
    return new OutboxEventMapperImpl();
  }

  private OutboxEvent pendingEvent(Instant availableAt) {
    return pendingEvent(
        availableAt,
        "payload"
    );
  }

  private OutboxEvent pendingEvent(
      Instant availableAt,
      Object payload
  ) {
    return new OutboxEvent(
        new OutboxId(UUID.randomUUID().toString()),
        new EventMessage<>(
            new EventId(UUID.randomUUID().toString()),
            "com.example.OrderCreated",
            NOW,
            "orders-service",
            "correlation-1",
            payload
        ),
        new Destination("orders.created"),
        0,
        availableAt,
        OutboxStatus.PENDING
    );
  }

  private OutboxEvent eventWithStatus(
      OutboxEvent event,
      OutboxStatus status
  ) {
    return new OutboxEvent(
        event.id(),
        event.event(),
        event.destination(),
        event.attemptCount(),
        event.nextAttemptAt(),
        status,
        "worker-a",
        1
    );
  }

  private static RetryPolicy noRetryPolicy() {
    return new RetryPolicy() {
      @Override
      public boolean allowsRetry(int failedAttemptCount) {
        return false;
      }

      @Override
      public Instant nextEligibleAt(
          int failedAttemptCount,
          Instant failedAt
      ) {
        throw new AssertionError("Retry should not be requested");
      }
    };
  }

  @Configuration(proxyBeanMethods = false)
  @ComponentScan(basePackageClasses = OutboxEventMapper.class)
  @EnableTransactionManagement
  @EnableJpaRepositories(basePackageClasses = OutboxEventRepository.class)
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
    OutboxPayloadCodec outboxPayloadCodec() {
      return PAYLOAD_CODEC;
    }

    @Bean
    OutboxClaimStrategy outboxClaimStrategy(OutboxEventRepository entityRepository) {
      return new JpaPessimisticOutboxClaimStrategy(entityRepository);
    }

    @Bean("workerARepository")
    OutboxService workerARepository(
        OutboxEventRepository entityRepository,
        OutboxEventMapper mapper,
        OutboxPayloadCodec payloadCodec,
        OutboxClaimStrategy claimStrategy
    ) {
      return new OutboxServiceImpl(
          entityRepository,
          mapper,
          payloadCodec,
          claimStrategy,
          "worker-a",
          Duration.ofMinutes(1),
          CLOCK
      );
    }

    @Bean("workerBRepository")
    OutboxService workerBRepository(
        OutboxEventRepository entityRepository,
        OutboxEventMapper mapper,
        OutboxPayloadCodec payloadCodec,
        OutboxClaimStrategy claimStrategy
    ) {
      return new OutboxServiceImpl(
          entityRepository,
          mapper,
          payloadCodec,
          claimStrategy,
          "worker-b",
          Duration.ofMinutes(1),
          CLOCK
      );
    }

    @Bean
    TransactionalCaller transactionalCaller(
        EntityManager entityManager,
        OutboxService workerARepository
    ) {
      return new TransactionalCaller(
          entityManager,
          workerARepository
      );
    }
  }

  public static class TransactionalCaller {

    private final EntityManager entityManager;
    private final OutboxService outboxService;

    TransactionalCaller(
        EntityManager entityManager,
        OutboxService outboxService
    )
    {
      this.entityManager = entityManager;
      this.outboxService = outboxService;
    }

    @Transactional
    public void clearOutboxEvents() {
      entityManager.createQuery("delete from OutboxEventEntity").executeUpdate();
    }

    @Transactional
    public void saveAndCommit(OutboxEvent event) {
      outboxService.save(event);
    }

    @Transactional
    public void saveAndRollback(OutboxEvent event) {
      outboxService.save(event);
      throw new RollbackRequestedException();
    }

    @Transactional
    public void setProcessingLease(
        OutboxId id,
        Instant lockedAt,
        String lockedBy
    ) {
      OutboxEventEntity entity = entityManager.find(
          OutboxEventEntity.class,
          id.value()
      );
      entity.setStatus(OutboxStatus.PROCESSING);
      entity.setLockedAt(lockedAt);
      entity.setLockedBy(lockedBy);
    }
  }

  private static final class RollbackRequestedException extends RuntimeException {

  }

}
