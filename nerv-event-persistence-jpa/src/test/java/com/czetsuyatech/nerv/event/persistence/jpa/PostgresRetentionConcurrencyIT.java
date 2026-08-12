package com.czetsuyatech.nerv.event.persistence.jpa;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.core.retention.EventRetention;
import com.czetsuyatech.nerv.event.core.retention.RetentionSidecarCleaner;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.TraceContextEntity;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.InboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.TraceContextEntityRepository;
import com.czetsuyatech.nerv.event.persistence.service.impl.EventRetentionImpl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * <p>
 * PostgreSQL-only verification for concurrent retention workers. Failsafe executes this {@code *IT} class only when
 * Maven's {@code integration-tests} profile is active.
 * </p>
 */
@Testcontainers(disabledWithoutDocker = true)
class PostgresRetentionConcurrencyIT {

  private static final Instant NOW = Instant.parse("2026-08-18T12:00:00Z");
  private static final Instant CUTOFF = NOW.minusSeconds(30);
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
      DockerImageName.parse("postgres:16-alpine")
  ).withDatabaseName("nerv_event").withUsername("nerv").withPassword("nerv");

  private AnnotationConfigApplicationContext context;
  private EventRetention retentionRepository;
  private RetentionSidecarCleaner sidecarCleaner;
  private EntityManager entityManager;
  private TransactionTemplate transactions;

  @BeforeEach
  void setUp() {
    context = new AnnotationConfigApplicationContext(TestConfiguration.class);
    retentionRepository = context.getBean(EventRetention.class);
    sidecarCleaner = context.getBean(RetentionSidecarCleaner.class);
    entityManager = context.getBean(EntityManager.class);
    transactions = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
  }

  @AfterEach
  void tearDown() {
    context.close();
  }

  @Test
  void concurrentWorkersDeleteEachOldPublishedRowOnceAndPreserveUnresolvedOutboxRows()
      throws Exception {
    for (int index = 0; index < 100; index++) {
      persistOutbox(
          "published-" + index,
          "published-event-" + index,
          OutboxStatus.PUBLISHED,
          CUTOFF
      );
    }
    persistOutbox(
        "processing",
        "processing-event",
        OutboxStatus.PROCESSING,
        CUTOFF
    );
    persistOutbox(
        "failed",
        "failed-event",
        OutboxStatus.FAILED,
        CUTOFF
    );

    int deleted = concurrently(
        () -> retentionRepository.deletePublishedBefore(
            CUTOFF,
            100
        )
    );

    assertThat(deleted).isEqualTo(100);
    assertThat(
        count(
            "select count(event) from OutboxEventEntity event where event.status = :status",
            OutboxStatus.PUBLISHED
        )
    )
        .isZero();
    assertThat(
        exists(
            OutboxEventEntity.class,
            "processing"
        )
    ).isTrue();
    assertThat(
        exists(
            OutboxEventEntity.class,
            "failed"
        )
    ).isTrue();
  }

  @Test
  void concurrentWorkersDeleteEachOldProcessedRowAndOrphanTraceContextOnce() throws Exception {
    for (int index = 0; index < 100; index++) {
      String eventId = "processed-event-" + index;
      persistInbox(
          eventId,
          InboxStatus.PROCESSED,
          CUTOFF
      );
      persistTrace(eventId);
    }
    persistInbox(
        "retry-pending-event",
        InboxStatus.RETRY_PENDING,
        CUTOFF
    );
    persistTrace("retry-pending-event");

    int inboxDeleted = concurrently(
        () -> retentionRepository.deleteProcessedBefore(
            CUTOFF,
            100
        )
    );
    int traceDeleted = concurrently(() -> sidecarCleaner.deleteUnreferenced(100));

    assertThat(inboxDeleted).isEqualTo(100);
    assertThat(traceDeleted).isEqualTo(100);
    assertThat(
        count(
            "select count(event) from InboxEventEntity event where event.status = :status",
            InboxStatus.PROCESSED
        )
    )
        .isZero();
    assertThat(
        exists(
            InboxEventEntity.class,
            "retry-pending-event"
        )
    ).isTrue();
    assertThat(
        exists(
            TraceContextEntity.class,
            "retry-pending-event"
        )
    ).isTrue();
  }

  private int concurrently(ThrowingIntSupplier worker) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<Integer> first = executor.submit(
          () -> invokeWhenReady(
              worker,
              ready,
              start
          )
      );
      Future<Integer> second = executor.submit(
          () -> invokeWhenReady(
              worker,
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
      return first.get(
          15,
          TimeUnit.SECONDS
      ) + second.get(15,
          TimeUnit.SECONDS
      );
    } finally {
      executor.shutdownNow();
    }
  }

  private static int invokeWhenReady(
      ThrowingIntSupplier worker,
      CountDownLatch ready,
      CountDownLatch start
  ) throws Exception {
    ready.countDown();
    if (!start.await(
        5,
        TimeUnit.SECONDS
    )) {
      throw new IllegalStateException("concurrent retention workers were not released");
    }
    return worker.getAsInt();
  }

  private void persistOutbox(
      String id,
      String eventId,
      OutboxStatus status,
      Instant publishedAt
  ) {
    transactions.executeWithoutResult(ignored -> {
      OutboxEventEntity event = new OutboxEventEntity();
      event.setId(id);
      event.setEventId(eventId);
      event.setEventType("example.Event");
      event.setSource("test");
      event.setEventTimestamp(NOW);
      event.setDestination("test.destination");
      event.setPayload("{\"payload\":true}");
      event.setStatus(status);
      event.setAttemptCount(0);
      event.setAvailableAt(NOW);
      event.setCreatedAt(NOW.minusSeconds(60));
      event.setUpdatedAt(NOW.minusSeconds(60));
      event.setPublishedAt(publishedAt);
      entityManager.persist(event);
    });
  }

  private void persistInbox(
      String eventId,
      InboxStatus status,
      Instant processedAt
  ) {
    transactions.executeWithoutResult(ignored -> {
      InboxEventEntity event = new InboxEventEntity();
      event.setEventId(eventId);
      event.setEventType("example.Event");
      event.setEventTimestamp(NOW);
      event.setSource("test");
      event.setPayload("{\"payload\":true}");
      event.setContentType("application/json");
      event.setStatus(status);
      event.setAttemptCount(0);
      event.setReceivedAt(NOW.minusSeconds(60));
      event.setCreatedAt(NOW.minusSeconds(60));
      event.setUpdatedAt(NOW.minusSeconds(60));
      event.setProcessedAt(processedAt);
      entityManager.persist(event);
    });
  }

  private void persistTrace(String eventId) {
    transactions.executeWithoutResult(
        ignored -> entityManager.persist(
            new TraceContextEntity(
                eventId,
                "{\"traceparent\":\"00-test\"}",
                NOW
            )
        )
    );
  }

  private long count(
      String query,
      Object status
  ) {
    return entityManager.createQuery(
        query,
        Long.class
    )
        .setParameter(
            "status",
            status
        )
        .getSingleResult();
  }

  private boolean exists(
      Class<?> entityClass,
      String id
  ) {
    return entityManager.find(
        entityClass,
        id
    ) != null;
  }

  @FunctionalInterface
  private interface ThrowingIntSupplier {
    int getAsInt() throws Exception;
  }

  @Configuration(proxyBeanMethods = false)
  @EnableTransactionManagement(proxyTargetClass = true)
  @EnableJpaRepositories(basePackageClasses = OutboxEventRepository.class)
  static class TestConfiguration {
    @Bean(destroyMethod = "close")
    EntityManagerFactory entityManagerFactory() {
      Map<String, Object> settings = new HashMap<>();
      settings.put(
          "jakarta.persistence.jdbc.driver",
          "org.postgresql.Driver"
      );
      settings.put(
          "jakarta.persistence.jdbc.url",
          POSTGRES.getJdbcUrl()
      );
      settings.put(
          "jakarta.persistence.jdbc.user",
          POSTGRES.getUsername()
      );
      settings.put(
          "jakarta.persistence.jdbc.password",
          POSTGRES.getPassword()
      );
      settings.put(
          "hibernate.hbm2ddl.auto",
          "create-drop"
      );
      return Persistence.createEntityManagerFactory(
          "nerv-event-test",
          settings
      );
    }

    @Bean
    PlatformTransactionManager transactionManager(EntityManagerFactory factory) {
      return new JpaTransactionManager(factory);
    }

    @Bean
    @Primary
    EntityManager entityManager(EntityManagerFactory factory) {
      return SharedEntityManagerCreator.createSharedEntityManager(factory);
    }

    @Bean
    EventRetentionImpl retentionRepository(
        OutboxEventRepository outboxRepository,
        InboxEventRepository inboxRepository,
        TraceContextEntityRepository traceContextRepository
    ) {
      return new EventRetentionImpl(
          outboxRepository,
          inboxRepository,
          traceContextRepository
      );
    }
  }
}
