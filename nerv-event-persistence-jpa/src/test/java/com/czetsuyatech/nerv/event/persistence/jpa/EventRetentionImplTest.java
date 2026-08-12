package com.czetsuyatech.nerv.event.persistence.jpa;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
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

class EventRetentionImplTest {

  private static final Instant NOW = Instant.parse("2026-08-18T12:00:00Z");
  private AnnotationConfigApplicationContext context;
  private EventRetentionImpl repository;
  private EntityManager entityManager;
  private TransactionTemplate transactions;

  @BeforeEach
  void setUp() {
    context = new AnnotationConfigApplicationContext(TestConfiguration.class);
    repository = context.getBean(EventRetentionImpl.class);
    entityManager = context.getBean(EntityManager.class);
    transactions = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
  }

  @AfterEach
  void tearDown() {
    context.close();
  }

  @Test
  void deletesOnlyOldPublishedOutboxRowsAndRespectsTheBatchLimit() {
    persistOutbox(
        "old-published-1",
        "event-1",
        OutboxStatus.PUBLISHED,
        NOW.minusSeconds(31),
        NOW
    );
    persistOutbox(
        "old-published-2",
        "event-2",
        OutboxStatus.PUBLISHED,
        NOW.minusSeconds(30),
        NOW
    );
    persistOutbox(
        "recent-published",
        "event-3",
        OutboxStatus.PUBLISHED,
        NOW.minusSeconds(29),
        NOW.minusSeconds(100)
    );
    persistOutbox(
        "pending",
        "event-4",
        OutboxStatus.PENDING,
        NOW.minusSeconds(100),
        NOW.minusSeconds(100)
    );
    persistOutbox(
        "processing",
        "event-5",
        OutboxStatus.PROCESSING,
        NOW.minusSeconds(100),
        NOW.minusSeconds(100)
    );
    persistOutbox(
        "failed",
        "event-6",
        OutboxStatus.FAILED,
        NOW.minusSeconds(100),
        NOW.minusSeconds(100)
    );

    assertThat(
        repository.deletePublishedBefore(
            NOW.minusSeconds(30),
            1
        )
    ).isEqualTo(1);
    assertThat(countOutbox()).isEqualTo(5);
    assertThat(
        repository.deletePublishedBefore(
            NOW.minusSeconds(30),
            10
        )
    ).isEqualTo(1);
    assertThat(countOutbox()).isEqualTo(4);
    assertThat(existsOutbox("recent-published")).isTrue();
    assertThat(existsOutbox("pending")).isTrue();
    assertThat(existsOutbox("processing")).isTrue();
    assertThat(existsOutbox("failed")).isTrue();
  }

  @Test
  void deletesOnlyOldProcessedInboxRowsAndUsesProcessedAtRatherThanCreatedAt() {
    persistInbox(
        "old-processed",
        InboxStatus.PROCESSED,
        NOW.minusSeconds(31),
        NOW
    );
    persistInbox(
        "recent-processed",
        InboxStatus.PROCESSED,
        NOW.minusSeconds(29),
        NOW.minusSeconds(100)
    );
    persistInbox(
        "received",
        InboxStatus.RECEIVED,
        NOW.minusSeconds(100),
        NOW.minusSeconds(100)
    );
    persistInbox(
        "processing",
        InboxStatus.PROCESSING,
        NOW.minusSeconds(100),
        NOW.minusSeconds(100)
    );
    persistInbox(
        "retry-pending",
        InboxStatus.RETRY_PENDING,
        NOW.minusSeconds(100),
        NOW.minusSeconds(100)
    );
    persistInbox(
        "failed",
        InboxStatus.FAILED,
        NOW.minusSeconds(100),
        NOW.minusSeconds(100)
    );

    assertThat(
        repository.deleteProcessedBefore(
            NOW.minusSeconds(30),
            10
        )
    ).isEqualTo(1);
    assertThat(countInbox()).isEqualTo(5);
    assertThat(existsInbox("recent-processed")).isTrue();
    assertThat(existsInbox("received")).isTrue();
    assertThat(existsInbox("processing")).isTrue();
    assertThat(existsInbox("retry-pending")).isTrue();
    assertThat(existsInbox("failed")).isTrue();
  }

  @Test
  void deletesOnlyTraceContextsUnreferencedByBothOutboxAndInbox() {
    persistTrace("outbox-reference");
    persistOutbox(
        "outbox-row",
        "outbox-reference",
        OutboxStatus.PUBLISHED,
        NOW.minusSeconds(100),
        NOW
    );
    persistTrace("inbox-reference");
    persistInbox(
        "inbox-reference",
        InboxStatus.PROCESSED,
        NOW.minusSeconds(100),
        NOW
    );
    persistTrace("orphan-1");
    persistTrace("orphan-2");

    assertThat(repository.deleteUnreferenced(1)).isEqualTo(1);
    assertThat(countTrace()).isEqualTo(3);
    assertThat(repository.deleteUnreferenced(10)).isEqualTo(1);
    assertThat(countTrace()).isEqualTo(2);
    assertThat(existsTrace("outbox-reference")).isTrue();
    assertThat(existsTrace("inbox-reference")).isTrue();
  }

  private void persistOutbox(
      String id,
      String eventId,
      OutboxStatus status,
      Instant publishedAt,
      Instant createdAt
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
      event.setCreatedAt(createdAt);
      event.setUpdatedAt(createdAt);
      event.setPublishedAt(publishedAt);
      entityManager.persist(event);
    });
  }

  private void persistInbox(
      String eventId,
      InboxStatus status,
      Instant processedAt,
      Instant createdAt
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
      event.setReceivedAt(createdAt);
      event.setCreatedAt(createdAt);
      event.setUpdatedAt(createdAt);
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

  private long countOutbox() {
    return count("select count(event) from OutboxEventEntity event");
  }

  private long countInbox() {
    return count("select count(event) from InboxEventEntity event");
  }

  private long countTrace() {
    return count("select count(context) from TraceContextEntity context");
  }

  private long count(String query) {
    return entityManager.createQuery(
        query,
        Long.class
    ).getSingleResult();
  }

  private boolean existsOutbox(String id) {
    return entityManager.find(
        OutboxEventEntity.class,
        id
    ) != null;
  }

  private boolean existsInbox(String id) {
    return entityManager.find(
        InboxEventEntity.class,
        id
    ) != null;
  }

  private boolean existsTrace(String id) {
    return entityManager.find(
        TraceContextEntity.class,
        id
    ) != null;
  }

  @Configuration(proxyBeanMethods = false)
  @EnableTransactionManagement(proxyTargetClass = true)
  @EnableJpaRepositories(basePackageClasses = OutboxEventRepository.class)
  static class TestConfiguration {
    @Bean(destroyMethod = "close")
    EntityManagerFactory entityManagerFactory() {
      return Persistence.createEntityManagerFactory("nerv-event-test");
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
