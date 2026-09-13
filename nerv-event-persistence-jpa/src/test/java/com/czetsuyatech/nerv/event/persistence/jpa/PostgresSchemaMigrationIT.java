package com.czetsuyatech.nerv.event.persistence.jpa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.springframework.data.jpa.repository.support.JpaRepositoryFactory;

/**
 * PostgreSQL contract test: canonical SQL first, then Hibernate validation and durable-row use.
 */
@Testcontainers(disabledWithoutDocker = true)
class PostgresSchemaMigrationIT {

  private static final Instant NOW = Instant.parse("2026-08-19T00:00:00Z");
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
      DockerImageName.parse("postgres:16-alpine")
  ).withDatabaseName("nerv_event").withUsername("nerv").withPassword("nerv");

  private static EntityManagerFactory entityManagerFactory;

  @BeforeAll
  static void migrateAndValidateJpaModel() throws Exception {
    executeCanonicalMigrations();
    entityManagerFactory = Persistence.createEntityManagerFactory(
        "nerv-event-test",
        validationProperties()
    );
  }

  @AfterAll
  static void closeEntityManagerFactory() {
    if (entityManagerFactory != null) {
      entityManagerFactory.close();
    }
  }

  @BeforeEach
  void clearRows() {
    inTransaction(entityManager -> {
      entityManager.createQuery("delete from TraceContextEntity").executeUpdate();
      entityManager.createQuery("delete from OutboxEventEntity").executeUpdate();
      entityManager.createQuery("delete from InboxEventEntity").executeUpdate();
    });
  }

  @Test
  void migrationCreatedSchemaPersistsReadablePayloadsAndLifecycleColumns() {
    String payload = "{\"message\":\"Mabuhay \\uD83C\\uDF0F\"}".repeat(1_000);
    inTransaction(entityManager -> {
      OutboxEventEntity outbox = outbox(
          "outbox-1",
          "event-1",
          payload
      );
      entityManager.persist(outbox);
      InboxEventEntity inbox = inbox(
          "event-1",
          payload
      );
      entityManager.persist(inbox);
      entityManager.persist(
          new TraceContextEntity(
              "event-1",
              "{\"traceparent\":\"00-abc\"}",
              NOW
          )
      );
    });

    inTransaction(entityManager -> {
      OutboxEventEntity outbox = entityManager.find(
          OutboxEventEntity.class,
          "outbox-1"
      );
      outbox.setStatus(OutboxStatus.PROCESSING);
      outbox.setLockedAt(NOW);
      outbox.setLockedBy("pod-a");
      outbox.setUpdatedAt(NOW);
    });
    inTransaction(entityManager -> {
      OutboxEventEntity outbox = entityManager.find(
          OutboxEventEntity.class,
          "outbox-1"
      );
      outbox.setStatus(OutboxStatus.PUBLISHED);
      outbox.setPublishedAt(NOW);
      outbox.setLockedAt(null);
      outbox.setLockedBy(null);
      outbox.setUpdatedAt(NOW);
      InboxEventEntity inbox = entityManager.find(
          InboxEventEntity.class,
          "event-1"
      );
      inbox.setStatus(InboxStatus.PROCESSING);
      inbox.setProcessingAt(NOW);
      inbox.setProcessingBy("pod-a");
      inbox.setStatus(InboxStatus.RETRY_PENDING);
      inbox.setAttemptCount(1);
      inbox.setAvailableAt(NOW);
      inbox.setFailedAt(NOW);
      inbox.setProcessingAt(null);
      inbox.setProcessingBy(null);
      inbox.setStatus(InboxStatus.PROCESSED);
      inbox.setProcessedAt(NOW);
      inbox.setAvailableAt(null);
      inbox.setUpdatedAt(NOW);
    });

    inTransaction(entityManager -> {
      assertThat(
          entityManager.find(
              OutboxEventEntity.class,
              "outbox-1"
          ).getPayload()
      ).isEqualTo(payload);
      assertThat(
          entityManager.find(
              OutboxEventEntity.class,
              "outbox-1"
          ).getStatus()
      ).isEqualTo(OutboxStatus.PUBLISHED);
      assertThat(
          entityManager.find(
              InboxEventEntity.class,
              "event-1"
          ).getStatus()
      ).isEqualTo(InboxStatus.PROCESSED);
      assertThat(
          entityManager.find(
              InboxEventEntity.class,
              "event-1"
          ).getContentType()
      ).isEqualTo("application/json");
      assertThat(
          entityManager.find(
              TraceContextEntity.class,
              "event-1"
          ).getContextJson()
      ).contains("traceparent");
    });
    inTransaction(entityManager -> {
      EventRetentionImpl retention = retentionRepository(entityManager);
      assertThat(
          retention.deletePublishedBefore(
              NOW,
              10
          )
      ).isEqualTo(1);
      assertThat(
          retention.deleteProcessedBefore(
              NOW,
              10
          )
      ).isEqualTo(1);
      assertThat(retention.deleteUnreferenced(10)).isEqualTo(1);
    });
    inTransaction(entityManager -> {
      assertThat(
          entityManager.find(
              OutboxEventEntity.class,
              "outbox-1"
          )
      ).isNull();
      assertThat(
          entityManager.find(
              InboxEventEntity.class,
              "event-1"
          )
      ).isNull();
      assertThat(
          entityManager.find(
              TraceContextEntity.class,
              "event-1"
          )
      ).isNull();
    });
  }

  @Test
  void databaseEnforcesIdentityRequiredColumnsAndNonnegativeCounters() throws Exception {
    inTransaction(entityManager -> {
      entityManager.persist(
          inbox(
              "duplicate-event",
              "{}"
          )
      );
      entityManager.persist(
          new TraceContextEntity(
              "duplicate-event",
              "{}",
              NOW
          )
      );
    });

    assertThatThrownBy(
        () -> inTransaction(
            entityManager -> entityManager.persist(
                inbox(
                    "duplicate-event",
                    "{}"
                )
            )
        )
    )
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(
        () -> inTransaction(
            entityManager -> entityManager.persist(
                new TraceContextEntity(
                    "duplicate-event",
                    "{}",
                    NOW
                )
            )
        )
    )
        .isInstanceOf(RuntimeException.class);

    try (Connection connection = connection(); Statement statement = connection.createStatement()) {
      assertThatThrownBy(
          () -> statement.executeUpdate("insert into nerv_inbox_event (event_id) values ('missing-columns')")
      )
          .isInstanceOf(SQLException.class);
      assertThatThrownBy(
          () -> statement
              .executeUpdate("update nerv_inbox_event set attempt_count = -1 where event_id = 'duplicate-event'")
      )
          .isInstanceOf(SQLException.class);
    }
  }

  @Test
  void migrationCreatesExpectedNamedIndexes() throws Exception {
    Set<String> indexNames = new HashSet<>();
    try (Connection connection = connection();
        ResultSet indexes = connection.getMetaData()
            .getIndexInfo(
                null,
                null,
                "nerv_outbox_event",
                false,
                false
            )) {
      while (indexes.next()) {
        indexNames.add(indexes.getString("INDEX_NAME"));
      }
    }
    assertThat(indexNames).contains(
        "idx_nerv_outbox_status_available",
        "idx_nerv_outbox_status_locked",
        "idx_nerv_outbox_status_published",
        "idx_nerv_outbox_status_updated",
        "idx_nerv_outbox_event_id",
        "idx_nerv_outbox_ordering_sequence"
    );
  }

  private static void executeCanonicalMigrations() throws Exception {
    try (Connection connection = connection()) {
      for (String migration : SchemaMigrationResourcesTest.MIGRATIONS) {
        String resource = "META-INF/nerv-event/db/postgresql/migration/" + migration;
        try (InputStream stream = PostgresSchemaMigrationIT.class.getClassLoader().getResourceAsStream(resource)) {
          if (stream == null) {
            throw new IllegalStateException("Missing migration resource " + resource);
          }
          String sql = new String(
              stream.readAllBytes(),
              StandardCharsets.UTF_8
          );
          for (String statement : sql.split(";")) {
            if (!statement.isBlank()) {
              try (Statement jdbcStatement = connection.createStatement()) {
                jdbcStatement.execute(statement);
              }
            }
          }
        }
      }
    }
  }

  private static Map<String, Object> validationProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(
        "jakarta.persistence.jdbc.driver",
        "org.postgresql.Driver"
    );
    properties.put(
        "jakarta.persistence.jdbc.url",
        POSTGRES.getJdbcUrl()
    );
    properties.put(
        "jakarta.persistence.jdbc.user",
        POSTGRES.getUsername()
    );
    properties.put(
        "jakarta.persistence.jdbc.password",
        POSTGRES.getPassword()
    );
    properties.put(
        "hibernate.hbm2ddl.auto",
        "validate"
    );
    return properties;
  }

  private static Connection connection() throws SQLException {
    return DriverManager.getConnection(
        POSTGRES.getJdbcUrl(),
        POSTGRES.getUsername(),
        POSTGRES.getPassword()
    );
  }

  private void inTransaction(java.util.function.Consumer<EntityManager> work) {
    EntityManager entityManager = entityManagerFactory.createEntityManager();
    try {
      entityManager.getTransaction().begin();
      work.accept(entityManager);
      entityManager.getTransaction().commit();
    } catch (RuntimeException exception) {
      if (entityManager.getTransaction().isActive()) {
        entityManager.getTransaction().rollback();
      }
      throw exception;
    } finally {
      entityManager.close();
    }
  }

  private static EventRetentionImpl retentionRepository(EntityManager entityManager) {
    JpaRepositoryFactory repositoryFactory = new JpaRepositoryFactory(entityManager);
    return new EventRetentionImpl(
        repositoryFactory.getRepository(OutboxEventRepository.class),
        repositoryFactory.getRepository(InboxEventRepository.class),
        repositoryFactory.getRepository(TraceContextEntityRepository.class)
    );
  }

  private static OutboxEventEntity outbox(
      String id,
      String eventId,
      String payload
  ) {
    OutboxEventEntity event = new OutboxEventEntity();
    event.setId(id);
    event.setEventId(eventId);
    event.setEventType("example.Event");
    event.setSource("migration-test");
    event.setEventTimestamp(NOW);
    event.setDestination("migration.destination");
    event.setPayload(payload);
    event.setStatus(OutboxStatus.PENDING);
    event.setAttemptCount(0);
    event.setAvailableAt(NOW);
    event.setCreatedAt(NOW);
    event.setUpdatedAt(NOW);
    return event;
  }

  private static InboxEventEntity inbox(
      String eventId,
      String payload
  ) {
    InboxEventEntity event = new InboxEventEntity();
    event.setEventId(eventId);
    event.setEventType("example.Event");
    event.setEventTimestamp(NOW);
    event.setSource("migration-test");
    event.setPayload(payload);
    event.setContentType("application/json");
    event.setStatus(InboxStatus.RECEIVED);
    event.setAttemptCount(0);
    event.setReceivedAt(NOW);
    event.setCreatedAt(NOW);
    event.setUpdatedAt(NOW);
    return event;
  }
}
