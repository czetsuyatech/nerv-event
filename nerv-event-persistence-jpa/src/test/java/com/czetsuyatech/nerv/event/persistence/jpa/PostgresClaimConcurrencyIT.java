package com.czetsuyatech.nerv.event.persistence.jpa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.exception.EventStateTransitionException;
import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.persistence.application.mapper.InboxEventMapper;
import com.czetsuyatech.nerv.event.persistence.application.mapper.OutboxEventMapper;
import com.czetsuyatech.nerv.event.persistence.service.impl.JpaPessimisticOutboxClaimStrategy;
import com.czetsuyatech.nerv.event.persistence.service.impl.OutboxClaimStrategy;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.application.dto.JacksonOutboxPayloadCodec;
import com.czetsuyatech.nerv.event.persistence.application.dto.OutboxPayloadCodec;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.InboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.service.InboxRegistrationWriter;
import com.czetsuyatech.nerv.event.persistence.service.impl.InboxServiceImpl;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.service.impl.OutboxServiceImpl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
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
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

/**
 * <p>
 * PostgreSQL correctness tests for short, independent claim transactions.
 * </p>
 *
 * <p>
 * The suite uses PostgreSQL 16 with its default READ COMMITTED isolation. Claims use Hibernate {@code
 * PESSIMISTIC_WRITE} ({@code SELECT ... FOR UPDATE}); it deliberately makes no H2 concurrency claim and does not rely
 * on {@code SKIP LOCKED}. Each repository bean represents an independent pod owner and every concurrent action is
 * released through a latch.
 * </p>
 */
@Testcontainers(disabledWithoutDocker = true)
class PostgresClaimConcurrencyIT {

  private static final Instant NOW = Instant.parse("2026-08-18T12:00:00Z");
  private static final Duration LEASE = Duration.ofMinutes(1);
  @Container
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
      DockerImageName.parse("postgres:16-alpine")
  ).withDatabaseName("nerv_event").withUsername("nerv").withPassword("nerv");

  private AnnotationConfigApplicationContext context;
  private TransactionTemplate transactions;
  private EntityManager entityManager;
  private OutboxService outboxA;
  private OutboxService outboxB;
  private OutboxService outboxC;
  private OutboxService outboxD;
  private InboxService inboxA;
  private InboxService inboxB;
  private InboxService inboxC;
  private InboxService inboxD;

  @BeforeEach
  void setUp() {
    context = new AnnotationConfigApplicationContext(TestConfiguration.class);
    transactions = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
    entityManager = context.getBean(EntityManager.class);
    outboxA = context.getBean(
        "outboxPodA",
        OutboxService.class
    );
    outboxB = context.getBean(
        "outboxPodB",
        OutboxService.class
    );
    outboxC = context.getBean(
        "outboxPodC",
        OutboxService.class
    );
    outboxD = context.getBean(
        "outboxPodD",
        OutboxService.class
    );
    inboxA = context.getBean(
        "inboxPodA",
        InboxService.class
    );
    inboxB = context.getBean(
        "inboxPodB",
        InboxService.class
    );
    inboxC = context.getBean(
        "inboxPodC",
        InboxService.class
    );
    inboxD = context.getBean(
        "inboxPodD",
        InboxService.class
    );
  }

  @AfterEach
  void tearDown() {
    context.close();
  }

  @Test
  @Timeout(30)
  void outboxConcurrentClaimsAssignOneActiveOwnerToEveryRowAcrossRepeatedRaces() throws Exception {
    for (int iteration = 0; iteration < 3; iteration++) {
      clear();
      List<String> ids = new ArrayList<>();
      for (int index = 0; index < 100; index++) {
        ids.add(
            persistOutbox(
                OutboxStatus.PENDING,
                NOW,
                null,
                null
            )
        );
      }

      List<List<OutboxEvent>> claims = concurrently(
          List.of(
              () -> outboxA.claimPending(
                  NOW,
                  100
              ),
              () -> outboxB.claimPending(
                  NOW,
                  100
              ),
              () -> outboxC.claimPending(
                  NOW,
                  100
              )
          )
      );

      List<String> claimedIds = claims.stream()
          .flatMap(List::stream)
          .map(event -> event.id().value())
          .toList();
      assertThat(claimedIds).hasSize(100).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(ids);
      assertThat(ids.stream().map(this::outbox).map(OutboxEventEntity::getStatus))
          .containsOnly(OutboxStatus.PROCESSING);
      assertThat(ids.stream().map(this::outbox).map(OutboxEventEntity::getLockedBy))
          .allMatch(
              owner -> List.of(
                  "pod-a",
                  "pod-b",
                  "pod-c"
              ).contains(owner)
          );
    }
  }

  @Test
  @Timeout(30)
  void outboxMultiBatchClaimsAccountForAllRowsAndDistributeWork() throws Exception {
    List<String> ids = new ArrayList<>();
    for (int index = 0; index < 250; index++) {
      ids.add(
          persistOutbox(
              OutboxStatus.PENDING,
              NOW,
              null,
              null
          )
      );
    }
    List<OutboxService> workers = List.of(
        outboxA,
        outboxB,
        outboxC,
        outboxD
    );
    Map<String, Integer> workByOwner = new HashMap<>();
    List<String> claimedIds = new ArrayList<>();

    for (int round = 0; round < 3; round++) {
      List<List<OutboxEvent>> claims = concurrently(
          workers.stream()
              .map(
                  worker -> (Supplier<List<OutboxEvent>>) () -> worker.claimPending(
                      NOW,
                      25
                  )
              )
              .toList()
      );
      for (int worker = 0; worker < claims.size(); worker++) {
        String owner = switch (worker) {
          case 0 -> "pod-a";
          case 1 -> "pod-b";
          case 2 -> "pod-c";
          default -> "pod-d";
        };
        workByOwner.merge(
            owner,
            claims.get(worker).size(),
            Integer::sum
        );
        claims.get(worker).forEach(event -> claimedIds.add(event.id().value()));
      }
    }

    assertThat(claimedIds).hasSize(250).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(ids);
    assertThat(workByOwner).containsKeys(
        "pod-a",
        "pod-b",
        "pod-c",
        "pod-d"
    );
    assertThat(workByOwner).allSatisfy(
        (
            owner,
            count) -> assertThat(count).isPositive()
    );
  }

  @Test
  void outboxLeaseRecoveryAndOwnerGuardsUsePostgresStatePredicates() {
    String active = persistOutbox(
        OutboxStatus.PROCESSING,
        NOW.minusSeconds(10),
        "pod-a",
        NOW.minusSeconds(10)
    );
    String expired = persistOutbox(
        OutboxStatus.PROCESSING,
        NOW.minus(LEASE).minusSeconds(1),
        "pod-a",
        NOW.minus(LEASE).minusSeconds(1)
    );

    assertThat(
        outboxB.claimPending(
            NOW,
            10
        ).stream().map(event -> event.id().value())
    )
        .containsExactly(expired);
    assertThat(outbox(active).getLockedBy()).isEqualTo("pod-a");
    assertThat(outbox(expired).getLockedBy()).isEqualTo("pod-b");
    assertThat(outbox(expired).getLockedAt()).isEqualTo(NOW);

    assertThat(
        outboxB.markPublished(
            new OutboxId(active),
            0,
            new BrokerPublishResult("ack")
        )
    ).isFalse();
    assertThat(
        outboxB.reschedule(
            new OutboxId(active),
            0,
            1,
            NOW.plusSeconds(30),
            "failed"
        )
    ).isFalse();
    assertThat(
        outboxB.markFailed(
            new OutboxId(active),
            0,
            1,
            "failed"
        )
    ).isFalse();
    assertThat(
        outboxA.markPublished(
            new OutboxId(active),
            1,
            new BrokerPublishResult("wrong-version")
        )
    ).isFalse();
    assertThat(
        outboxA.markPublished(
            new OutboxId(active),
            0,
            new BrokerPublishResult("ack")
        )
    ).isTrue();
    assertThat(outbox(active).getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
  }

  @Test
  void outboxCrashAfterPublishCanBeReclaimedAndRepublishedAtLeastOnce() {
    String id = persistOutbox(
        OutboxStatus.PENDING,
        NOW,
        null,
        null
    );
    OutboxEvent claimA = outboxA.claimPending(
        NOW,
        1
    ).getFirst(); // broker publish succeeds; no markPublished follows.
    assertThat(claimA.claimVersion()).isEqualTo(1);

    OutboxEvent claimB = outboxB.claimPending(
        NOW.plus(LEASE).plusSeconds(1),
        1
    ).getFirst(); // a second broker publication remains possible under at-least-once delivery.
    assertThat(claimB.id().value()).isEqualTo(id);
    assertThat(claimB.claimVersion()).isEqualTo(2);
    assertThat(outbox(id).getStatus()).isEqualTo(OutboxStatus.PROCESSING);
    assertThat(outbox(id).getLockedBy()).isEqualTo("pod-b");

    assertThat(
        outboxA.markPublished(
            claimA.id(),
            claimA.claimVersion(),
            new BrokerPublishResult("stale-ack")
        )
    ).isFalse();
    assertThat(
        outboxA.reschedule(
            claimA.id(),
            claimA.claimVersion(),
            1,
            NOW.plusSeconds(30),
            "stale retry"
        )
    ).isFalse();
    assertThat(
        outboxA.markFailed(
            claimA.id(),
            claimA.claimVersion(),
            1,
            "stale failure"
        )
    ).isFalse();

    OutboxEventEntity stillOwnedByB = outbox(id);
    assertThat(stillOwnedByB.getStatus()).isEqualTo(OutboxStatus.PROCESSING);
    assertThat(stillOwnedByB.getLockedBy()).isEqualTo("pod-b");
    assertThat(stillOwnedByB.getLockedAt()).isEqualTo(NOW.plus(LEASE).plusSeconds(1));
    assertThat(stillOwnedByB.getClaimVersion()).isEqualTo(2);
    assertThat(stillOwnedByB.getAttemptCount()).isZero();
    assertThat(stillOwnedByB.getLastError()).isNull();

    assertThat(
        outboxB.markPublished(
            claimB.id(),
            claimB.claimVersion(),
            new BrokerPublishResult("current-ack")
        )
    ).isTrue();
    assertThat(outbox(id).getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
  }

  @Test
  @Timeout(30)
  void inboxRegistrationAndClaimsAreSafeAcrossPods() throws Exception {
    InboxEvent received = inboxEvent(
        "duplicate-event",
        InboxStatus.RECEIVED,
        null,
        null
    );
    List<InboxRegistration> registrations = concurrently(
        List.of(
            () -> inboxA.register(received),
            () -> inboxB.register(received),
            () -> inboxC.register(received)
        )
    );
    assertThat(registrations).filteredOn(InboxRegistration::created).hasSize(1);
    assertThat(countInbox()).isEqualTo(1);

    List<java.util.Optional<InboxEvent>> claims = concurrently(
        List.of(
            () -> inboxA.claim(
                received.eventId(),
                NOW,
                "pod-a",
                LEASE
            ),
            () -> inboxB.claim(
                received.eventId(),
                NOW,
                "pod-b",
                LEASE
            )
        )
    );
    assertThat(claims.stream().filter(java.util.Optional::isPresent)).hasSize(1);
    assertThat(inbox(received.eventId().value()).getStatus()).isEqualTo(InboxStatus.PROCESSING);
    assertThat(inbox(received.eventId().value()).getProcessingBy()).isIn(
        "pod-a",
        "pod-b"
    );
  }

  @Test
  void inboxLeaseRecoveryOwnerGuardsAndRetryEligibilityAreStateSafe() {
    InboxEvent active = inboxEvent(
        "active",
        InboxStatus.PROCESSING,
        NOW.minusSeconds(10),
        "pod-a"
    );
    InboxEvent expired = inboxEvent(
        "expired",
        InboxStatus.PROCESSING,
        NOW.minus(LEASE).minusSeconds(1),
        "pod-a"
    );
    persistInbox(active);
    persistInbox(expired);

    assertThat(
        inboxB.claim(
            active.eventId(),
            NOW,
            "pod-b",
            LEASE
        )
    ).isEmpty();
    assertThat(
        inboxB.claim(
            expired.eventId(),
            NOW,
            "pod-b",
            LEASE
        )
    ).isPresent();
    assertThat(inbox(expired.eventId().value()).getProcessingBy()).isEqualTo("pod-b");
    assertThat(inbox(expired.eventId().value()).getProcessingAt()).isEqualTo(NOW);

    assertThatThrownBy(
        () -> inboxB.markProcessed(
            active.eventId(),
            "pod-b",
            NOW
        )
    )
        .isInstanceOf(EventStateTransitionException.class);
    assertThatThrownBy(
        () -> inboxB.markRetryPending(
            active.eventId(),
            "pod-b",
            1,
            NOW,
            NOW,
            "failed"
        )
    )
        .isInstanceOf(EventStateTransitionException.class);
    assertThatThrownBy(
        () -> inboxB.markFailed(
            active.eventId(),
            "pod-b",
            1,
            NOW,
            "failed"
        )
    )
        .isInstanceOf(EventStateTransitionException.class);
    inboxA.markProcessed(
        active.eventId(),
        "pod-a",
        NOW
    );

    InboxEvent due = inboxEvent(
        "due",
        InboxStatus.RETRY_PENDING,
        null,
        null
    );
    InboxEvent future = inboxEvent(
        "future",
        InboxStatus.RETRY_PENDING,
        null,
        null,
        NOW.plusSeconds(1)
    );
    InboxEvent failed = inboxEvent(
        "failed",
        InboxStatus.FAILED,
        null,
        null
    );
    persistInbox(due);
    persistInbox(future);
    persistInbox(failed);
    assertThat(
        inboxC.claimPendingRetries(
            10,
            NOW,
            "pod-c",
            LEASE
        )
    ).extracting(event -> event.eventId().value())
        .containsExactly("due");
    assertThat(inbox("future").getStatus()).isEqualTo(InboxStatus.RETRY_PENDING);
    assertThat(inbox("failed").getStatus()).isEqualTo(InboxStatus.FAILED);
  }

  @Test
  @Timeout(30)
  void retryPendingRowsAreAssignedToOnlyOneConcurrentWorker() throws Exception {
    for (int index = 0; index < 100; index++) {
      persistInbox(
          inboxEvent(
              "retry-" + index,
              InboxStatus.RETRY_PENDING,
              null,
              null
          )
      );
    }
    List<List<InboxEvent>> claims = concurrently(
        List.of(
            () -> inboxA.claimPendingRetries(
                100,
                NOW,
                "pod-a",
                LEASE
            ),
            () -> inboxB.claimPendingRetries(
                100,
                NOW,
                "pod-b",
                LEASE
            ),
            () -> inboxC.claimPendingRetries(
                100,
                NOW,
                "pod-c",
                LEASE
            )
        )
    );
    List<String> ids = claims.stream().flatMap(List::stream).map(event -> event.eventId().value()).toList();
    assertThat(ids).hasSize(100).doesNotHaveDuplicates();
    assertThat(ids.stream().map(this::inbox).map(InboxEventEntity::getStatus)).containsOnly(InboxStatus.PROCESSING);
  }

  /**
   * <p>
   * Moderate, Docker-backed resilience tier. It intentionally tests persistence recovery semantics rather than broker
   * throughput: broker adapters have their own Kafka and LocalStack suites.
   * </p>
   */
  @Test
  @Tag("resilience")
  @Timeout(180)
  void thousandRecordOutboxAndInboxBacklogsDrainWithBoundedMultiPodClaims() throws Exception {
    persistOutboxBacklog(1_000);
    ConcurrentHashMap<String, String> outboxOwners = new ConcurrentHashMap<>();
    List<Integer> outboxCompleted = concurrently(
        List.of(
            () -> drainOutbox(
                outboxA,
                "pod-a",
                outboxOwners
            ),
            () -> drainOutbox(
                outboxB,
                "pod-b",
                outboxOwners
            ),
            () -> drainOutbox(
                outboxC,
                "pod-c",
                outboxOwners
            ),
            () -> drainOutbox(
                outboxD,
                "pod-d",
                outboxOwners
            )
        )
    );
    assertThat(outboxCompleted.stream().mapToInt(Integer::intValue).sum()).isEqualTo(1_000);
    assertThat(outboxOwners).hasSize(1_000);
    assertThat(countOutbox(OutboxStatus.PUBLISHED)).isEqualTo(1_000);
    assertThat(countOutbox(OutboxStatus.PENDING)).isZero();
    assertThat(countOutbox(OutboxStatus.PROCESSING)).isZero();

    clear();
    persistInboxRetryBacklog(1_000);
    ConcurrentHashMap<String, String> inboxOwners = new ConcurrentHashMap<>();
    List<Integer> inboxCompleted = concurrently(
        List.of(
            () -> drainInboxRetries(
                inboxA,
                "pod-a",
                inboxOwners
            ),
            () -> drainInboxRetries(
                inboxB,
                "pod-b",
                inboxOwners
            ),
            () -> drainInboxRetries(
                inboxC,
                "pod-c",
                inboxOwners
            ),
            () -> drainInboxRetries(
                inboxD,
                "pod-d",
                inboxOwners
            )
        )
    );
    assertThat(inboxCompleted.stream().mapToInt(Integer::intValue).sum()).isEqualTo(1_000);
    assertThat(inboxOwners).hasSize(1_000);
    assertThat(countInbox(InboxStatus.PROCESSED)).isEqualTo(1_000);
    assertThat(countInbox(InboxStatus.RETRY_PENDING)).isZero();
    assertThat(countInbox(InboxStatus.PROCESSING)).isZero();
  }

  private int drainOutbox(
      OutboxService repository,
      String owner,
      ConcurrentHashMap<String, String> owners
  ) {
    int completed = 0;
    while (true) {
      List<OutboxEvent> claimed = repository.claimPending(
          NOW,
          50
      );
      if (claimed.isEmpty()) {
        return completed;
      }
      for (OutboxEvent event : claimed) {
        String previousOwner = owners.putIfAbsent(
            event.id().value(),
            owner
        );
        if (previousOwner != null) {
          throw new AssertionError(
              "outbox event claimed twice id=" + event.id().value()
                  + " firstOwner=" + previousOwner + " owner=" + owner
          );
        }
        repository.markPublished(
            event.id(),
            event.claimVersion(),
            new BrokerPublishResult("resilience-ack")
        );
        completed++;
      }
    }
  }

  private int drainInboxRetries(
      InboxService repository,
      String owner,
      ConcurrentHashMap<String, String> owners
  ) {
    int completed = 0;
    while (true) {
      List<InboxEvent> claimed = repository.claimPendingRetries(
          50,
          NOW,
          owner,
          LEASE
      );
      if (claimed.isEmpty()) {
        return completed;
      }
      for (InboxEvent event : claimed) {
        String previousOwner = owners.putIfAbsent(
            event.eventId().value(),
            owner
        );
        if (previousOwner != null) {
          throw new AssertionError(
              "inbox event claimed twice eventId=" + event.eventId().value()
                  + " firstOwner=" + previousOwner + " owner=" + owner
          );
        }
        repository.markProcessed(
            event.eventId(),
            owner,
            NOW
        );
        completed++;
      }
    }
  }

  private <T> List<T> concurrently(List<Supplier<T>> actions) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(actions.size());
    CountDownLatch ready = new CountDownLatch(actions.size());
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<T>> results = new ArrayList<>();
      for (Supplier<T> action : actions) {
        results.add(
            executor.submit(
                releaseTogether(
                    action,
                    ready,
                    start
                )
            )
        );
      }
      assertThat(
          ready.await(
              5,
              TimeUnit.SECONDS
          )
      ).isTrue();
      start.countDown();
      List<T> values = new ArrayList<>();
      for (Future<T> result : results) {
        values.add(
            result.get(
                100,
                TimeUnit.SECONDS
            )
        );
      }
      return values;
    } finally {
      executor.shutdownNow();
    }
  }

  private static <T> Callable<T> releaseTogether(
      Supplier<T> action,
      CountDownLatch ready,
      CountDownLatch start
  ) {
    return () -> {
      ready.countDown();
      if (!start.await(
          5,
          TimeUnit.SECONDS
      )) {
        throw new IllegalStateException("concurrent PostgreSQL workers were not released");
      }
      return action.get();
    };
  }

  private void clear() {
    transactions.executeWithoutResult(ignored -> {
      entityManager.createQuery("delete from TraceContextEntity").executeUpdate();
      entityManager.createQuery("delete from OutboxEventEntity").executeUpdate();
      entityManager.createQuery("delete from InboxEventEntity").executeUpdate();
    });
  }

  private String persistOutbox(
      OutboxStatus status,
      Instant availableAt,
      String lockedBy,
      Instant lockedAt
  ) {
    String id = UUID.randomUUID().toString();
    transactions.executeWithoutResult(ignored -> {
      OutboxEventEntity event = new OutboxEventEntity();
      event.setId(id);
      event.setEventId("event-" + id);
      event.setEventType("example.Event");
      event.setSource("test");
      event.setEventTimestamp(NOW);
      event.setDestination("test.destination");
      event.setPayload("{\"payload\":true}");
      event.setStatus(status);
      event.setAttemptCount(0);
      event.setAvailableAt(availableAt);
      event.setLockedBy(lockedBy);
      event.setLockedAt(lockedAt);
      event.setCreatedAt(NOW.minusSeconds(60));
      event.setUpdatedAt(NOW.minusSeconds(60));
      entityManager.persist(event);
    });
    return id;
  }

  private void persistOutboxBacklog(int count) {
    transactions.executeWithoutResult(ignored -> {
      for (int index = 0; index < count; index++) {
        OutboxEventEntity event = new OutboxEventEntity();
        String id = "resilience-outbox-" + index;
        event.setId(id);
        event.setEventId("resilience-event-" + index);
        event.setEventType("example.ResilienceEvent");
        event.setSource("resilience-test");
        event.setEventTimestamp(NOW);
        event.setDestination("resilience.destination");
        event.setPayload("{\"kind\":\"resilience\"}");
        event.setStatus(OutboxStatus.PENDING);
        event.setAttemptCount(0);
        event.setAvailableAt(NOW);
        event.setCreatedAt(NOW.minusSeconds(60));
        event.setUpdatedAt(NOW.minusSeconds(60));
        entityManager.persist(event);
      }
      entityManager.flush();
    });
  }

  private void persistInbox(InboxEvent inboxEvent) {
    transactions.executeWithoutResult(
        ignored -> entityManager.persist(
            context.getBean(InboxEventMapper.class).toJpa(inboxEvent)
        )
    );
  }

  private void persistInboxRetryBacklog(int count) {
    InboxEventMapper mapper = context.getBean(InboxEventMapper.class);
    transactions.executeWithoutResult(ignored -> {
      for (int index = 0; index < count; index++) {
        entityManager.persist(
            mapper.toJpa(
                inboxEvent(
                    "resilience-inbox-" + index,
                    InboxStatus.RETRY_PENDING,
                    null,
                    null
                )
            )
        );
      }
      entityManager.flush();
    });
  }

  private InboxEvent inboxEvent(
      String id,
      InboxStatus status,
      Instant processingAt,
      String processingBy
  ) {
    return inboxEvent(
        id,
        status,
        processingAt,
        processingBy,
        status == InboxStatus.RETRY_PENDING ? NOW : null
    );
  }

  private InboxEvent inboxEvent(
      String id,
      InboxStatus status,
      Instant processingAt,
      String processingBy,
      Instant availableAt
  ) {
    return InboxEvent.builder()
        .eventId(new EventId(id))
        .eventType("example.Event")
        .timestamp(NOW)
        .source("test")
        .payload(
            new SerializedPayload(
                "{\"payload\":true}",
                "application/json"
            )
        )
        .status(status)
        .attemptCount(0)
        .receivedAt(NOW.minusSeconds(60))
        .availableAt(availableAt)
        .processingAt(processingAt)
        .processingBy(processingBy)
        .build();
  }

  private OutboxEventEntity outbox(String id) {
    return transactions.execute(
        ignored -> entityManager.find(
            OutboxEventEntity.class,
            id
        )
    );
  }

  private InboxEventEntity inbox(String id) {
    return transactions.execute(
        ignored -> entityManager.find(
            InboxEventEntity.class,
            id
        )
    );
  }

  private long countInbox() {
    return transactions.execute(
        ignored -> entityManager.createQuery(
            "select count(event) from InboxEventEntity event",
            Long.class
        ).getSingleResult()
    );
  }

  private long countInbox(InboxStatus status) {
    return transactions.execute(
        ignored -> entityManager.createQuery(
            "select count(event) from InboxEventEntity event where event.status = :status",
            Long.class
        )
            .setParameter(
                "status",
                status
            )
            .getSingleResult()
    );
  }

  private long countOutbox(OutboxStatus status) {
    return transactions.execute(
        ignored -> entityManager.createQuery(
            "select count(event) from OutboxEventEntity event where event.status = :status",
            Long.class
        )
            .setParameter(
                "status",
                status
            )
            .getSingleResult()
    );
  }

  @Configuration(proxyBeanMethods = false)
  @ComponentScan(basePackageClasses = OutboxEventMapper.class)
  @EnableJpaRepositories(basePackageClasses = OutboxEventRepository.class)
  @EnableTransactionManagement(proxyTargetClass = true)
  static class TestConfiguration {
    @Bean(destroyMethod = "close")
    EntityManagerFactory entityManagerFactory() {
      Map<String, Object> properties = Map.of(
          "jakarta.persistence.jdbc.driver",
          "org.postgresql.Driver",
          "jakarta.persistence.jdbc.url",
          POSTGRES.getJdbcUrl(),
          "jakarta.persistence.jdbc.user",
          POSTGRES.getUsername(),
          "jakarta.persistence.jdbc.password",
          POSTGRES.getPassword(),
          "hibernate.hbm2ddl.auto",
          "create-drop",
          "hibernate.connection.isolation",
          "2"
      );
      return Persistence.createEntityManagerFactory(
          "nerv-event-test",
          properties
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
    OutboxPayloadCodec outboxPayloadCodec() {
      return new JacksonOutboxPayloadCodec(new ObjectMapper());
    }

    @Bean
    OutboxClaimStrategy outboxClaimStrategy(OutboxEventRepository entityRepository) {
      return new JpaPessimisticOutboxClaimStrategy(entityRepository);
    }

    @Bean("outboxPodA")
    OutboxService outboxPodA(
        OutboxEventRepository entities,
        OutboxEventMapper mapper,
        OutboxPayloadCodec codec,
        OutboxClaimStrategy claimStrategy
    ) {
      return outbox(
          entities,
          mapper,
          codec,
          claimStrategy,
          "pod-a"
      );
    }

    @Bean("outboxPodB")
    OutboxService outboxPodB(
        OutboxEventRepository entities,
        OutboxEventMapper mapper,
        OutboxPayloadCodec codec,
        OutboxClaimStrategy claimStrategy
    ) {
      return outbox(
          entities,
          mapper,
          codec,
          claimStrategy,
          "pod-b"
      );
    }

    @Bean("outboxPodC")
    OutboxService outboxPodC(
        OutboxEventRepository entities,
        OutboxEventMapper mapper,
        OutboxPayloadCodec codec,
        OutboxClaimStrategy claimStrategy
    ) {
      return outbox(
          entities,
          mapper,
          codec,
          claimStrategy,
          "pod-c"
      );
    }

    @Bean("outboxPodD")
    OutboxService outboxPodD(
        OutboxEventRepository entities,
        OutboxEventMapper mapper,
        OutboxPayloadCodec codec,
        OutboxClaimStrategy claimStrategy
    ) {
      return outbox(
          entities,
          mapper,
          codec,
          claimStrategy,
          "pod-d"
      );
    }

    private OutboxService outbox(
        OutboxEventRepository entities,
        OutboxEventMapper mapper,
        OutboxPayloadCodec codec,
        OutboxClaimStrategy strategy,
        String owner
    ) {
      return new OutboxServiceImpl(
          entities,
          mapper,
          codec,
          strategy,
          owner,
          LEASE,
          Clock.fixed(
              NOW,
              ZoneOffset.UTC
          )
      );
    }

    @Bean
    InboxRegistrationWriter inboxRegistrationWriter(InboxEventRepository entityRepository) {
      return new InboxRegistrationWriter(entityRepository);
    }

    @Bean("inboxPodA")
    InboxService inboxPodA(
        InboxEventRepository entities,
        InboxRegistrationWriter writer,
        InboxEventMapper mapper
    ) {
      return new InboxServiceImpl(
          entities,
          writer,
          mapper
      );
    }

    @Bean("inboxPodB")
    InboxService inboxPodB(
        InboxEventRepository entities,
        InboxRegistrationWriter writer,
        InboxEventMapper mapper
    ) {
      return new InboxServiceImpl(
          entities,
          writer,
          mapper
      );
    }

    @Bean("inboxPodC")
    InboxService inboxPodC(
        InboxEventRepository entities,
        InboxRegistrationWriter writer,
        InboxEventMapper mapper
    ) {
      return new InboxServiceImpl(
          entities,
          writer,
          mapper
      );
    }

    @Bean("inboxPodD")
    InboxService inboxPodD(
        InboxEventRepository entities,
        InboxRegistrationWriter writer,
        InboxEventMapper mapper
    ) {
      return new InboxServiceImpl(
          entities,
          writer,
          mapper
      );
    }
  }
}
