package com.czetsuyatech.nerv.event.starter;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.InboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.TraceContextEntityRepository;
import com.czetsuyatech.nerv.event.persistence.repository.InboxOperationRepository;
import com.czetsuyatech.nerv.event.persistence.repository.OutboxOperationRepository;
import com.czetsuyatech.nerv.event.publisher.EventPublisher;
import com.czetsuyatech.nerv.event.services.InboxOperationService;
import com.czetsuyatech.nerv.event.services.OutboxOperationService;
import com.czetsuyatech.nerv.event.starter.consumer.Order;
import com.czetsuyatech.nerv.event.starter.consumer.OrderRepository;
import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.boot.autoconfigure.condition.ConditionEvaluationReport;
import org.springframework.boot.data.jpa.autoconfigure.DataJpaRepositoriesAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Exercises the dependency set a normal application receives from the one public starter.
 */
@SpringBootTest(
    classes = com.czetsuyatech.nerv.event.starter.consumer.StarterConsumerApplication.class,
    properties = {
        "spring.datasource.url=jdbc:h2:mem:nerv-starter;DB_CLOSE_DELAY=-1",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "nerv.event.dispatcher.enabled=false",
        "nerv.event.inbox.dispatcher.enabled=false",
        "nerv.event.retention.enabled=false"
    })
class StarterConsumerContextTest {

  private final EventPublisher eventPublisher;
  private final OutboxService outboxService;
  private final InboxService inboxService;
  private final OrderRepository orderRepository;
  private final OutboxEventRepository outboxEntityRepository;
  private final InboxEventRepository inboxEntityRepository;
  private final TraceContextEntityRepository traceContextEntityRepository;
  private final OutboxOperationRepository outboxOperationRepository;
  private final InboxOperationRepository inboxOperationRepository;
  private final OutboxOperationService outboxOperationService;
  private final InboxOperationService inboxOperationService;
  private final EntityManagerFactory entityManagerFactory;
  private final ConfigurableListableBeanFactory beanFactory;

  @Autowired
  StarterConsumerContextTest(
      EventPublisher eventPublisher,
      OutboxService outboxService,
      InboxService inboxService,
      OrderRepository orderRepository,
      OutboxEventRepository outboxEntityRepository,
      InboxEventRepository inboxEntityRepository,
      TraceContextEntityRepository traceContextEntityRepository,
      OutboxOperationRepository outboxOperationRepository,
      InboxOperationRepository inboxOperationRepository,
      OutboxOperationService outboxOperationService,
      InboxOperationService inboxOperationService,
      EntityManagerFactory entityManagerFactory,
      ConfigurableListableBeanFactory beanFactory
  )
  {
    this.eventPublisher = eventPublisher;
    this.outboxService = outboxService;
    this.inboxService = inboxService;
    this.orderRepository = orderRepository;
    this.outboxEntityRepository = outboxEntityRepository;
    this.inboxEntityRepository = inboxEntityRepository;
    this.traceContextEntityRepository = traceContextEntityRepository;
    this.outboxOperationRepository = outboxOperationRepository;
    this.inboxOperationRepository = inboxOperationRepository;
    this.outboxOperationService = outboxOperationService;
    this.inboxOperationService = inboxOperationService;
    this.entityManagerFactory = entityManagerFactory;
    this.beanFactory = beanFactory;
  }

  @Test
  void providesTheBaselineOutboxInboxPublisherAndApplicationRepositoryWithoutConsumerScanning() {
    var repositoryAutoConfigurationOutcomes = ConditionEvaluationReport.get(beanFactory)
        .getConditionAndOutcomesBySource()
        .get(DataJpaRepositoriesAutoConfiguration.class.getName());

    assertThat(repositoryAutoConfigurationOutcomes).isNotNull();
    assertThat(repositoryAutoConfigurationOutcomes.isFullMatch()).isTrue();
    assertThat(eventPublisher).isNotNull();
    assertThat(outboxService).isNotNull();
    assertThat(inboxService).isNotNull();
    assertThat(orderRepository).isNotNull();
    assertThat(outboxEntityRepository).isNotNull();
    assertThat(inboxEntityRepository).isNotNull();
    assertThat(traceContextEntityRepository).isNotNull();
    assertThat(outboxOperationRepository).isNotNull();
    assertThat(inboxOperationRepository).isNotNull();
    assertThat(outboxOperationService).isNotNull();
    assertThat(inboxOperationService).isNotNull();
    assertThat(AutoConfigurationPackages.get(beanFactory))
        .contains(
            "com.czetsuyatech.nerv.event.starter.consumer",
            "com.czetsuyatech.nerv.event.persistence"
        );
    var entityClasses = entityManagerFactory.getMetamodel()
        .getEntities()
        .stream()
        .map(entityType -> (Class<?>) entityType.getJavaType())
        .toList();
    assertThat(entityClasses.contains(Order.class)).isTrue();
    assertThat(entityClasses.contains(OutboxEventEntity.class)).isTrue();
  }
}
