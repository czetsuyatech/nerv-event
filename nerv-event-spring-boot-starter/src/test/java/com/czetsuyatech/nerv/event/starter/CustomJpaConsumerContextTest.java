package com.czetsuyatech.nerv.event.starter;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.InboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import com.czetsuyatech.nerv.event.starter.customconsumer.CustomOrder;
import com.czetsuyatech.nerv.event.starter.customconsumer.CustomOrderRepository;
import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Verifies that explicit consumer repository configuration coexists with NERV's narrow scan.
 */
@SpringBootTest(
    classes = com.czetsuyatech.nerv.event.starter.customconsumer.CustomJpaConsumerApplication.class,
    properties = {
        "spring.datasource.url=jdbc:h2:mem:nerv-custom-jpa;DB_CLOSE_DELAY=-1",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "nerv.event.outbox.enabled=false",
        "nerv.event.dispatcher.enabled=false",
        "nerv.event.inbox.dispatcher.enabled=false",
        "nerv.event.retention.enabled=false"
    })
class CustomJpaConsumerContextTest {

  private final CustomOrderRepository customOrderRepository;
  private final OutboxEventRepository outboxEntityRepository;
  private final InboxEventRepository inboxEntityRepository;
  private final EntityManagerFactory entityManagerFactory;

  @Autowired
  CustomJpaConsumerContextTest(
      CustomOrderRepository customOrderRepository,
      OutboxEventRepository outboxEntityRepository,
      InboxEventRepository inboxEntityRepository,
      EntityManagerFactory entityManagerFactory
  )
  {
    this.customOrderRepository = customOrderRepository;
    this.outboxEntityRepository = outboxEntityRepository;
    this.inboxEntityRepository = inboxEntityRepository;
    this.entityManagerFactory = entityManagerFactory;
  }

  @Test
  void retainsApplicationAndNervRepositoriesWhenTheApplicationConfiguresItsOwnScan() {
    assertThat(customOrderRepository).isNotNull();
    assertThat(outboxEntityRepository).isNotNull();
    assertThat(inboxEntityRepository).isNotNull();
    var entityClasses = entityManagerFactory.getMetamodel()
        .getEntities()
        .stream()
        .map(entityType -> (Class<?>) entityType.getJavaType())
        .toList();
    assertThat(entityClasses.contains(CustomOrder.class)).isTrue();
    assertThat(entityClasses.contains(OutboxEventEntity.class)).isTrue();
  }
}
