package com.czetsuyatech.nerv.event.persistence.autoconfigure;

import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import com.czetsuyatech.nerv.event.core.retention.EventRetention;
import com.czetsuyatech.nerv.event.persistence.application.mapper.InboxEventMapper;
import com.czetsuyatech.nerv.event.persistence.application.mapper.OutboxEventMapper;
import com.czetsuyatech.nerv.event.persistence.persistence.config.NervEventJpaRepositoriesConfiguration;
import com.czetsuyatech.nerv.event.persistence.service.InboxRegistrationWriter;
import com.czetsuyatech.nerv.event.persistence.service.impl.EventRetentionImpl;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.TraceContextEntityRepository;
import com.czetsuyatech.nerv.event.persistence.service.impl.JpaPessimisticOutboxClaimStrategy;
import com.czetsuyatech.nerv.event.persistence.service.impl.OutboxClaimStrategy;
import com.czetsuyatech.nerv.event.persistence.application.dto.JacksonOutboxPayloadCodec;
import com.czetsuyatech.nerv.event.persistence.application.dto.OutboxPayloadCodec;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.InboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.service.impl.InboxServiceImpl;
import com.czetsuyatech.nerv.event.persistence.service.impl.OutboxServiceImpl;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventAutoConfiguration;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import com.czetsuyatech.nerv.event.spring.dispatcher.DispatcherOwnerResolver;
import jakarta.persistence.EntityManagerFactory;
import java.time.Clock;
import org.mapstruct.factory.Mappers;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.jpa.autoconfigure.DataJpaRepositoriesAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import tools.jackson.databind.ObjectMapper;

/**
 * Registers the JPA outbox and inbox adapters without requiring component scanning.
 */
@AutoConfiguration(
    before = NervEventAutoConfiguration.class,
    after = {
        HibernateJpaAutoConfiguration.class,
        DataJpaRepositoriesAutoConfiguration.class
    })
@ConditionalOnClass(EntityManagerFactory.class)
@Import(NervEventJpaRepositoriesConfiguration.class)
public class NervEventJpaAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(OutboxEventMapper.class)
  OutboxEventMapper jpaOutboxEventMapper() {
    return Mappers.getMapper(OutboxEventMapper.class);
  }

  @Bean
  @ConditionalOnMissingBean(InboxEventMapper.class)
  InboxEventMapper jpaInboxEventMapper() {
    return Mappers.getMapper(InboxEventMapper.class);
  }

  @Bean
  @ConditionalOnMissingBean(OutboxPayloadCodec.class)
  OutboxPayloadCodec outboxPayloadCodec(ObjectMapper objectMapper) {
    return new JacksonOutboxPayloadCodec(objectMapper);
  }

  @Bean
  @ConditionalOnMissingBean(OutboxClaimStrategy.class)
  OutboxClaimStrategy outboxClaimStrategy(OutboxEventRepository entityRepository) {
    return new JpaPessimisticOutboxClaimStrategy(entityRepository);
  }

  @Bean
  @ConditionalOnMissingBean(InboxRegistrationWriter.class)
  InboxRegistrationWriter inboxRegistrationWriter(InboxEventRepository entityRepository) {
    return new InboxRegistrationWriter(entityRepository);
  }

  @Bean
  @ConditionalOnMissingBean(OutboxService.class)
  OutboxServiceImpl outboxRepository(
      OutboxEventRepository entityRepository,
      OutboxEventMapper mapper,
      OutboxPayloadCodec payloadCodec,
      OutboxClaimStrategy claimStrategy,
      NervEventProperties properties,
      Clock clock,
      Environment environment
  ) {
    return new OutboxServiceImpl(
        entityRepository,
        mapper,
        payloadCodec,
        claimStrategy,
        DispatcherOwnerResolver.resolve(
            properties.getDispatcher(),
            environment
        ),
        properties.getDispatcher().getLeaseDuration(),
        clock
    );
  }

  @Bean
  @ConditionalOnMissingBean(InboxService.class)
  InboxServiceImpl inboxRepository(
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
  @ConditionalOnMissingBean(EventRetention.class)
  EventRetentionImpl eventRetentionRepository(
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
