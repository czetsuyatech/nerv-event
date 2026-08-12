package com.czetsuyatech.nerv.event.autoconfigure;

import com.czetsuyatech.nerv.event.persistence.repository.InboxOperationRepository;
import com.czetsuyatech.nerv.event.persistence.repository.OutboxOperationRepository;
import com.czetsuyatech.nerv.event.persistence.repository.OperationsMetrics;
import com.czetsuyatech.nerv.event.services.InboxOperationService;
import com.czetsuyatech.nerv.event.services.OutboxOperationService;
import com.czetsuyatech.nerv.event.services.impl.InboxOperationServiceImpl;
import com.czetsuyatech.nerv.event.services.impl.OutboxOperationServiceImpl;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.EntityManagerFactory;
import java.time.Clock;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Optional JPA-backed operational API configuration.
 */
@AutoConfiguration(
    afterName = {
        "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration",
        "org.springframework.boot.data.jpa.autoconfigure.DataJpaRepositoriesAutoConfiguration"
    })
@ConditionalOnBean(EntityManagerFactory.class)
public class NervEventOperationsAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(OutboxOperationService.class)
  OutboxOperationService outboxOperations(
      OutboxOperationRepository repository,
      Clock clock,
      ObjectProvider<MeterRegistry> meterRegistry
  ) {
    return new OutboxOperationServiceImpl(
        repository,
        clock,
        OperationsMetrics.of(meterRegistry)
    );
  }

  @Bean
  @ConditionalOnMissingBean(InboxOperationService.class)
  InboxOperationService inboxOperations(
      InboxOperationRepository repository,
      Clock clock,
      ObjectProvider<MeterRegistry> meterRegistry
  ) {
    return new InboxOperationServiceImpl(
        repository,
        clock,
        OperationsMetrics.of(meterRegistry)
    );
  }
}
