package com.czetsuyatech.nerv.event.operations.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.services.InboxOperationService;
import com.czetsuyatech.nerv.event.services.OutboxOperationService;
import com.czetsuyatech.nerv.event.autoconfigure.NervEventOperationsAutoConfiguration;
import com.czetsuyatech.nerv.event.web.controller.InboxOperationController;
import com.czetsuyatech.nerv.event.web.controller.OutboxOperationController;
import com.czetsuyatech.nerv.event.autoconfigure.NervEventOperationWebAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class NervEventOperationWebAutoConfigurationTest {

  private final WebApplicationContextRunner contextRunner = new WebApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(NervEventOperationWebAutoConfiguration.class))
      .withUserConfiguration(OperationsConfiguration.class);

  @Test
  void runsAfterTheOperationsAutoConfiguration() {
    assertThat(NervEventOperationWebAutoConfiguration.class.getAnnotation(AutoConfiguration.class).after())
        .contains(NervEventOperationsAutoConfiguration.class);
  }

  @Test
  void endpointsDoNotExistUnlessExplicitlyEnabled() {
    contextRunner.run(context -> {
      assertThat(context).doesNotHaveBean(OutboxOperationController.class);
      assertThat(context).doesNotHaveBean(InboxOperationController.class);
    });
  }

  @Test
  void eachAvailableOperationsCapabilityGetsOnlyItsOwnController() {
    contextRunner.withPropertyValues("nerv.event.operations.web.enabled=true").run(context -> {
      assertThat(context).hasSingleBean(OutboxOperationController.class);
      assertThat(context).hasSingleBean(InboxOperationController.class);
    });
  }

  @Configuration(proxyBeanMethods = false)
  static class OperationsConfiguration {
    @Bean
    OutboxOperationService outboxOperations() {
      return org.mockito.Mockito.mock(OutboxOperationService.class);
    }

    @Bean
    InboxOperationService inboxOperations() {
      return org.mockito.Mockito.mock(InboxOperationService.class);
    }
  }
}
