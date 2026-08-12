package com.czetsuyatech.nerv.event.autoconfigure;

import com.czetsuyatech.nerv.event.services.InboxOperationService;
import com.czetsuyatech.nerv.event.services.OutboxOperationService;
import com.czetsuyatech.nerv.event.web.advice.OperationWebExceptionHandler;
import com.czetsuyatech.nerv.event.web.controller.InboxOperationController;
import com.czetsuyatech.nerv.event.web.controller.OutboxOperationController;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.DispatcherServlet;

/**
 * Registers the MVC operations endpoints only after explicit opt-in.
 */
@AutoConfiguration(after = NervEventOperationsAutoConfiguration.class)
@ConditionalOnClass(DispatcherServlet.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "nerv.event.operations.web", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(NervEventOperationWebProperties.class)
public class NervEventOperationWebAutoConfiguration {

  @Bean
  @ConditionalOnBean(OutboxOperationService.class)
  OutboxOperationController outboxOperationsController(
      OutboxOperationService operations,
      NervEventOperationWebProperties properties
  ) {
    return new OutboxOperationController(
        operations,
        properties
    );
  }

  @Bean
  @ConditionalOnBean(InboxOperationService.class)
  InboxOperationController inboxOperationsController(
      InboxOperationService operations,
      NervEventOperationWebProperties properties
  ) {
    return new InboxOperationController(
        operations,
        properties
    );
  }

  @Bean
  OperationWebExceptionHandler operationsWebExceptionHandler() {
    return new OperationWebExceptionHandler();
  }
}
