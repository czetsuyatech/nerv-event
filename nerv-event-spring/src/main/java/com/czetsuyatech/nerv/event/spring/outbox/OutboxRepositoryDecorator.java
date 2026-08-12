package com.czetsuyatech.nerv.event.spring.outbox;

import com.czetsuyatech.nerv.event.core.outbox.OutboxService;

/**
 * Optional transparent decoration applied only to integrations created by Nerv Event Spring.
 */
public interface OutboxRepositoryDecorator {
  OutboxService decorate(OutboxService repository);
}
