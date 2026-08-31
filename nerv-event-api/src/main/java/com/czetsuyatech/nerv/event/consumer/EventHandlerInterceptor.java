package com.czetsuyatech.nerv.event.consumer;

import com.czetsuyatech.nerv.event.model.EventMessage;

/**
 * <p>
 * Broker-neutral extension point for cross-cutting logic around one resolved {@link EventHandler} execution.
 * </p>
 *
 * <p>
 * The consumer invokes interceptors after durable Inbox duplicate detection and claiming, and before its final Inbox
 * outcome handling. Interceptors run in their configured order. An implementation must invoke
 * {@link EventHandlerChain#proceed()} exactly once; returning normally without proceeding is invalid and does not
 * silently skip the event.
 * </p>
 *
 * <p>
 * Interceptor exceptions participate in normal event processing failure and retry classification. Use
 * {@code try/finally} around {@code chain.proceed()} to restore execution context. This extension point is not for
 * event routing or filtering; those remain the responsibility of consumer configuration and handler resolution.
 * </p>
 */
@FunctionalInterface
public interface EventHandlerInterceptor {

  void intercept(
      EventMessage<?> event,
      EventHandlerChain chain
  );
}
