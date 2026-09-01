package com.czetsuyatech.nerv.event.consumer;

/**
 * <p>
 * Continues one {@link EventHandlerInterceptor} invocation toward the resolved {@link EventHandler}.
 * </p>
 *
 * <p>
 * This operation executes synchronously in the current consumer processing call and must be invoked exactly once. The
 * final chain element invokes the resolved handler. Repeated invocation is invalid; implementations must not invoke
 * this chain asynchronously.
 * </p>
 */
@FunctionalInterface
public interface EventHandlerChain {

  void proceed();
}
