package com.czetsuyatech.nerv.event.observability.tracing;

import java.util.Map;

/**
 * Internal thread-scoped carrier for broker integrations; never contains event payload data.
 */
public final class TraceContextCarrier {

  private static final ThreadLocal<Map<String, String>> CURRENT = new ThreadLocal<>();

  private TraceContextCarrier() {
  }

  public static Map<String, String> current() {
    Map<String, String> fields = CURRENT.get();
    return fields == null ? Map.of() : fields;
  }

  public static Scope open(Map<String, String> fields) {
    Map<String, String> previous = CURRENT.get();
    CURRENT.set(Map.copyOf(fields));
    return () -> {
      if (previous == null) {
        CURRENT.remove();
      } else {
        CURRENT.set(previous);
      }
    };
  }

  public interface Scope extends AutoCloseable {

    @Override
    void close();
  }
}
