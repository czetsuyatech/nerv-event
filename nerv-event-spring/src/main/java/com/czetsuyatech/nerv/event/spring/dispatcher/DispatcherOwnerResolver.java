package com.czetsuyatech.nerv.event.spring.dispatcher;

import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import java.util.UUID;
import org.springframework.core.env.Environment;

/**
 * Resolves one readable dispatcher owner identifier for the application lifetime.
 */
public final class DispatcherOwnerResolver {

  private DispatcherOwnerResolver() {
  }

  public static String resolve(
      NervEventProperties.Dispatcher properties,
      Environment environment
  ) {
    return resolve(
        properties.getOwner(),
        environment
    );
  }

  public static String resolve(
      String configuredOwner,
      Environment environment
  ) {
    if (configuredOwner != null) {
      return configuredOwner;
    }
    String podName = environment.getProperty("POD_NAME");
    if (podName == null || podName.isBlank()) {
      podName = environment.getProperty("HOSTNAME");
    }
    if (podName != null && !podName.isBlank()) {
      return podName;
    }
    String applicationName = environment.getProperty("spring.application.name");
    if (applicationName != null && !applicationName.isBlank()) {
      return applicationName + "-" + randomSuffix();
    }
    return "nerv-event-" + randomSuffix();
  }

  private static String randomSuffix() {
    return UUID.randomUUID()
        .toString()
        .substring(
            0,
            8
        );
  }
}
