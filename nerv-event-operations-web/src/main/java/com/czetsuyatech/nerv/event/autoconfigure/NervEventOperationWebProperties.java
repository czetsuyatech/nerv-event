package com.czetsuyatech.nerv.event.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the optional operations HTTP surface.
 */
@ConfigurationProperties("nerv.event.operations.web")
public class NervEventOperationWebProperties {

  private String basePath = "/management/nerv-event";
  private final Payload payload = new Payload();

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    if (basePath == null || basePath.isBlank() || !basePath.startsWith("/")) {
      throw new IllegalArgumentException("nerv.event.operations.web.base-path must start with '/'");
    }

    this.basePath = basePath.endsWith("/") && basePath.length() > 1
        ? basePath.substring(0, basePath.length() - 1)
        : basePath;
  }

  public Payload getPayload() {
    return payload;
  }

  public static class Payload {

    private boolean enabled;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }
  }
}
