package com.czetsuyatech.nerv.event.observability.autoconfigure;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties("nerv.event.observability")
public class NervEventObservabilityProperties {

  private boolean enabled = true;
  private Metrics metrics = new Metrics();
  private Tracing tracing = new Tracing();
  private Health health = new Health();

  @Getter
  @Setter
  public static class Metrics {

    private boolean enabled = true;
  }

  @Getter
  @Setter
  public static class Tracing {

    private boolean enabled = true;
  }

  @Getter
  @Setter
  public static class Health {

    private boolean enabled = true;
    private Outbox outbox = new Outbox();
    private Inbox inbox = new Inbox();

    @Getter
    @Setter
    public static class Outbox {

      private Long maxPending;
      private Duration maxOldestPendingAge;
    }

    @Getter
    @Setter
    public static class Inbox {

      private Long maxRetryPending;
      private Duration maxOldestRetryAge;
    }
  }
}
