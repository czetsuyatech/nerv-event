package com.czetsuyatech.nerv.event.sqs.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.sqs.client.SqsClientId;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SqsDestinationResolverTest {
  @Test
  void resolvesClientAndQueueWithoutFallback() {
    SqsDestination expected = new SqsDestination(
        new SqsClientId("account-a"),
        "orders-prod"
    );
    SqsDestinationResolver resolver = new SqsDestinationResolver(
        Map.of(
            "orders",
            expected
        )
    );
    assertThat(resolver.resolve("orders")).isEqualTo(expected);
    assertThatThrownBy(() -> resolver.resolve("missing"))
        .hasMessage("No SQS destination configured for target 'missing'");
  }

  @Test
  void allowsFifoProducerDestinations() {
    SqsDestination destination = new SqsDestination(
        new SqsClientId("account-a"),
        "orders.fifo"
    );

    assertThat(destination.queue()).isEqualTo("orders.fifo");
  }
}
