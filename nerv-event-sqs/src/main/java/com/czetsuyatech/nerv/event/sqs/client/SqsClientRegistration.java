package com.czetsuyatech.nerv.event.sqs.client;

import java.util.Objects;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Associates a client ID with an SQS client and records adapter ownership.
 */
public record SqsClientRegistration(
    SqsClientId clientId,
    SqsAsyncClient client,
    boolean owned
)
{
  public SqsClientRegistration {
    Objects.requireNonNull(
        clientId,
        "clientId must not be null"
    );
    Objects.requireNonNull(
        client,
        "client must not be null"
    );
  }

  public static SqsClientRegistration applicationProvided(
      SqsClientId id,
      SqsAsyncClient client
  ) {
    return new SqsClientRegistration(
        id,
        client,
        false
    );
  }

  public static SqsClientRegistration adapterOwned(
      SqsClientId id,
      SqsAsyncClient client
  ) {
    return new SqsClientRegistration(
        id,
        client,
        true
    );
  }
}
