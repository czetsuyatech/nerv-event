package com.czetsuyatech.nerv.event.sqs.client;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Immutable registry of reusable SQS clients.
 */
public final class SqsClientRegistry implements AutoCloseable {
  private final Map<SqsClientId, SqsClientRegistration> registrations;

  public SqsClientRegistry(Collection<SqsClientRegistration> registrations) {
    Objects.requireNonNull(
        registrations,
        "registrations must not be null"
    );
    Map<SqsClientId, SqsClientRegistration> indexed = new LinkedHashMap<>();
    for (SqsClientRegistration registration : registrations) {
      Objects.requireNonNull(
          registration,
          "registration must not be null"
      );
      if (indexed.putIfAbsent(
          registration.clientId(),
          registration
      ) != null) {
        throw new IllegalStateException("Duplicate SQS clientId '" + registration.clientId().value() + "'");
      }
    }
    this.registrations = Map.copyOf(indexed);
  }

  public SqsAsyncClient client(SqsClientId clientId) {
    Objects.requireNonNull(
        clientId,
        "clientId must not be null"
    );
    SqsClientRegistration registration = registrations.get(clientId);
    if (registration == null) {
      throw new IllegalStateException("No SQS client configured for clientId '" + clientId.value() + "'");
    }
    return registration.client();
  }

  public boolean contains(SqsClientId clientId) {
    return registrations.containsKey(clientId);
  }

  public int size() {
    return registrations.size();
  }

  @Override
  public void close() {
    registrations.values()
        .stream()
        .filter(SqsClientRegistration::owned)
        .forEach(registration -> registration.client().close());
  }
}
