package com.czetsuyatech.nerv.event.sqs.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

class SqsClientRegistryTest {
  @Test
  void registersAndSelectsMultipleClientsById() {
    SqsAsyncClient a = mock(SqsAsyncClient.class);
    SqsAsyncClient b = mock(SqsAsyncClient.class);
    SqsClientRegistry registry = new SqsClientRegistry(
        List.of(
            SqsClientRegistration.applicationProvided(
                new SqsClientId("account-a"),
                a
            ),
            SqsClientRegistration.applicationProvided(
                new SqsClientId("account-b"),
                b
            )
        )
    );

    assertThat(registry.size()).isEqualTo(2);
    assertThat(registry.client(new SqsClientId("account-a"))).isSameAs(a);
    assertThat(registry.client(new SqsClientId("account-b"))).isSameAs(b);
  }

  @Test
  void unknownClientFailsWithoutArbitrarySelection() {
    SqsClientRegistry registry = new SqsClientRegistry(
        List.of(
            SqsClientRegistration.applicationProvided(
                new SqsClientId("default"),
                mock(SqsAsyncClient.class)
            )
        )
    );
    assertThatThrownBy(() -> registry.client(new SqsClientId("account-a")))
        .hasMessage("No SQS client configured for clientId 'account-a'");
  }

  @Test
  void duplicateClientIdFailsFast() {
    SqsClientId id = new SqsClientId("account-a");
    assertThatThrownBy(
        () -> new SqsClientRegistry(
            List.of(
                SqsClientRegistration.applicationProvided(
                    id,
                    mock(SqsAsyncClient.class)
                ),
                SqsClientRegistration.applicationProvided(
                    id,
                    mock(SqsAsyncClient.class)
                )
            )
        )
    )
        .hasMessage("Duplicate SQS clientId 'account-a'");
  }

  @Test
  void closesOnlyAdapterOwnedClients() {
    SqsAsyncClient owned = mock(SqsAsyncClient.class);
    SqsAsyncClient supplied = mock(SqsAsyncClient.class);
    new SqsClientRegistry(
        List.of(
            SqsClientRegistration.adapterOwned(
                new SqsClientId("owned"),
                owned
            ),
            SqsClientRegistration.applicationProvided(
                new SqsClientId("supplied"),
                supplied
            )
        )
    ).close();
    verify(owned).close();
    verify(
        supplied,
        never()
    ).close();
  }
}
