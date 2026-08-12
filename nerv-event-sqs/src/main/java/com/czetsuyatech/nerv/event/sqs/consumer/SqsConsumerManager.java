package com.czetsuyatech.nerv.event.sqs.consumer;

import com.czetsuyatech.nerv.event.sqs.autoconfigure.NervEventSqsProperties;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientId;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistry;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Owns lifecycle and client selection for configured SQS consumer containers.
 */
@Slf4j
public final class SqsConsumerManager implements SmartLifecycle {
  private final NervEventSqsProperties properties;
  private final SqsClientRegistry clientRegistry;
  private final SqsConsumerContainerFactory containerFactory;
  private final SqsConsumerAdapter adapter;
  private final Map<String, MessageListenerContainer<String>> containers = new LinkedHashMap<>();
  private volatile boolean running;

  public SqsConsumerManager(
      NervEventSqsProperties properties,
      SqsClientRegistry clientRegistry,
      SqsConsumerContainerFactory containerFactory,
      SqsConsumerAdapter adapter
  )
  {
    this.properties = Objects.requireNonNull(properties);
    this.clientRegistry = Objects.requireNonNull(clientRegistry);
    this.containerFactory = Objects.requireNonNull(containerFactory);
    this.adapter = Objects.requireNonNull(adapter);
    validateClientReferences();
  }

  private void validateClientReferences() {
    properties.getConsumers()
        .forEach(
            (
                name,
                consumer) -> {
              if (consumer.isEnabled()) {
                SqsClientId id = new SqsClientId(consumer.getClient());
                if (!clientRegistry.contains(id)) {
                  throw new IllegalStateException(
                      "Invalid SQS consumer '" + name + "': client '"
                          + id.value() + "' is not configured"
                  );
                }
              }
            }
        );
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    properties.getConsumers()
        .forEach(
            (
                name,
                consumer) -> {
              if (!consumer.isEnabled()) {
                return;
              }
              SqsClientId clientId = new SqsClientId(consumer.getClient());
              SqsAsyncClient client = clientRegistry.client(clientId);
              MessageListenerContainer<String> container = containers.computeIfAbsent(
                  name,
                  ignored -> containerFactory.create(
                      name,
                      consumer,
                      client,
                      adapter.forConsumer(
                          name,
                          consumer.getQueue(),
                          clientId
                      )
                  )
              );
              container.start();
              log.info(
                  "SQS event consumer started name={} queue={} clientId={} maxConcurrentMessages={}",
                  name,
                  consumer.getQueue(),
                  clientId.value(),
                  consumer.getMaxConcurrentMessages()
              );
            }
        );
    running = true;
    log.info(
        "SQS event consumer startup complete consumers={}",
        containers.size()
    );
  }

  @Override
  public synchronized void stop() {
    if (!running) {
      return;
    }
    containers.forEach(
        (
            name,
            container) -> {
          container.stop();
          log.info(
              "SQS event consumer stopped name={}",
              name
          );
        }
    );
    running = false;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public boolean isAutoStartup() {
    return true;
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE - 100;
  }

  public synchronized Map<String, MessageListenerContainer<String>> containers() {
    return Map.copyOf(containers);
  }
}
