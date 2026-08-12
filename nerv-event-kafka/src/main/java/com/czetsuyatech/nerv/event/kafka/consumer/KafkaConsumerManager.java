package com.czetsuyatech.nerv.event.kafka.consumer;

import com.czetsuyatech.nerv.event.kafka.autoconfigure.NervEventKafkaProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * Starts and stops the listener containers configured for this application instance.
 */
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerManager implements SmartLifecycle {

  @NonNull
  private final NervEventKafkaProperties properties;

  @NonNull
  private final KafkaConsumerContainerFactory containerFactory;

  @NonNull
  private final KafkaConsumerAdapter adapter;
  private final Map<String, MessageListenerContainer> containers = new LinkedHashMap<>();
  private volatile boolean running;

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
              if (consumer.isEnabled()) {
                MessageListenerContainer container = containers.computeIfAbsent(
                    name,
                    ignored -> containerFactory.create(
                        name,
                        consumer,
                        adapter.forConsumer(
                            name,
                            consumer.getLeaseDuration()
                        )
                    )
                );
                container.start();
                log.info(
                    "Kafka event consumer started name={} topic={} groupId={} concurrency={}",
                    name,
                    consumer.getTopic(),
                    consumer.getGroupId(),
                    consumer.getConcurrency()
                );
              }
            }
        );
    running = true;
    log.info(
        "Kafka event consumer startup complete consumers={}",
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
              "Kafka event consumer stopped name={}",
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

  public synchronized Map<String, MessageListenerContainer> containers() {
    return Map.copyOf(containers);
  }
}
