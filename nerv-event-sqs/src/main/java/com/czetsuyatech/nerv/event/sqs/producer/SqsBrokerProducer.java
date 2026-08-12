package com.czetsuyatech.nerv.event.sqs.producer;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.observability.tracing.TraceContextCarrier;
import com.czetsuyatech.nerv.event.sqs.client.SqsClientRegistry;
import com.czetsuyatech.nerv.event.sqs.routing.SqsDestination;
import com.czetsuyatech.nerv.event.sqs.routing.SqsDestinationResolver;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

/**
 * Publishes generic broker messages to standard SQS queues.
 */
@Slf4j
public final class SqsBrokerProducer implements BrokerProducer {
  public static final BrokerId BROKER_ID = new BrokerId("sqs");
  public static final String EVENT_ID_ATTRIBUTE = "nerv-event-id";
  public static final String EVENT_TYPE_ATTRIBUTE = "nerv-event-type";
  public static final String EVENT_SOURCE_ATTRIBUTE = "nerv-event-source";
  public static final String EVENT_TIMESTAMP_ATTRIBUTE = "nerv-event-timestamp";
  public static final String CONTENT_TYPE_ATTRIBUTE = "nerv-event-content-type";
  public static final String CORRELATION_ID_ATTRIBUTE = "nerv-event-correlation-id";
  private final SqsDestinationResolver destinationResolver;
  private final SqsClientRegistry clientRegistry;
  private final Duration sendTimeout;

  public SqsBrokerProducer(
      SqsDestinationResolver destinationResolver,
      SqsClientRegistry clientRegistry,
      Duration sendTimeout
  )
  {
    this.destinationResolver = java.util.Objects.requireNonNull(destinationResolver);
    this.clientRegistry = java.util.Objects.requireNonNull(clientRegistry);
    this.sendTimeout = java.util.Objects.requireNonNull(sendTimeout);
    if (sendTimeout.isZero() || sendTimeout.isNegative()) {
      throw new IllegalArgumentException("sendTimeout must be greater than zero");
    }
  }

  @Override
  public BrokerId brokerId() {
    return BROKER_ID;
  }

  @Override
  public BrokerPublishResult publish(BrokerMessage message) {
    java.util.Objects.requireNonNull(
        message,
        "message must not be null"
    );
    SqsDestination destination = destinationResolver.resolve(message.target());
    SqsAsyncClient client = clientRegistry.client(destination.clientId());
    log.debug(
        "Publishing event to SQS eventId={} eventType={} target={} queue={} clientId={} correlationId={}",
        message.eventId().value(),
        message.eventType(),
        message.target(),
        destination.queue(),
        destination.clientId().value(),
        message.correlationId()
    );
    try {
      CompletableFuture<String> queueUrl = destination.queue().startsWith("https://")
          || destination.queue().startsWith("http://")
              ? CompletableFuture.completedFuture(destination.queue())
              : client.getQueueUrl(GetQueueUrlRequest.builder().queueName(destination.queue()).build())
                  .thenApply(response -> response.queueUrl());
      SendMessageResponse response = queueUrl.thenCompose(
          url -> client.sendMessage(
              SendMessageRequest.builder()
                  .queueUrl(url)
                  .messageBody(message.payload().value())
                  .messageAttributes(attributes(message))
                  .build()
          )
      )
          .get(
              sendTimeout.toMillis(),
              TimeUnit.MILLISECONDS
          );
      if (response == null || response.messageId() == null || response.messageId().isBlank()) {
        throw new IllegalStateException("SQS acknowledgement must include a messageId");
      }
      log.debug(
          "SQS event published eventId={} eventType={} queue={} clientId={} messageId={}",
          message.eventId().value(),
          message.eventType(),
          destination.queue(),
          destination.clientId().value(),
          response.messageId()
      );
      return new BrokerPublishResult(response.messageId());
    } catch (TimeoutException exception) {
      throw failure(
          message,
          destination,
          "timed out after " + sendTimeout
              + "; delivery may have succeeded and is ambiguous",
          exception
      );
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw failure(
          message,
          destination,
          "was interrupted",
          exception
      );
    } catch (ExecutionException exception) {
      throw failure(
          message,
          destination,
          "failed",
          exception.getCause()
      );
    } catch (RuntimeException exception) {
      if (exception instanceof SqsPublishException) {
        throw exception;
      }
      throw failure(
          message,
          destination,
          "failed",
          exception
      );
    }
  }

  private static Map<String, MessageAttributeValue> attributes(BrokerMessage message) {
    Map<String, MessageAttributeValue> values = new LinkedHashMap<>();
    put(
        values,
        EVENT_ID_ATTRIBUTE,
        message.eventId().value()
    );
    put(
        values,
        EVENT_TYPE_ATTRIBUTE,
        message.eventType()
    );
    put(
        values,
        EVENT_SOURCE_ATTRIBUTE,
        message.source()
    );
    put(
        values,
        EVENT_TIMESTAMP_ATTRIBUTE,
        message.timestamp().toString()
    );
    put(
        values,
        CONTENT_TYPE_ATTRIBUTE,
        message.payload().contentType()
    );
    if (message.correlationId() != null && !message.correlationId().isBlank()) {
      put(
          values,
          CORRELATION_ID_ATTRIBUTE,
          message.correlationId()
      );
    }
    TraceContextCarrier.current()
        .forEach(
            (
                name,
                value) -> {
              if (!values.containsKey(name))
                put(
                    values,
                    name,
                    value
                );
            }
        );
    return Map.copyOf(values);
  }

  private static void put(
      Map<String, MessageAttributeValue> values,
      String name,
      String value
  ) {
    values.put(
        name,
        MessageAttributeValue.builder().dataType("String").stringValue(value).build()
    );
  }

  private SqsPublishException failure(
      BrokerMessage message,
      SqsDestination destination,
      String outcome,
      Throwable cause
  ) {
    return new SqsPublishException(
        "SQS publish " + outcome + " for eventId=" + message.eventId().value()
            + " target=" + message.target() + " queue=" + destination.queue() + " clientId="
            + destination.clientId().value(),
        cause
    );
  }
}
