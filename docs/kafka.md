# Kafka

Enable Kafka and map a logical destination to a topic:

```yaml
nerv:
  event:
    destinations:
      orders-kafka: { broker: kafka, target: order-events }
    kafka:
      enabled: true
      producer.send-timeout: 30s
      consumers:
        order-events:
          enabled: true
          topic: order-events
          group-id: orders-service
          concurrency: 1
          lease-duration: 2m
```

`KafkaBrokerProducer` sends to the destination target and uses `EventPublication.orderingKey` as the Kafka record key when present. NERV serializes Outbox claims for a shared key; Kafka then preserves order within the selected partition. Unkeyed records retain their previous null-key behavior. Consumer adapters manually acknowledge Kafka only after a durable Inbox outcome. Configure Kafka poll and heartbeat limits above the worst-case handler duration.

Required headers are `nerv-event-id`, `nerv-event-type`, `nerv-event-source`, `nerv-event-timestamp`, and `nerv-event-content-type`; `nerv-event-correlation-id` is present when configured. The adapter also propagates trace context when tracing is active. Kafka remains at-least-once: acknowledgement ambiguity can produce redelivery, which is resolved through the Inbox.
