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

`KafkaBrokerProducer` sends to the destination target. Consumer adapters are configuration-driven and manually acknowledge Kafka only after a durable Inbox outcome. Inbox owns retry classification and timing; Kafka retry topics and DLQs are not the primary business retry mechanism. Configure Spring Kafka's `max.poll.interval.ms`, `max.poll.records`, and heartbeat/session timeouts for a period longer than the worst-case handler duration; those broker-client settings remain outside the `nerv.event.kafka` namespace.

Required headers are `nerv-event-id`, `nerv-event-type`, `nerv-event-source`, `nerv-event-timestamp`, and `nerv-event-content-type`; `nerv-event-correlation-id` is present when configured. The adapter also propagates trace context when tracing is active. Kafka remains at-least-once: acknowledgement ambiguity can produce redelivery, which is resolved through the Inbox.
