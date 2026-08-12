# SQS

`SqsBrokerProducer` publishes to standard SQS queues. `BrokerId("sqs")` selects the technology; `SqsClientId` selects one configured AWS client/account. Map a logical SQS target to that client and queue:

```yaml
nerv:
  event:
    sqs:
      enabled: true
      clients:
        account-a:
          region: us-east-1
        account-b:
          region: us-west-2
      destinations:
        payments: { client: account-a, queue: payment-events }
        notifications: { client: account-b, queue: notification-events }
      consumers:
        payments:
          enabled: true
          client: account-a
          queue: payment-events
          max-concurrent-messages: 10
          max-messages-per-poll: 10
          poll-timeout: 20s
          visibility-timeout: 2m
```

Each client requires either `region` (with the AWS SDK's normal credential provider behavior) or an application-provided client `bean-name`; `bean-name` cannot be combined with region/endpoint. `endpoint` is useful for a controlled emulator such as the demo's LocalStack setting. Do not put credentials in configuration examples.

Consumers perform manual delete only after durable Inbox registration/result. Configure visibility timeout longer than the combined worst-case registration, handler, durable result, and acknowledgement time, with additional buffer beyond the Inbox lease: the current adapter does **not** extend visibility for slow handlers. FIFO consumer queues are intentionally unsupported; an enabled queue ending in `.fifo` is rejected. SQS message attributes use the same `nerv-event-id`, `nerv-event-type`, `nerv-event-source`, `nerv-event-timestamp`, `nerv-event-content-type`, and optional `nerv-event-correlation-id` names as Kafka's headers.
