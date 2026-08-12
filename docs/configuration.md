# Configuration reference

Durations use Spring Boot duration syntax (`1s`, `30s`, `30m`, `30d`). Maps are keyed by application-selected names.

## Core

| Property | Default | Meaning |
| --- | --- | --- |
| `nerv.event.destinations.<name>.broker` | required | Broker ID, currently `kafka` or `sqs`. |
| `nerv.event.destinations.<name>.target` | required | Broker-specific target: Kafka topic or configured SQS destination name. |
| `nerv.event.dispatcher.enabled` | `true` | Enables Outbox dispatcher. |
| `.batch-size` | `100` | Rows claimed per dispatch cycle. Size this from database and broker load testing. |
| `.lease-duration` | `2m` | Outbox processing lease. Set it above worst-case claimed-batch processing time. |
| `.owner` | unset | Optional stable worker owner; it must be unique per running application instance. Otherwise resolved from pod/host/application identity. |
| `.polling.min-interval`, `.max-interval`, `.multiplier`, `.jitter` | `1s`, `30s`, `2.0`, `0.10` | Adaptive Outbox polling. `max-interval` is a hard upper bound after jitter. |
| application `RetryPolicy` bean | required for dispatcher | No library default exists because retry eligibility is business-specific. Use a bounded attempt count, capped backoff, and a terminal-failure alert/recovery policy. |
| `nerv.event.inbox.retry.enabled` | `true` | Enables policy-driven automatic retry decisions. |
| `.max-attempts`, `.initial-delay`, `.multiplier`, `.max-delay` | `5`, `5s`, `2.0`, `10m` | Handler-execution retry policy. `max-attempts` includes the initial handler execution; tune the retry budget to the dependency recovery objective. |
| `nerv.event.inbox.dispatcher.enabled` | `true` | Enables database-backed retry scheduler. |
| `.batch-size`, `.lease-duration` | `100`, `2m` | Retry claim batch and lease. Set the lease above worst-case handler and persistence time. |
| `.owner` | unset | Optional retry worker owner; it must be unique per running application instance. |
| `.polling.min-interval`, `.max-interval`, `.multiplier`, `.jitter` | `1s`, `30s`, `2.0`, `0.10` | Adaptive retry polling. `max-interval` is a hard upper bound after jitter. |
| `nerv.event.scheduler.health.execution-grace` | `2m` | Extra time allowed beyond a scheduler's expected next execution before scheduler health becomes `DOWN`. |

## Retention

| Property | Default | Meaning |
| --- | --- | --- |
| `nerv.event.retention.enabled` | `false` | Enables successful-row retention scheduler. Enable only after choosing an approved retention window. |
| `.outbox.enabled`, `.outbox.age` | `true`, `30d` | When retention is enabled, remove `PUBLISHED` Outbox rows older than age. |
| `.inbox.enabled`, `.inbox.age` | `true`, `30d` | When retention is enabled, remove `PROCESSED` Inbox rows older than age. |
| `.batch-size` | `500` | Maximum deletes for each category per cycle. |
| `.polling.min-interval`, `.max-interval`, `.multiplier`, `.jitter` | `30s`, `30m`, `2.0`, `0.10` | Adaptive retention polling. `max-interval` is a hard upper bound after jitter. |

## Kafka

| Property | Default | Meaning |
| --- | --- | --- |
| `nerv.event.kafka.enabled` | `false` | Activates Kafka adapter. |
| `.producer.send-timeout` | `30s` | Bounded broker acknowledgement wait. A timeout is ambiguous and must be handled as at-least-once delivery. |
| `.consumers.<name>.enabled` | `false` | Activates one configured consumer. |
| `.topic`, `.group-id` | required when enabled | Kafka topic and consumer group. |
| `.concurrency`, `.lease-duration` | `1`, `2m` | Consumer concurrency and Inbox lease. Do not configure concurrency above the partitions assigned to an instance. |

## SQS

| Property | Default | Meaning |
| --- | --- | --- |
| `nerv.event.sqs.enabled` | `false` | Activates SQS adapter. |
| `.clients.<id>.region` | required unless bean name | SDK-built client region. |
| `.clients.<id>.endpoint` | unset | Optional endpoint override. |
| `.clients.<id>.bean-name` | unset | Application-provided `SqsAsyncClient`; exclusive with region/endpoint. |
| `.destinations.<target>.client`, `.queue` | required | Client ID and queue selected by a logical SQS target. |
| `.producer.send-timeout` | `30s` | Bounded send acknowledgement wait. A timeout is ambiguous and must be handled as at-least-once delivery. |
| `.consumers.<name>.enabled` | `false` | Activates a consumer. |
| `.client`, `.queue` | required when enabled | Client ID and standard queue. |
| `.max-concurrent-messages`, `.max-messages-per-poll` | `10`, `10` | Container concurrency/poll size (poll size 1–10). |
| `.poll-timeout`, `.visibility-timeout` | `20s`, `2m` | Long poll (maximum 20s) and visibility (maximum 12h). Visibility must exceed worst-case registration, handler, durable result, and acknowledgement time. |

## Observability and operations web

| Property | Default | Meaning |
| --- | --- | --- |
| `nerv.event.observability.enabled` | `true` | Master observability switch. |
| `.metrics.enabled`, `.tracing.enabled`, `.health.enabled` | `true` | Individual integration switches. |
| `.health.outbox.max-pending`, `.max-oldest-pending-age` | unset | Optional thresholds; when configured and exceeded, the Outbox health indicator reports `DOWN`. |
| `.health.inbox.max-retry-pending`, `.max-oldest-retry-age` | unset | Optional thresholds; when configured and exceeded, the Inbox health indicator reports `DOWN`. |
| `nerv.event.operations.web.enabled` | `false` | Explicitly enables MVC administrative endpoints. |
| `.base-path` | `/management/nerv-event` | MVC endpoint prefix; must start with `/`. |
| `.payload.enabled` | `false` | Include stored payload in detail responses only. |

The full runnable configuration is in the [demo](../../nerv-examples/nerv-event-spring-boot-demo/src/main/resources/application.yml).
