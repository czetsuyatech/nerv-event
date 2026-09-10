# Release Readiness

## Verdict

**READY FOR RC AND CENTRAL RELEASE PREPARATION.** The complete JDK 21 reactor, Docker-backed
integration suites, resilience profile, and external Spring Boot acceptance suite all pass. The
2026-08-24 re-evaluation also passed after the JPA repository-registration refactor and confirmed
the operations-web endpoint live. Central metadata, source JARs, Javadoc JARs, and the
signing/publishing profile are present. The release signing key is managed by the release owner;
no signing or Central deployment was attempted in this audit.

## Environment

- OS: Windows 11; Docker Desktop 28.4.0.
- Build JDK: Corretto Java 21.0.12. The project targets Java 21 and the full release matrix below ran on that runtime.
- Maven: local Apache Maven installation; Enforcer requires Java 21+ and Maven 3.8.1+.
- Spring Boot: exactly 4.1.0, not an asserted version range.
- Integration infrastructure exercised: PostgreSQL 16/17 Testcontainers, Apache Kafka Native 3.8/3.9, and LocalStack SQS 4.7/4.10.
- Bugbot: not executed in this session; the defects found during the audit were reproduced and verified through the automated suites below.

## Test Matrix

| Area | Result | Notes |
|---|---|---|
| Full reactor + integration | PASS | `mvn clean verify -Pintegration-tests -q` on Java 21.0.12. Docker-backed PostgreSQL, Kafka, and SQS suites passed. |
| Resilience | PASS | `mvn verify -Presilience -q` on Java 21.0.12. |
| PostgreSQL concurrency | PASS | Active leases denied, expired leases reclaimed, stale-owner and stale-version transitions rejected, and retention/migrations passed. |
| Kafka adapter | PASS | Kafka integration suite passed. |
| SQS adapter | PASS | LocalStack suite passed. |
| Starter | PASS | Minimal and explicit application JPA repository scans passed. |
| External demo E2E | PASS | `mvn clean verify -Pintegration-tests -q` on Java 21. It exercised REST -> Outbox -> Kafka -> Inbox -> SQS -> Inbox, retry, operations recovery, and rollback. |
| Publication artifacts | PASS | All ten publishable modules produced one main JAR, one source JAR, and one Javadoc JAR. |
| GPG signing | READY | The release owner confirmed the private signing key is managed in their local profile. No signing/deploy was attempted in this audit. |
| Dependency tree / vulnerability scan | NOT EXECUTED | No configured vulnerability scanner; retain this evidence in CI before a production release. |

## Re-evaluation: 2026-08-24

| Area | Result | Notes |
|---|---|---|
| Working trees | PASS | `nerv-event` and `nerv-event-spring-boot-demo` were clean before the checks. |
| Library release gate | PASS | `mvn clean verify` passed on the active X: checkout, including formatter validation, compilation, unit tests, source JARs, and Javadoc JARs. |
| Normal consumer adoption | PASS | The demo uses only `@SpringBootApplication`; it has no application-specific JPA repository scan or NERV opt-in annotation. |
| Repository registration | PASS | The Boot package marker covers `com.czetsuyatech.nerv.event.persistence`, registering both core and operations Spring Data repositories while preserving an application's arbitrary package namespace. |
| Demo build | PASS | `mvn clean verify` passed after resolving the exact verified library artifacts from the local Maven cache. |
| Demo integration | PASS | `mvn verify -Pintegration-tests` passed with PostgreSQL, Kafka, and LocalStack Testcontainers. |
| Operations-web route | PASS | `GET http://localhost:8080/management/nerv-event/outbox` returned HTTP 200 with the expected empty paged JSON response. |

## Findings

| ID | Severity | Area | Finding | Resolution | Status |
|---|---|---|---|---|---|
| RR-01 | BLOCKER | Demo adoption | The external Kafka-to-SQS acceptance flow initially had no valid transaction boundary for its handler and lacked required Outbox retry configuration. | The demo now uses `2.0.0`, provides the documented `RetryPolicy`, and runs its follow-up publisher in `@Transactional`. Full Docker E2E passes. | Fixed |
| RR-02 | BLOCKER | Maven Central | Central identity metadata, source/Javadoc JAR generation, GPG configuration, and the Central publishing profile are present. | The release owner confirmed their existing private signing key is managed locally. Run the approved Central deployment from the release environment. | Fixed |
| RR-03 | BLOCKER | Operations | Operations implementations were final while class-based transactional proxies were enabled. | Removed `final`; reactor and demo verification pass. | Fixed |
| RR-04 | HIGH | Kafka Boot integration | Kafka producer/consumer auto-configuration could evaluate before Boot supplied `KafkaTemplate`/`ConsumerFactory`. | Ordered configuration after Boot Kafka; external Kafka-to-SQS E2E passes. | Fixed |
| RR-05 | MEDIUM | Java compatibility | Release target is Java 21. | Full integration and resilience matrices passed under Java 21.0.12. | Fixed |
| RR-06 | MEDIUM | Long-running handlers | Inbox lease defaults to 30 seconds and SQS visibility is application configured; neither has heartbeat/visibility extension. | Tune lease/visibility above handler latency; handlers must remain idempotent. | Known limitation |
| RR-07 | MEDIUM | Multiple persistence units | JPA auto-configuration targets the default Boot persistence unit. | Keep/document explicit configuration requirement for multi-EMF applications. | Known limitation |
| RR-08 | INFO | Delivery semantics | Broker-send success followed by database `markPublished` failure remains intentionally ambiguous. | Documented at-least-once behavior. | Accepted |
| RR-09 | INFO | Scheduling | All pods may run Outbox, Inbox retry, and retention schedulers. PostgreSQL claims, leases, owner guards, and bounded state-guarded deletes provide correctness; leader election is not required. | Covered by PostgreSQL concurrency tests. | Accepted |
| RR-10 | MEDIUM | Inbox observability | Successful processing did not increment durable `attempt_count`, so a retry-then-success appeared as one execution. | The JPA `PROCESSED` transition now increments the counter; focused persistence and demo retry tests pass. | Fixed |
| RR-11 | LOW | Operations-web regression coverage | The demo integration suite does not make an HTTP request to the operations-web routes. | The operations-web module covers the controller route contract, and the live demo smoke check returned HTTP 200. Add the route to the demo integration suite before the next feature release. | Open |

## Known Limitations

- Delivery is at-least-once, not distributed exactly-once. Consumers must remain idempotent.
- A handler exceeding its Inbox lease or SQS visibility timeout may be delivered/claimed again.
- Default single `DataSource`/`EntityManagerFactory` adoption is seamless; multiple persistence units need explicit application configuration.
- Retention removes only terminal successful `PUBLISHED` Outbox and `PROCESSED` Inbox rows. Failed and in-flight records are intentionally retained.
- The demo integration suite does not yet assert the operations-web HTTP routes directly. The
  operations-web module covers them, and the current demo smoke check passed.

## Release Action

1. From the approved release environment, run the intentional Central deployment from a clean checkout with `-Pcentral-release`.

## Compatibility

- Coordinates: `com.czetsuyatech.nerv`; current version `2.0.0`.
- Version 2.0.0 intentionally changes the public `OutboxService` post-claim transition contract.
  See [Upgrading to 2.0](docs/upgrading-to-2.0.md).
- Java bytecode target: 21.
- Spring Boot target tested here: 4.1.0 only.
- Kafka and SQS envelopes use UTF-8 text payloads and carry EventId, type, source, correlation ID, timestamp, and content type. Persisted payloads depend on the configured Jackson-compatible payload model; upgrades must preserve deserialization compatibility.
- Canonical PostgreSQL migrations are packaged under `META-INF/nerv-event/db/postgresql/migration/`, outside Flyway's automatic discovery path. Runtime does not require DDL privileges.

## Delivery Guarantees

`EventPublisher` calls `JpaOutboxRepository.save` with `MANDATORY` propagation, so an application transaction atomically commits or rolls back its business row and Outbox row. Claim, register, and state-transition work uses short `REQUIRES_NEW` transactions. No database transaction spans broker send, handler execution, or broker acknowledgement.

Outbox claims use PostgreSQL pessimistic write locks, persist owner/lease state, increment the
durable claim version, release the database transaction before sending, and guard subsequent
transitions by owner, exact claim version, and `PROCESSING` status. Inbox registration relies on
the primary-key EventId uniqueness constraint and converts duplicate-key races to durable duplicate
results. Kafka/SQS adapters persist durable Inbox outcomes before acknowledgement/delete; terminal
and retry-pending duplicates are acknowledged without handler replay.

## Final Recommendation

**Create and test the RC now. The release owner may publish to Maven Central through the approved
signed deployment process. Add an operations-web HTTP assertion to the demo integration suite
before the next feature release.**
