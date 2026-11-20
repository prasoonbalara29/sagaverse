# ADR-0011: Runtime, tooling and test harness

- Status: Accepted
- Date: 2026-11-20

## Decision

**Stack:** Java 21, Spring Boot 3.5.x (the latest 3.x patch, checked when the parent POM is created),
Spring Kafka, Spring JDBC, Flyway, PostgreSQL. Maven multi-module via `./mvnw`.

**JDK:** there's no SDKMAN and no `.sdkmanrc`. Developers set `JAVA_HOME` explicitly to their chosen
JDK's home (IntelliJ project SDK, or `export JAVA_HOME=<path-to-intended-jdk>/Contents/Home`).
The Makefile also reads an untracked `.java-home` file. The Maven enforcer requires Java `[21,22)`.
Nothing selects a JDK by version number alone.

**Message format:** JSON envelope `{eventId, sagaId, type, version, occurredAt, payload}`, with
Jackson. The `events` module holds the records and constants only. No Schema Registry and no Avro.
Each consumer rejects an unknown `type` or unsupported `version` (they go to the DLT, ADR-0007).
Every service's `ObjectMapper` **must** enable `USE_BIG_DECIMAL_FOR_FLOATS` and
`JsonNodeFactory.withExactBigDecimals(true)`. Consumers read the payload as a `JsonNode` before
converting it, and Jackson's default would turn `42.50` into the double `42.5` and lose precision
on money. `EnvelopeTest` caught this in Phase 1a.

**Logging:** Spring Boot's built-in structured logging (`logging.structured.format.console=ecs`),
with no extra dependency. `sagaId` and `orderId` are put into MDC by a Kafka `RecordInterceptor`
(read from the envelope and the key), a servlet filter (REST), and wrappers around relay and sweeper
work. Every log line within a saga can be found with one grep.

**Quality gates (`make verify`):** Spotless (google-java-format) check, Error Prone at compile time,
Surefire (`*Test`), Failsafe (`*IT`).

**Tests:**
- Unit: JUnit 5, AssertJ, Mockito (state machine transitions, handlers).
- Integration (per service): Testcontainers Postgres + Kafka (`docker.io/apache/kafka`), real
  Flyway migrations.
- End-to-end (`e2e-tests`): boots **all three Spring apps in one JVM** (`SpringApplicationBuilder`,
  separate contexts, random ports, separate DBs on one Postgres container) against one Kafka
  container. A "crash" means closing a service's context mid-saga and starting a new one. That's fast
  and deterministic, with no container orchestration inside tests. To make this work, service
  modules produce a plain jar, and the Boot repackage step uses the `exec` classifier.
- Awaitility for every async wait. `Thread.sleep` is forbidden.
- Testcontainers targets **Podman** via `DOCKER_HOST` (exported by the Makefile from `podman machine
  inspect`). It never falls back to another Docker-compatible runtime.

**Containers and compose (Podman):**

Public Docker Hub (`docker.io`) is blocked on this network: Zscaler TLS inspection, and the Podman VM doesn't
trust its CA. Every Docker Hub image therefore comes through the internal mirror
**`docker.intuit.com/docker-rmt/<namespace>/<image>:<tag>`** (official images use the `library/` namespace).
Testcontainers uses the same mirror via `hub.image.name.prefix`. Redpanda Console and AKHQ are not
available on the mirror, so the opt-in Kafka UI profile was dropped. Use `make topics` and
`make consume TOPIC=…` instead.

| Service | Image | Memory limit |
|---|---|---|
| postgres | `docker.intuit.com/docker-rmt/library/postgres:17-alpine` (init script creates 3 DBs + 3 users) | 256m |
| kafka | `docker.intuit.com/docker-rmt/apache/kafka:3.9.2` (KRaft, single broker, `KAFKA_HEAP_OPTS=-Xmx512m -Xms512m`) | 768m |
| topics-init | same Kafka image, runs once | 256m |
| order/payment/inventory | Jib images (base `jib.base.image`, default `docker.intuit.com/docker-rmt/library/amazoncorretto:21`), `-Xmx256m` | 384m each |

That's about 2.1 GB with everything up. Healthchecks are on every long-running service. Ordering uses
`depends_on: condition: service_healthy` and `service_completed_successfully` (topics-init).

**Run modes:** dev (`make up` starts infra; services run via `./mvnw -pl <svc> spring-boot:run`) and
full (`make up-all` builds the Jib images with `-Djib.dockerClient.executable=podman` and starts everything).

## Alternatives rejected

- **Multi-stage Containerfile:** works, but it rebuilds dependencies inside the container. Jib
  layers dependencies separately and needs no Dockerfile.
- **Booting services as containers in e2e tests:** closer to production, but slower and flakier,
  and a "crash" becomes container orchestration. The chaos demo covers real container kills.
- **logstash-logback-encoder:** an unnecessary dependency since Boot 3.4 added structured logging.
- **Checkstyle + SpotBugs:** heavier and noisier than Spotless + Error Prone for a learning repo.
