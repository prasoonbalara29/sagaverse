# PROGRESS

## Current phase: 1c (service skeletons) — done. Next: 2 (happy path)

### Phase checklist

| Phase | Scope | Status |
|---|---|---|
| 0 | git init, AGENTS.md, README skeleton, PROGRESS.md, ADRs 0001–0011 | ✅ done |
| 1a | `./mvnw`, parent POM (enforcer, Spotless, Error Prone, Jib), `events` module + `EnvelopeTest` | ✅ done |
| 1b | `compose.yaml` (Postgres + 3 DBs/users, Kafka KRaft, topics-init), Makefile | ✅ done |
| 1c | 3 service skeletons, Flyway V1, actuator health, `*ApplicationIT` (Testcontainers) | ✅ done |
| 2 | Happy path end to end (naive mode), `.http` file, `HappyPathE2EIT` | ⏳ next |
| 3 | Declines and compensations (F1, F2, F7 naive) | pending |
| 4 | Outbox, inbox, idempotency (F3, F4: red on naive first, then green on reliable) | pending |
| 5 | Sweeper, timeouts, crash recovery (F5, F6, F8, F9), Jib images, `make up-all`, `make chaos-inventory` | pending |
| 6 | Observability polish, `make trace`, full README, final summary + quiz | pending |

1b was split from the original plan: infra in 1b, services in 1c. Together they'd have been about 20 files.

### Done in Phase 1c
- `order-service` (8081), `payment-service` (8082) and `inventory-service` (8083):
  - Spring Boot apps with web, actuator, validation, JDBC, Spring Kafka, Flyway and the PostgreSQL driver.
  - The Boot fat jar uses the `exec` classifier, so the plain jar can be a dependency of e2e-tests.
  - `application.yml` per service: own DB and user, `KAFKA_BOOTSTRAP`/`DB_HOST` overridable,
    `enable-auto-commit: false`, `acks=all` plus an idempotent producer, BigDecimal-for-floats,
    health with details and probes, ECS structured logs.
- Flyway `V1__init.sql` per service, with the tables from the ADRs:
  - order: `orders`, `saga_instance` (version column, partial due-index for the sweeper), `saga_transition`, `outbox`, `inbox`.
  - payment: `payment` (saga_id UNIQUE), `ledger` (append-only, no unique key on purpose), `outbox`, `inbox`.
  - inventory: `stock`, `reservation` (saga_id PK, quantity 0 = tombstone), `outbox`, `inbox`, plus seed SKU-1=100, SKU-2=10, SKU-EMPTY=0.
- `*ServiceApplicationIT` per service, using Testcontainers Postgres + Kafka with `@ServiceConnection`. They check:
  health UP including the DB, Flyway created exactly that service's tables, the Kafka broker is reachable,
  and (inventory) the stock is seeded.
- `testcontainers.properties` per service: `hub.image.name.prefix=docker.intuit.com/docker-rmt/`. This also
  covers Ryuk, and works from IntelliJ, not only from Maven.
- `make build` target. The README "How to run" section now covers dev mode.
- File count: 21 (above the ~15 guideline). Most of it is per-service boilerplate repeated 3×. I didn't split
  further, because each phase must cover all three services.

### Real output (Phase 1c)
```
$ make verify
Tests run: 18, Failures: 0 ... EnvelopeTest
Tests run: 3, Failures: 0 ... OrderServiceApplicationIT
Tests run: 3, Failures: 0 ... PaymentServiceApplicationIT
Tests run: 4, Failures: 0 ... InventoryServiceApplicationIT
events / order-service / payment-service / inventory-service ... SUCCESS
BUILD SUCCESS

Dev mode (make up, then ./mvnw -pl <svc> spring-boot:run for all three):
8081: UP db=UP   8082: UP db=UP   8083: UP db=UP
{"@timestamp":"…","log":{"level":"INFO","logger":"io.sagaverse.order.OrderServiceApplication"},…,"service":{"name":"order-service"…},"message":"Started OrderServiceApplication in …
```
Prediction before the run: health UP with the db component, exact table sets per V1, 1 Kafka node, 3 seeded SKUs.
Result: matched. No differences.

### Done in Phase 1b
- `compose.yaml`:
  - Postgres 17: the init script creates `order_db`, `payment_db` and `inventory_db`, each owned by its own user.
    `CONNECT` is revoked from PUBLIC. The healthcheck goes over TCP and waits for the last DB.
  - Kafka 3.9.2 KRaft, single broker. Auto-create is off, 3 default partitions, 512m heap.
    Listener `kafka:9092` for containers, `localhost:9092` for the host.
  - `topics-init`: a one-shot run of `infra/kafka/create-topics.sh` (8 topics, idempotent).
  - Memory limits on everything (Postgres 256m, Kafka 768m, init 256m).
- Makefile: `help`, `up`, `down`, `ps`, `logs`, `topics`, `consume TOPIC=…`, `test`, `verify`, `fmt`.
  `JAVA_HOME` comes from the env or `.java-home`, and is never selected by version.
  `DOCKER_HOST` is pinned to the Podman machine socket.
- **Image mirror.** Public Docker Hub is blocked: Zscaler TLS inspection, untrusted inside the Podman VM.
  All images now come from `docker.intuit.com/docker-rmt/...`. The Jib base default is the
  `docker-rmt/library/amazoncorretto:21` image. Recorded in ADR-0011 and AGENTS.md.
- **Kafka UI dropped.** Neither Redpanda Console nor AKHQ is available on the mirror. The CLI targets
  (`topics`, `consume`) replace them.

### Real output
```
$ make up        # ... Created topic payment.commands. ... (8 topics)
$ make topics
topic                   partitions  rf
inventory.commands      3           1
inventory.commands.DLT  3           1
... (8 rows, all 3 / 1)
DB isolation:  order_svc -> order_db: order_svc
               payment_svc -> order_db: DETAIL:  User does not have CONNECT privilege.
Memory:        sagaverse-kafka-1 428.5MB / 805.3MB, sagaverse-postgres-1 30.75MB / 268.4MB
Second `make up`: exit 0 (topics-init idempotent)
$ make verify    # openjdk version "21.0.10" ... Tests run: 18, Failures: 0 ... BUILD SUCCESS
```

### Done in Phase 1a
- `./mvnw` (Maven Wrapper 3.3.4, `only-script`, Maven 3.9.11 from Maven Central).
- Parent POM on `spring-boot-starter-parent` **3.5.16** (the latest 3.x). Boot manages Kafka clients 3.9.2,
  Jackson 2.21.4, Flyway 11.7.2, PostgreSQL driver 42.7.11, Testcontainers 1.21.4 and Awaitility 4.2.2.
- Enforcer: Java `[21,22)`, Maven `[3.9,)`.
- Spotless 3.11.0 (google-java-format + remove unused imports), with `check` bound to `verify`.
- Error Prone 2.53.0 as a javac plugin (`.mvn/jvm.config` holds the JDK 21 `--add-exports`).
  Confirmed working: a planted `DeadException` failed compilation, and the probe was deleted.
- Jib 3.5.2 in `pluginManagement`. Base image property `jib.base.image`, defaulting to
  `docker.io/library/amazoncorretto:21`. Services will enable it in 1b/5.
- `events` module with no compile-scope dependencies:
  - `EventEnvelope<P>` (eventId, sagaId, type, version, occurredAt, payload).
  - A sealed `SagaPayload` with sealed `PaymentCommand`, `PaymentReply`, `InventoryCommand` and `InventoryReply`.
    Handlers can switch over them exhaustively.
  - Payload records in `PaymentMessages` and `InventoryMessages`, validated in their compact constructors.
  - `EventType` catalogue (wire name → payload class → topic). `Topics` (4 main + 4 DLT, 3 partitions).
- `EnvelopeTest`, 18 tests: round trip of every type the way a consumer reads it, exact wire fields, a fresh
  eventId per envelope, the catalogue covers every sealed leaf, each command/reply goes to the right topic,
  an unknown type gives empty, an invalid payload is rejected at deserialization, and the DLT list.

### Real output (`./mvnw -B -ntp clean verify`, explicit `JAVA_HOME`)
```
[INFO] Rule 0: org.apache.maven.enforcer.rules.version.RequireJavaVersion passed
[INFO] Rule 1: org.apache.maven.enforcer.rules.version.RequireMavenVersion passed
[INFO] Tests run: 18, Failures: 0, Errors: 0, Skipped: 0
[INFO] --- spotless:3.11.0:check (spotless-check) @ events ---
[INFO] sagaverse .......................................... SUCCESS
[INFO] events ............................................. SUCCESS
[INFO] BUILD SUCCESS
```

### Decisions made in this phase
- **Money precision bug, found by a test.** The first run failed:
  `expected: ChargePayment[... amount=42.50] but was: ChargePayment[... amount=42.5]`.
  Reading a payload as a `JsonNode` makes Jackson store decimals as `double`. The fix is a rule now recorded
  in ADR-0011: every service mapper enables `USE_BIG_DECIMAL_FOR_FLOATS` and exact BigDecimal nodes.
- Payloads are grouped as nested records in one file per counterpart (`PaymentMessages`, `InventoryMessages`)
  instead of 10 tiny files. That makes the contract easy to read in one place and keeps the phase small.
- The plan said `EventTypes` constants. I used an `EventType` enum instead, which also carries the payload
  class and topic, so tests can check that the catalogue is complete.
- The wrapper downloads Maven from Maven Central (portable). Dependencies still go through your
  `~/.m2/settings.xml` mirror.

### Open issues
- Jackson `JsonNodeFactory.withExactBigDecimals(true)` can't be set via properties. Phase 2 adds a mapper customizer
  in each service (ADR-0011).
- Jib runs on the host JDK. Pulling the base image from `docker.intuit.com` should work, but that's untested until Phase 5.
- `podman compose` delegates to Rancher Desktop's `docker-compose` v5 binary, pointed at the Podman socket. It works,
  but it prints a banner on every call.

### Next step
Phase 2: happy path in naive mode. `POST /orders`, the orchestrator state machine (unit-tested per transition),
the payment and inventory command handlers, the `e2e-tests` module and `HappyPathE2EIT`, and `requests.http`.
Phase 2 will be well over 15 files, so I'll propose a split (2a: orchestrator + order API, 2b: handlers + e2e) when I start it.
