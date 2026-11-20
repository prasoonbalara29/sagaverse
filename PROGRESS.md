# PROGRESS

## Current phase: 2b (payment + inventory handlers) — done. Next: 2c (e2e module)

### Phase checklist

| Phase | Scope | Status |
|---|---|---|
| 0 | git init, AGENTS.md, README skeleton, PROGRESS.md, ADRs 0001–0011 | ✅ done |
| 1a | `./mvnw`, parent POM (enforcer, Spotless, Error Prone, Jib), `events` module + `EnvelopeTest` | ✅ done |
| 1b | `compose.yaml` (Postgres + 3 DBs/users, Kafka KRaft, topics-init), Makefile | ✅ done |
| 1c | 3 service skeletons, Flyway V1, actuator health, `*ApplicationIT` (Testcontainers) | ✅ done |
| 2a | Order API, orchestrator state machine (happy path), naive publisher, reply listener, `OrderSagaHappyPathIT` | ✅ done |
| 2b | Payment + inventory command handlers (naive), per-service command ITs | ✅ done |
| 2c | `e2e-tests` module booting all three apps, `HappyPathE2EIT`, `requests.http` | ⏳ next |
| 3 | Declines and compensations (F1, F2, F7 naive) | pending |
| 4 | Outbox, inbox, idempotency (F3, F4: red on naive first, then green on reliable) | pending |
| 5 | Sweeper, timeouts, crash recovery (F5, F6, F8, F9), Jib images, `make up-all`, `make chaos-inventory` | pending |
| 6 | Observability polish, `make trace`, full README, final summary + quiz | pending |

1b was split from the original plan: infra in 1b, services in 1c. Together they'd have been about 20 files.

### Done in Phase 2b (payment-service and inventory-service)
- **payment-service `PaymentService`**: `ChargePayment` appends a `CHARGE` ledger row, upserts the per-saga `payment` row
  (CHARGED) and replies `PaymentCharged(paymentId, amount)`. **Naive**: no dedup. Each delivery adds another ledger
  row, and the upsert hides the repeat. That's F3, demonstrated in Phase 4. `RefundPayment` is logged and ignored
  until Phase 3.
- **inventory-service `InventoryService`**: `ReserveStock` does an atomic `UPDATE stock … WHERE available >= qty`.
  On success it upserts a reservation (naive: the quantity is *added*, so a duplicate doubles the hold) and replies
  `StockReserved`. Otherwise it replies `StockUnavailable(OUT_OF_STOCK)` and touches nothing. `ReleaseStock` is
  logged and ignored until Phase 3.
- Each service has its own copy of `EnvelopeCodec` (plus a `decode(json, PaymentCommand.class)` overload that
  rejects a message from the wrong family), `MessagePublisher`, `DirectKafkaPublisher` (naive, after-commit
  send), `AppConfig` (Clock + exact-decimal Jackson) and a `CommandListener` that sets `orderId`/`sagaId` in MDC.
  The copies are deliberate (ADR-0001).
- `application.yml`: `ack-mode: record`, `saga.mode: naive`.
- Tests: `PaymentCommandsIT` (1) and `InventoryCommandsIT` (2). In each, the test plays the orchestrator over real Kafka.
- 17 files touched, counting PROGRESS.md.

### Predict, then run (2b)
Predicted:
- payment: a `PaymentCharged` reply with the same sagaId, the reply's paymentId equal to the DB row's,
  `payment.status = CHARGED`, exactly 1 `ledger` row `CHARGE 42.50`.
- inventory: SKU-1×2 gives `StockReserved`, stock 100→98, reservation RESERVED qty 2.
  SKU-EMPTY×1 gives `StockUnavailable(OUT_OF_STOCK)`, stock stays 0, no reservation row.

Result: **matched.**

```
$ make verify
Tests run: 18 EnvelopeTest | 8 SagaStateMachineTest | 5 EnvelopeCodecTest
Tests run: 3 OrderServiceApplicationIT | 3 OrderSagaHappyPathIT
Tests run: 3 PaymentServiceApplicationIT | 1 PaymentCommandsIT
Tests run: 4 InventoryServiceApplicationIT | 2 InventoryCommandsIT
BUILD SUCCESS   (53 tests)
```

### Done in Phase 2a (order-service only; payment and inventory handlers come in 2b)
- **`SagaStateMachine`**: a pure function from (saga, reply) to `Decision` (from, to, trigger, next step,
  deadline, order status, commands, or ignored with a reason). No DB and no Kafka, so every transition is
  unit-tested without containers. Happy path only: `start → PAYMENT_PENDING (ChargePayment)`,
  `PaymentCharged → INVENTORY_PENDING (ReserveStock)`, `StockReserved → COMPLETED (order CONFIRMED)`.
  Any other reply, or a reply in the wrong state, is ignored with a reason. That includes `PaymentDeclined`
  and `StockUnavailable`, which a test pins until Phase 3 adds them.
- **`SagaOrchestrator`**: one `TransactionTemplate` transaction updates the saga (optimistic `version`), the order
  status and `saga_transition`, and hands the commands to `MessagePublisher`.
- **`MessagePublisher`**: the seam for ADR-0010. The only implementation so far is **`DirectKafkaPublisher` (naive)**.
  It registers an `afterCommit` callback and sends then. That is the dual write, commit then send, with a comment
  marking where a crash loses the event (F4, demonstrated in Phase 4).
- **`EnvelopeCodec`**: two-step decode (raw tree, check type and version, then convert). It rejects malformed,
  unknown-type and future-version messages with `MessageRejectedException` subtypes. The DLT routing that uses
  them comes in Phase 3.
- **`ReplyListener`**: `@KafkaListener` on `payment.replies` + `inventory.replies`, `ack-mode: record`. It puts
  `orderId`/`sagaId` into MDC.
- **REST**: `POST /orders` → 202 with a `Location` header (Bean Validation: 400 on a bad body). `GET /orders/{id}` →
  the order plus saga state, step, attempts, deadline and full transition history.
- **Config**: `SagaProperties` (`saga.mode/step-timeout/max-step-attempts/max-compensation-attempts/sweep-interval`),
  a `Clock` bean, and a Jackson customizer for exact-BigDecimal tree nodes (the open issue from 1b, now closed).
- Tests: `SagaStateMachineTest` (8), `EnvelopeCodecTest` (5), `OrderSagaHappyPathIT` (3, with the test playing
  payment and inventory over real Kafka).
- 19 files touched, counting PROGRESS.md (above the ~15 guideline). The state machine, orchestrator and their tests are one unit,
  and splitting them would leave a phase that can't be tested end to end.

### Predict, then run: `OrderSagaHappyPathIT`
Predicted:
1. POST gives 202, order PENDING, saga PAYMENT_PENDING (step ChargePayment, attempts 1, deadline now+10s),
   transition `null → PAYMENT_PENDING`.
2. `ChargePayment(orderId, cust-1, 42.50)` on `payment.commands`, keyed by orderId, with the same sagaId.
3. After `PaymentCharged`: saga INVENTORY_PENDING, and `ReserveStock(orderId, SKU-1, 2)` on `inventory.commands`.
4. After `StockReserved`: saga COMPLETED, deadline null, order CONFIRMED, exactly 3 transitions.

Result: **matched, no differences.**

### Real output (Phase 2a)
```
$ make verify
Tests run: 18 ... EnvelopeTest
Tests run: 8  ... SagaStateMachineTest
Tests run: 5  ... EnvelopeCodecTest
Tests run: 3  ... OrderServiceApplicationIT
Tests run: 3  ... OrderSagaHappyPathIT
Tests run: 3  ... PaymentServiceApplicationIT
Tests run: 4  ... InventoryServiceApplicationIT
BUILD SUCCESS

Log lines from OrderSagaHappyPathIT (ECS JSON; sagaId/orderId are top-level fields on each):
{sagaId: 54cfee77…, orderId: 5cb00751…} DirectKafkaPublisher | sent ChargePayment to payment.commands
{sagaId: 54cfee77…, orderId: 5cb00751…} SagaOrchestrator     | order placed, saga started in PAYMENT_PENDING
{sagaId: 54cfee77…, orderId: 5cb00751…} DirectKafkaPublisher | sent ReserveStock to inventory.commands
{sagaId: 54cfee77…, orderId: 5cb00751…} SagaOrchestrator     | saga PAYMENT_PENDING -> INVENTORY_PENDING on PaymentCharged
{sagaId: 54cfee77…, orderId: 5cb00751…} SagaOrchestrator     | saga INVENTORY_PENDING -> COMPLETED on StockReserved
```

### Decisions made in 2a
- The naive publisher sends from an `afterCommit` transaction callback, so the orchestrator code is the **same
  in both modes**. In Phase 4 the reliable publisher only swaps the bean (an outbox insert inside the same
  transaction). Seeing that the same call site is unsafe or safe depending only on the publisher is the lesson.
- I used Spring's `org.springframework.lang.Nullable`. `jspecify` isn't on the approved dependency list, and Spring 6.2
  doesn't pull it in.
- A reply for an unknown saga is logged and dropped (WARN). It can't be compensated, because there's no saga to drive.

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
- Kafka client config dumps (INFO) clutter the ECS logs. Phase 6 will set `org.apache.kafka` to WARN.
- `DefaultErrorHandler` + DLT routing for `MessageRejectedException` isn't wired yet (Phase 3). Today a poison reply
  is retried by Spring Kafka's default backoff (10 attempts), then skipped and logged.
- Jib runs on the host JDK. Pulling the base image from `docker.intuit.com` should work, but that's untested until Phase 5.
- `podman compose` delegates to Rancher Desktop's `docker-compose` v5 binary, pointed at the Podman socket. It works,
  but it prints a banner on every call.

### Next step
Phase 2c: the `e2e-tests` module boots order, payment and inventory in one JVM (separate contexts and DBs) against
one Testcontainers Kafka + Postgres. `HappyPathE2EIT` places an order over HTTP and asserts the final state of all three
DBs. Plus `requests.http`.
