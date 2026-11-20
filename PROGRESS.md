# PROGRESS

## Current phase: 1a (build and contracts) — done

### Phase checklist

| Phase | Scope | Status |
|---|---|---|
| 0 | git init, AGENTS.md, README skeleton, PROGRESS.md, ADRs 0001–0011 | ✅ done (reviewed and approved) |
| 1a | `./mvnw`, parent POM (enforcer, Spotless, Error Prone, Jib), `events` module + `EnvelopeTest` | ✅ done |
| 1b | `compose.yaml`, Makefile, 3 service skeletons, Flyway V1, actuator health, `*ApplicationIT` | ⏳ next |
| 2 | Happy path end to end (naive mode), `.http` file, `HappyPathE2EIT` | pending |
| 3 | Declines and compensations (F1, F2, F7 naive) | pending |
| 4 | Outbox, inbox, idempotency (F3, F4: red on naive first, then green on reliable) | pending |
| 5 | Sweeper, timeouts, crash recovery (F5, F6, F8, F9), Jib images, `make up-all`, `make chaos-inventory` | pending |
| 6 | Observability polish, `make trace`, full README, final summary + quiz | pending |

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
- The Podman machine is **stopped**. Phase 1b needs `podman machine start`, for Testcontainers and compose.
- Rancher Desktop's `docker`/`docker-compose` are on PATH. 1b will pin `DOCKER_HOST` to the Podman socket and
  choose the compose provider explicitly.
- `docker.io/apache/kafka` patch tag to pin in 1b. The plan is 3.9.x to match the Boot-managed clients (3.9.2).

### Next step
Phase 1b: `compose.yaml`, Makefile, and the three service skeletons with Flyway V1, actuator and `*ApplicationIT`.
