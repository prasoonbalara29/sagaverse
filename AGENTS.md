# AGENTS.md: rules for anyone (human or AI) changing this repo

Sagaverse is a **learning repo** for the orchestrated saga pattern. It shows an
order → payment → inventory flow over Kafka, built by hand. Clarity and
demonstrable correctness matter more than production completeness. Read
`README.md`, `PROGRESS.md` and `docs/adr/` before changing anything.

## Layout

| Module | Purpose |
|---|---|
| `pom.xml` | Parent POM: dependency management, plugins, enforcer, formatting, static analysis |
| `events/` | Message **contracts only**: envelope record, payload records, topic and type constants. No logic and no Spring. |
| `order-service/` | REST entry point and saga orchestrator. Owns `order_db`. |
| `payment-service/` | Fake payment gateway. Owns `payment_db`. |
| `inventory-service/` | Stock reservations. Owns `inventory_db`. |
| `e2e-tests/` | Boots all three services in one JVM against Testcontainers Kafka and Postgres, then drives every failure scenario. |
| `compose.yaml`, `Makefile`, `scripts/` | Local runtime on Podman |
| `docs/adr/` | Architecture Decision Records. Every non-obvious choice gets one. |

## Hard rules

1. **No shared domain code.** Services share only the `events` module. If two services need the
   same helper (outbox, inbox, MDC), each keeps its own copy. That duplication is deliberate
   (ADR-0001).
2. **No cross-database access.** A service connects only to its own database, with its own user.
3. **Kafka only between services.** No HTTP calls from one service to another.
4. **Every topic is keyed by `orderId`** (ADR-0002).
5. **Every state change and its outgoing message happen in one local DB transaction**, through the
   outbox in `reliable` mode (ADR-0004). `reliable` is the default `saga.mode`. The `naive` mode is kept on purpose so the bugs stay
   reproducible. Don't "fix" it.
6. **Consumers must be idempotent.** Replaying any message twice must not change the outcome (ADR-0005).
7. **No new dependencies without the owner's approval.** Approved so far: Spring Boot 3.x (web, actuator,
   validation, data-jdbc/jpa, kafka), PostgreSQL driver, Flyway, JUnit 5, AssertJ, Mockito,
   Testcontainers, Awaitility, Spotless (google-java-format), Error Prone, Jib.
8. **Non-goals stay out** unless the owner explicitly approves them: auth, UI, cloud/K8s, Schema Registry, saga
   frameworks, tracing stacks, Kafka EOS transactions, load testing. See README.

## JDK selection

- Java **21**. The Maven enforcer rejects any other major version.
- Never pick a JDK by version alone (for example `java_home -v 21`). Set `JAVA_HOME` explicitly to
  the intended JDK's home:
  `export JAVA_HOME=<path-to-intended-jdk>/Contents/Home`
  The Makefile also reads an untracked `.java-home` file containing that path.
- Check the result with `"$JAVA_HOME/bin/java" -version` and `cat "$JAVA_HOME/release"` (`IMPLEMENTOR`, `JAVA_VERSION`).
- Don't commit machine-specific JDK paths.

## Containers

- **Podman only.** Use fully qualified image names from the internal mirror:
  `docker.intuit.com/docker-rmt/...`. Public `docker.io` is blocked (ADR-0011).
- Testcontainers must target the Podman socket. The Makefile exports `DOCKER_HOST` from
  `podman machine inspect`. Don't let tools fall back to another Docker-compatible runtime that may
  also be installed.

## Build and test

```bash
make up-all     # full stack in Podman (images via Jib); make demo / chaos-inventory / trace / down
make verify     # spotless check + compile (Error Prone) + unit + IT + e2e
make test       # unit tests only
./mvnw -pl order-service spring-boot:run   # dev mode, after `make up`
```

- Unit tests end in `*Test` (Surefire). Integration tests end in `*IT` (Failsafe) and use
  Testcontainers, not mocks.
- Never use `Thread.sleep` in tests. Use Awaitility.
- AssertJ for assertions, Mockito only in unit tests.
- Config goes through `@ConfigurationProperties` and profiles. Keep secrets out of git. Local credentials
  in `compose.yaml` are dev-only defaults.

## Logging

Use structured JSON (Spring Boot ECS format). Every line produced while handling a message, request,
relay batch or sweep must carry `sagaId` and `orderId` in MDC.

## Working agreement

- Work in phases (see `PROGRESS.md`). Each phase ends with a green `./mvnw verify`, an updated
  `PROGRESS.md` and a commit. Split any phase that touches more than about 15 files.
- **Prove the problem first.** Write a failing test against naive mode, then fix it, then show it passing.
- **Predict, then run.** Before an e2e run, write down the expected event sequence and the final DB state.
- Paste real command output. Never claim something works without running it.
- Edit scripts must fail loudly when a replacement doesn't match. Spotless reformats code, and silent
  no-op edits caused two wrong runs in this repo's history (see PROGRESS.md, 4a/4b).
