# PROGRESS

## Current phase: 0 (scaffold and decisions) — done, awaiting owner review

### Phase checklist

| Phase | Scope | Status |
|---|---|---|
| 0 | git init, AGENTS.md, README skeleton, PROGRESS.md, ADRs 0001–0011 | ✅ done, awaiting review |
| 1a | `./mvnw`, parent POM (enforcer, Spotless, Error Prone, Jib), `events` module + `EnvelopeTest` | ⏳ next |
| 1b | `compose.yaml`, Makefile, 3 service skeletons, Flyway V1, actuator health, `*ApplicationIT` | pending |
| 2 | Happy path end to end (naive mode), `.http` file, `HappyPathE2EIT` | pending |
| 3 | Declines and compensations (F1, F2, F7 naive) | pending |
| 4 | Outbox, inbox, idempotency (F3, F4: red on naive first, then green on reliable) | pending |
| 5 | Sweeper, timeouts, crash recovery (F5, F6, F8, F9), Jib images, `make up-all`, `make chaos-inventory` | pending |
| 6 | Observability polish, `make trace`, full README, final summary + quiz | pending |

### Done in Phase 0
- Initialized the git repo (`main`) and added `.gitignore`.
- `AGENTS.md`: hard rules, JDK selection rule, Podman rule, test conventions.
- `README.md` skeleton: problem, architecture, sequence and state diagrams, message table, ADR index.
- ADRs 0001–0011, covering every decision approved in the plan.

### Decisions made (approved plan)
- Orchestration lives in order-service. Payment runs before inventory.
- Four topics plus DLTs, 3 partitions, keyed by orderId.
- Saga state is in `saga_instance` (Postgres), with a sweeper using `FOR UPDATE SKIP LOCKED`.
- Outbox plus a two-layer inbox (eventId, then sagaId). At-least-once delivery, no Kafka EOS.
- Blocking retries, then DLT. Non-retryable errors go straight to the DLT.
- Idempotent compensations with a RELEASED tombstone. The terminal COMPENSATION_FAILED state needs a human.
- Step timeout 10s × 6 attempts. "Stuck" = non-terminal and `deadline_at < now`.
- `saga.mode=naive|reliable` flag. Naive tests assert that the violation occurs.
- No SDKMAN or `.sdkmanrc`. The owner sets the JDK in IntelliJ. Builds use an explicit `JAVA_HOME`.
- Approved extra dependencies: Testcontainers, Awaitility, Spotless, Error Prone, Jib.

### Open issues
- The Podman machine `podman-machine-default` is **stopped**. It must be started (`podman machine start`)
  before Phase 1b tests and compose can run.
- Rancher Desktop's `docker`/`docker-compose` are on PATH. The Makefile must pin `DOCKER_HOST` to the
  Podman socket, and use `podman compose` with an explicit provider, or `podman-compose` if it's installed
  later. To be settled in Phase 1b.
- The exact Spring Boot 3.5.x and Kafka 3.9.x patch versions will be pinned in Phase 1a.

### Next step
Owner reviews Phase 0 docs. On approval, start Phase 1a.
