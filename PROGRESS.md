# PROGRESS

## Current phase: 5a (timeout sweeper) — done. Next: 5b (F8 failing refunds, F9 late reply)

### Phase checklist

| Phase | Scope | Status |
|---|---|---|
| 0 | git init, AGENTS.md, README skeleton, PROGRESS.md, ADRs 0001–0011 | ✅ done |
| 1a | `./mvnw`, parent POM (enforcer, Spotless, Error Prone, Jib), `events` module + `EnvelopeTest` | ✅ done |
| 1b | `compose.yaml` (Postgres + 3 DBs/users, Kafka KRaft, topics-init), Makefile | ✅ done |
| 1c | 3 service skeletons, Flyway V1, actuator health, `*ApplicationIT` (Testcontainers) | ✅ done |
| 2a | Order API, orchestrator state machine (happy path), naive publisher, reply listener, `OrderSagaHappyPathIT` | ✅ done |
| 2b | Payment + inventory command handlers (naive), per-service command ITs | ✅ done |
| 2c | Per-service config name + migration folder, so all three jars can share one classpath | ✅ done |
| 2d | `e2e-tests` module booting all three apps, `HappyPathE2EIT`, `requests.http` | ✅ done |
| 3a | Payment decline rule + RefundPayment, inventory ReleaseStock, tombstones | ✅ done |
| 3b | F1/F2 e2e red first, then orchestrator FAILED / COMPENSATING / COMPENSATED | ✅ done |
| 3c | Retry + DLT wiring, poison-message test (F7) | ✅ done |
| 4a | Transactional outbox + relay in all services, FaultInjector, InvariantChecker, F4 naive vs reliable | ✅ done |
| 4b | Inbox + sagaId business idempotency, F3 naive vs reliable, default mode → reliable | ✅ done |
| 5a | Timeout sweeper + state machine timeouts + late-reservation rule, F5/F6 naive vs reliable | ✅ done |
| 5b | F8 (refund fails transiently / forever → COMPENSATION_FAILED), F9 (late reservation after cancellation) | ⏳ next |
| 5c | Jib images, `make up-all`, `make chaos-inventory`, `make demo` | pending |
| 6 | Observability polish, `make trace`, full README, final summary + quiz | pending |

1b was split from the original plan: infra in 1b, services in 1c. Together they'd have been about 20 files.

### Done in Phase 5a (timeouts)
- **`SagaStateMachine.onTimeout`** (pure):
  - PAYMENT_PENDING / INVENTORY_PENDING resend the step with a new deadline, up to `max-step-attempts`.
  - Then compensate everything that may have happened: Refund for payment, Release + Refund for inventory.
  - COMPENSATING resends the outstanding compensations up to `max-compensation-attempts`, then goes to
    **COMPENSATION_FAILED** (order `CANCELLED_NEEDS_ATTENTION`, ERROR log).
  - Terminal sagas ignore timeouts.
- **Late reservation (F9 rule):** `StockReserved` after cancellation sends `ReleaseStock`, unless one is already pending.
- `Decision` now carries `stepAttempts` explicitly (previously it was inferred in the orchestrator).
- **`SagaOrchestrator`** has one `apply()` path for replies and timeouts. `handleTimeouts(limit)` runs over
  `SagaRepository.lockDue` (`FOR UPDATE SKIP LOCKED`). Decisions log as `saga X -> Y on T (attempt n)`.
- **`SagaTimeoutSweeper`** (reliable mode only): a `SchedulingConfigurer` with `addFixedDelayTask(…, saga.sweep-interval)`.
- `InvariantChecker.STUCK_SAGA` now needs the deadline to be **more than 1 s** past. That's the grace for the 500 ms
  sweeper, because otherwise a healthy reliable saga caught between its deadline and the next sweep would be flagged.
- `SagaStateMachineTest`: +12 tests (25 total) for timeouts, exhaustion, compensation retries, COMPENSATION_FAILED,
  terminal sagas, and the late-reservation cases.
- The e2e mode tests now run order-service with a 2 s step timeout, 3 attempts, 20 compensation attempts and a 500 ms sweep.

**Scenarios.**
- **F5:** inventory is stopped and an order placed. The test waits 3 s (longer than one step timeout), restarts
  inventory, and lets the saga settle.
- **F6:** inventory is stopped and an order placed. The test waits until only `ReleaseStock` is pending (reliable)
  or 15 s pass (naive), takes a snapshot, then restarts inventory.

*An honest correction to the plan:* the plan's table said naive F5 "hangs". It doesn't. Stopping a consumer leaves the
command in Kafka, so naive mode recovers once inventory returns. It just never resends. Naive's real crash danger is the
commit-then-send window, which is F4. The naive tests assert exactly this.

**Step 1: prove the problem** (reliable assertions run in naive mode). Real output:
```
f5: [sweeper resent ReserveStock] Expecting actual: 1 to be greater than: 1
f6: Expecting empty but was: [STUCK_SAGA saga=37cdb147… state=INVENTORY_PENDING order=PENDING deadline=…17:47:21Z]
[ERROR] Tests run: 7, Failures: 5   (f3a, f3b, f4 as before; f1, f2 pass)
```
**Step 2: implement** (above). **Step 3: green.** Prediction: F5 attempts ≥ 2 then COMPLETED with one reservation. F6 COMPENSATING
with only `[ReleaseStock]` left while inventory is down (no violations), then COMPENSATED with money and stock both net zero.
**Result: matched.** The F6 timeline is in ADR-0009 (attempt 1 → 2 → 3, two seconds apart, then COMPENSATING; refund at once;
when inventory returns, reserve once, two stored-outcome replies, release; three late `StockReserved` ignored; COMPENSATED).
```
$ make verify
Tests run: 25 SagaStateMachineTest | 7 ReliableModeE2EIT | 7 NaiveModeE2EIT | 3 PoisonMessageE2EIT | 2 FailurePathsE2EIT | 1 HappyPathE2EIT | ...
BUILD SUCCESS   (89 tests)
```

### Done in Phase 4b (inbox + business idempotency, F3; reliable becomes the default)
**Scenarios added** (`ScenarioRunner`, run by both `ReliableModeE2EIT` and `NaiveModeE2EIT`):
- F1 and F2, which must be correct in both modes.
- **F3a, redelivery:** after the saga completes, the exact `ChargePayment` and `ReserveStock` records (same bytes, same
  eventId) are found on the topics and sent again.
- **F3b, logical resend:** the same commands with a new eventId (`KafkaTool.withNewEventId`, using exact-decimal Jackson).

The scenarios wait until the `payment-service` / `inventory-service` consumer groups have committed past the end of
their topics (`KafkaTool.awaitConsumed`), so "processed" is measured rather than guessed.

**Step 1: prove the problem** (reliable assertions run in naive mode). Real output:
```
f3a: Expecting empty but was: [DOUBLE_CHARGE saga=d4ccc724… chargeRows=2 net=22.00,
                               WRONG_CHARGE saga=d4ccc724… net=22.00 amount=11.00,
                               DOUBLE_RESERVATION saga=d4ccc724… held=2 ordered=1]
f3b: Expecting empty but was: [DOUBLE_CHARGE … chargeRows=2 net=24.00, WRONG_CHARGE … net=24.00 amount=12.00,
                               DOUBLE_RESERVATION … held=2 ordered=1]
f1, f2: pass (as predicted).   f4: fails (as expected in naive mode).
```
**A mistake of mine, caught by this step:** the first red run also failed F1 (`expected "FAILED" but was "COMPLETED"`).
My edit adding `payment.decline-amount-over=100.00` to both mode tests had silently matched nothing, because Spotless had
reformatted the line, so the 500.00 charge was approved. I fixed it, and the edit scripts now assert that every replacement
matched. The next attempt was blocked by that assertion (a missing import), and the run after that produced the output above.

**Step 2: fix.**
- `IdempotencyGuard` per service. `Inbox` (reliable) does `INSERT … ON CONFLICT (event_id) DO NOTHING` inside the handler's
  transaction. `TrustEveryDelivery` (naive) always processes.
- Called first in `PaymentService.handle`, `InventoryService.handle` and `SagaOrchestrator.onReply`.
- Business keys (reliable): a `ChargePayment` for a saga with a payment row replies with the stored outcome
  (CHARGED → same paymentId and amount; DECLINED → same reason). A `ReserveStock` for a saga with a RESERVED row replies
  `StockReserved` with the stored sku and qty. Tombstones keep working in both modes.
- **Default `saga.mode` is now `reliable`**: every yml, `SagaProperties`, and `matchIfMissing` on the reliable beans.
  `HappyPathE2EIT`, `FailurePathsE2EIT` and `PoisonMessageE2EIT` now run in reliable mode. Naive runs only in `NaiveModeE2EIT`.

**Step 3: green.** Prediction: F3a skipped by the inbox, F3b answered from stored outcomes, 0 violations; naive still shows
the three violations. **Result: matched**, and the log proves the mechanism (ReliableModeE2EIT run):
```
1 x payment   | duplicate delivery of ChargePayment … skipped                                  (F3a)
1 x inventory | duplicate delivery of ReserveStock … skipped                                   (F3a)
1 x payment   | charge for this saga already CHARGED: replying with the stored outcome         (F3b)
1 x inventory | stock already reserved for this saga: replying with the stored outcome         (F3b)
$ make verify
Tests run: 5 ReliableModeE2EIT | 5 NaiveModeE2EIT | 3 PoisonMessageE2EIT | 2 FailurePathsE2EIT | 1 HappyPathE2EIT | ...
BUILD SUCCESS   (73 tests)
```
Docs: ADR-0005 (where the guard lives, plus the measured naive vs reliable table), ADR-0010 (default is reliable), AGENTS.md,
and the README results table (F1 to F4 filled in).

### Done in Phase 4a (transactional outbox, F4 lost event)
**Test harness (ADR-0010):**
- `InvariantChecker` reads all three DBs for a saga and reports `DOUBLE_CHARGE`, `UNREFUNDED_CHARGE`, `WRONG_CHARGE`,
  `DOUBLE_RESERVATION`, `ORPHAN_RESERVATION` and `STUCK_SAGA` (non-terminal with its deadline passed), each with the numbers.
- `ScenarioRunner` drives a scenario identically in both modes.
- `ReliableModeE2EIT` asserts zero violations, and `NaiveModeE2EIT` asserts the specific violation.
- `SagaverseCluster.restart(service)` simulates crash + restart.

**F4 scenario:** order-service runs with `fault.crash-before-publish=true` (the new `FaultInjector`, off by default). The order
is placed, then order-service is stopped and the unpublished outbox rows for the saga are counted. Then it's restarted
without the fault, and the test waits up to 10 s.
- Naive: the fault fires in the after-commit send, so the client gets a 500 after the commit.
- Reliable: the fault fires in the relay, before the Kafka send.

**Step 1: prove the problem.** I ran the reliable assertion (`isEmpty()`) against naive mode. Real output:
```
FAULT INJECTED: simulated crash after commit, before publishing ChargePayment
Expecting empty but was: [STUCK_SAGA saga=7a7b9995… state=PAYMENT_PENDING order=PENDING deadline=2026-11-20T17:22:39Z]
[ERROR] Tests run: 1, Failures: 1
```
**Step 2: fix.** `OutboxPublisher` (reliable) inserts the envelope into `outbox` inside the caller's transaction, and
refuses to run without one. `OutboxRelay` (reliable, `@Scheduled` every `messaging.outbox.poll-interval-ms`=200) runs
`SELECT … WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED`, does a synchronous send, sets
`published_at`, and stops at the first failure to keep the order. Both live in all three services (payment and inventory
replies were dual writes too). order-service's relay calls `FaultInjector.beforePublish`.

**Step 3: same scenario, both modes.** Prediction: reliable completes with zero violations, naive stays PAYMENT_PENDING
with exactly `STUCK_SAGA` and no payment row. **Result: matched.**

**A gap I found and closed:** the first green run logged no `FAULT INJECTED` in reliable mode. The restart came before the
relay's first 200 ms tick, so the "crash" happened through the stop, not the fault hook. It's still a real crash after
commit, but nothing proved the command was unsent. The scenario now counts unpublished outbox rows right after stopping the
service. Reliable asserts **1** (the command survived in the DB), naive asserts **0** (no durable copy exists). Both pass.

Reliable log for the F4 saga after the restart: `order.OutboxRelay relayed ChargePayment` → `payment … charged 7.00` →
`payment.OutboxRelay relayed PaymentCharged` → `… INVENTORY_PENDING` → `order.OutboxRelay relayed ReserveStock` →
`inventory … reserved 1 x SKU-1` → `inventory.OutboxRelay relayed StockReserved` → `… COMPLETED`.
```
$ make verify
Tests run: 1 ReliableModeE2EIT | 1 NaiveModeE2EIT | 3 PoisonMessageE2EIT | 2 FailurePathsE2EIT | 1 HappyPathE2EIT | ...
BUILD SUCCESS   (65 tests)
```

### Done in Phase 3c (retry + dead-letter, F7)
**Step 1: prove the problem.** `PoisonMessageE2EIT` sends garbage to all 3 partitions of `payment.commands`, an unknown
type (`ShipOrder`) to `inventory.commands` and a version-2 envelope to `payment.replies`. It expects each one on its
`.DLT`. Prediction: today the default handler retries 10× with no delay, logs, and skips, so the DLTs stay empty.
Real output:
```
[records with key poison-c15faf80… on payment.commands.DLT]   Expected size: 3 but was: 0
[records with key poison-f819844a… on inventory.commands.DLT] Expected size: 1 but was: 0
[records with key poison-6daa1c4d… on payment.replies.DLT]    Expected size: 1 but was: 0
[ERROR] Tests run: 3, Failures: 0, Errors: 3
logged by the default handler:
"Backoff FixedBackOff{interval=0, currentAttempts=10, maxAttempts=9} exhausted for payment.commands-0@0"  (and -1, -2, inventory, replies)
```
**Step 2: fix.** `KafkaErrorHandling` in each service (a deliberate per-service copy): a `DefaultErrorHandler` bean with
blocking `ExponentialBackOffWithMaxRetries` (1s, 2s, 4s from `messaging.retry.*`), `MessageRejectedException` marked
not-retryable, and a `DeadLetterPublishingRecoverer` to `<topic>.DLT` (same partition) that logs `dead-lettering …`.

**Step 3: same test green.** Prediction: 3 malformed records on `payment.commands.DLT` with cause `MalformedMessageException`,
and a later order on those partitions still CONFIRMED. `ShipOrder` gives `UnknownMessageTypeException`, version 2 gives
`UnsupportedVersionException`. **Result: matched.**
```
dead-lettering payment.commands-0@0: …EnvelopeCodec$MalformedMessageException: not a valid envelope: Unrecognized token 'not' …
dead-lettering payment.commands-1@0: … (same)
dead-lettering payment.commands-2@0: … (same)
dead-lettering inventory.commands-2@0: …EnvelopeCodec$UnknownMessageTypeException: unknown message type: ShipOrder
dead-lettering payment.replies-2@0: …EnvelopeCodec$UnsupportedVersionException: unsupported version 2 of PaymentDeclined
$ make verify
Tests run: 3 PoisonMessageE2EIT | 2 FailurePathsE2EIT | 1 HappyPathE2EIT | ...
BUILD SUCCESS   (63 tests; first reported as 69, which was a miscount, corrected in 4a)
```
ADR-0007 now says "first attempt + 3 retries" (it said "3 attempts"), lists the real exception types and header names, and
records what the default handler does without this config.

**Known gap (by design, until Phase 5):** a poisoned or dead-lettered *command* means its saga never gets a reply. In naive mode
nothing notices. The Phase 5 sweeper turns that into a timeout and compensation (F6/F7 reliable column).

### Done in Phase 3b (orchestrator failure transitions)
**Step 1: prove the problem.** I wrote `FailurePathsE2EIT` (F1, F2) and ran it against the Phase 3a orchestrator, which ignored
`PaymentDeclined` and `StockUnavailable`. Prediction: F1 stuck in PAYMENT_PENDING; F2 stuck in INVENTORY_PENDING with the
customer charged. Real output:
```
[order/saga for 6395d4ce…; payment ledger=[]]
expected: "CANCELLED/FAILED"
 but was: "PENDING/PAYMENT_PENDING" within 20 seconds.

[order/saga for 4c9fbacb…; payment ledger=[{entry_type=CHARGE, amount=10.00}]]
expected: "CANCELLED/COMPENSATED"
 but was: "PENDING/INVENTORY_PENDING" within 20 seconds.
[ERROR] Tests run: 2, Failures: 0, Errors: 2
```
F2's red run shows the exact inconsistency from the brief: **money taken for an order that can't be fulfilled**.

**Step 2: fix.**
- `SagaStateMachine`:
  - `PAYMENT_PENDING + PaymentDeclined → FAILED` (order CANCELLED, nothing to undo).
  - `INVENTORY_PENDING + StockUnavailable → COMPENSATING`, sending `RefundPayment` and recording `compensationsPending=[RefundPayment]`.
  - In COMPENSATING, `PaymentRefunded` / `StockReleased` acknowledge one compensation. The last ack gives
    `COMPENSATED` (order CANCELLED). Otherwise it keeps waiting, with the deadline unchanged.
  - A duplicate ack, or a decline/unavailable in the wrong state, is ignored.
- `SagaInstance.compensationsPending`, persisted as the JSONB `saga_instance.compensations_pending` (`SagaRepository`).
- `SagaStateMachineTest`: the pinned "failure replies not handled yet" test was deliberately replaced by 6 transition tests (13 total).

**Step 3: same tests green.** Predicted: F1 `ChargePayment → PaymentDeclined` gives CANCELLED/FAILED, payment DECLINED, no ledger,
no reservation, stock unchanged. F2 `ChargePayment → PaymentCharged → ReserveStock → StockUnavailable → RefundPayment →
PaymentRefunded` gives CANCELLED/COMPENSATED, ledger `[CHARGE 10.00, REFUND -10.00]`, no reservation, SKU-EMPTY still 0.
**Result: matched.** Logs for the F2 saga, from the run:
```
payment.PaymentService            charged 10.00 to cust-e2e
order.SagaOrchestrator            saga PAYMENT_PENDING -> INVENTORY_PENDING on PaymentCharged
inventory.InventoryService        cannot reserve 1 x SKU-EMPTY: out of stock
order.DirectKafkaPublisher        sent RefundPayment to payment.commands
order.SagaOrchestrator            saga INVENTORY_PENDING -> COMPENSATING on StockUnavailable (awaiting [RefundPayment])
payment.PaymentService            refunded 10.00 to cust-e2e (OUT_OF_STOCK)
order.SagaOrchestrator            saga COMPENSATING -> COMPENSATED on PaymentRefunded
```
(The F2 saga's initial "sent ChargePayment" line is missing from the captured failsafe report, although payment logged
receiving that eventId. It looks like a log-capture artifact. Recheck in Phase 6.)
```
$ make verify
Tests run: 13 SagaStateMachineTest | 2 FailurePathsE2EIT | 1 HappyPathE2EIT | ...
BUILD SUCCESS   (60 tests; first reported as 66, which was a miscount, corrected in 4a)
```
Also: `requests.http` gained F1 (amount 5000 > the dev limit of 1000) and F2 (SKU-EMPTY). The README results table now has F1/F2 (naive).

### Done in Phase 3a (payment + inventory compensation handlers)
- **Fake gateway** (`FakeGateway` + `PaymentProperties`): `payment.decline-amount-over` (default 1000.00, deterministic)
  and `payment.fail-rate` (random, for demos). A decline writes `payment.status = DECLINED` + reason, no ledger row,
  and replies `PaymentDeclined`.
- **RefundPayment** (idempotent in every mode): row lock on `payment`. CHARGED gives a `REFUND -amount` ledger row,
  status REFUNDED, and `PaymentRefunded(paymentId)`. Already refunded or declined: reply without moving money.
  No row: write a REFUNDED tombstone and reply `PaymentRefunded(null)`. A later `ChargePayment` hitting a REFUNDED row
  gives `PaymentDeclined(SAGA_CANCELLED)`.
- **ReleaseStock** (idempotent in every mode): row lock on `reservation`. RESERVED gives stock back, status RELEASED,
  and `StockReleased(true)`. Already RELEASED gives `StockReleased(false)`. No row writes a RELEASED tombstone
  (`sku` NULL, qty 0). A later `ReserveStock` hitting a RELEASED row gives `StockUnavailable(SAGA_CANCELLED)`.
- Migrations: `payment/V2__refund_tombstone.sql` (amount, customer_id nullable) and
  `inventory/V2__release_tombstone.sql` (sku nullable).
- ADR-0008 updated: the payment tombstone, and why a compensation can only overtake its step through DLT replay or an in-flight resend.
- **Deprecation fix**: `JsonNodeFactory.withExactBigDecimals` is deprecated in Jackson 2.21. All three `AppConfig`s and both
  mapper tests now disable `JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES` instead. The 42.50 round-trip tests still pass.
- New ITs: payment +3 (declined, refund-twice-refunds-once, refund-before-charge tombstone), inventory +2
  (release-twice-releases-once, release-before-reserve tombstone).

Predicted, then run (3a): decline gives `AMOUNT_LIMIT_EXCEEDED`, DECLINED, 0 ledger rows. Two refunds give
ledger `[CHARGE 10.00, REFUND -10.00]`. Tombstone then charge gives `SAGA_CANCELLED`, 0 ledger rows. Reserve 4 then release
twice gives `[true, false]`, stock restored. Tombstone then reserve gives `SAGA_CANCELLED`, stock unchanged. **Result: matched.**
```
$ make verify
Tests run: 4 PaymentCommandsIT | 4 InventoryCommandsIT | ... | 1 HappyPathE2EIT
BUILD SUCCESS   (59 tests, no deprecation warnings)
```

### Done in Phase 2d (e2e module)
- `e2e-tests` module (tests only). It depends on the three services' plain jars.
- `SagaverseCluster`: creates `order_db`/`payment_db`/`inventory_db` on one Testcontainers Postgres and all 8 topics on one
  Kafka. It starts each service with `XServiceApplication.application()` in its own context (random port, its own DB,
  `saga.mode` chosen by the test). `stop(service)`/`start(service)` are ready for the Phase 5 crash tests. `db(service)` gives
  direct JDBC access for assertions, which also works while a service is stopped.
- `HappyPathE2EIT`: POST an order over HTTP to the real order-service and wait for CONFIRMED, then assert all three DBs.
- `requests.http`: happy path, watch the saga, 400, 404, and the three health checks. The README links it.
- ADR-0011 now records the one-classpath rules and the in-JVM `service.name` caveat.

### Predict, then run: `HappyPathE2EIT`
Predicted events: ChargePayment (order→payment), PaymentCharged (payment→order), ReserveStock (order→inventory),
StockReserved (inventory→order). Final state: order_db order CONFIRMED / saga COMPLETED / 3 transitions;
payment_db 1 payment CHARGED 19.99 + exactly 1 ledger CHARGE 19.99; inventory_db SKU-2 10→7, reservation RESERVED qty 3.
Flagged risk: JVM-global logging, so `service.name` may be wrong in-JVM.

Result: **matched.** The risk turned out real but small. Logs for one sagaId, from the e2e run:
```
order-service  order.messaging.DirectKafkaPublisher      sent ChargePayment to payment.commands
order-service  order.saga.SagaOrchestrator               order placed, saga started in PAYMENT_PENDING
order-service  payment.messaging.CommandListener         received ChargePayment
order-service  payment.payment.PaymentService            charged 19.99 to cust-e2e
order-service  payment.messaging.DirectKafkaPublisher    sent PaymentCharged to payment.replies
order-service  order.messaging.DirectKafkaPublisher      sent ReserveStock to inventory.commands
order-service  order.saga.SagaOrchestrator               saga PAYMENT_PENDING -> INVENTORY_PENDING on PaymentCharged
order-service  inventory.messaging.CommandListener       received ReserveStock
order-service  inventory.stock.InventoryService          reserved 3 x SKU-2
order-service  inventory.messaging.DirectKafkaPublisher  sent StockReserved to inventory.replies
order-service  order.saga.SagaOrchestrator               saga INVENTORY_PENDING -> COMPLETED on StockReserved
```
(The first column is the ECS `service.name`, the same for every context in one JVM. The logger package identifies the service.)

```
$ make verify
Tests run: 18 EnvelopeTest | 8 SagaStateMachineTest | 5 EnvelopeCodecTest
Tests run: 3 OrderServiceApplicationIT | 3 OrderSagaHappyPathIT
Tests run: 3 PaymentServiceApplicationIT | 1 PaymentCommandsIT
Tests run: 4 InventoryServiceApplicationIT | 2 InventoryCommandsIT
Tests run: 1 HappyPathE2EIT (9.7 s)
BUILD SUCCESS   (54 tests)
```

### Done in Phase 2c (refactor, no behavior change)
**Why:** the e2e module boots all three services in one JVM (ADR-0011), so their jars share one classpath. Each jar had
`application.yml` and `db/migration/V1__init.sql` at the same path. Spring would load only the first `application.yml`,
and Flyway would see three `V1` migrations and refuse to start.
- Each service's config is now `<svc>-service.yml`. `XServiceApplication.CONFIG_NAME` (`spring.config.name=<svc>-service`)
  is applied by `main()` through `XServiceApplication.application()`, and by every `@SpringBootTest(properties = CONFIG_NAME)`.
- Migrations moved to `db/migration/<svc>/`, with `spring.flyway.locations` per service.
- Evidence that the config is really loaded: the "Flyway created exactly this service's tables" ITs still pass. They
  would find no tables if the config were ignored. Dev mode via `main()` still works: inventory came up `UP`, and Flyway
  said "Successfully validated 1 migration" against the compose DB.
- 15 files (3 mains, 3 yml renames, 3 migration moves, 6 ITs) + PROGRESS.md.

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
Phase 5b:
- **F8, failing refunds.** Add `payment.refund-fail-times` (deterministic: the first N refunds throw). With N = 5 the refund
  outlasts Kafka's retries, goes to the DLT, is resent by the sweeper, and the saga still ends COMPENSATED. With N large the
  saga ends COMPENSATION_FAILED / CANCELLED_NEEDS_ATTENTION, and `UNREFUNDED_CHARGE` is reported for a human. Naive mode
  stays stuck in COMPENSATING.
- **F9, late reservation.** An out-of-stock saga is COMPENSATED, stock is restocked, then a ReserveStock resend (new eventId)
  arrives late. It reserves, and the orchestrator must release it, leaving no `ORPHAN_RESERVATION`.

Then 5c: Jib images, `make up-all`, `make chaos-inventory`, `make demo`.
