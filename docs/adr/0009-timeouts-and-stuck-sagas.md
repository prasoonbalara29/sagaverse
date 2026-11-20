# ADR-0009: Timeouts, and what "stuck" means

- Status: Accepted
- Date: 2026-11-20

## Context

Messages are never lost in reliable mode, but they can be **delayed**. A service may be down,
restarting, slow, or its command may be sitting in a DLT. The orchestrator has to decide when to
stop waiting, and what to do then.

## Decision

**Definition: a saga is stuck when its state is non-terminal and `deadline_at < now()`.** In other
words, the reply for its current step hasn't arrived in time.

Each transition into a waiting state sets `deadline_at = now + saga.step-timeout`.

On each sweep (ADR-0003), for every stuck saga:

| State | attempts < max | attempts exhausted |
|---|---|---|
| PAYMENT_PENDING | Re-send ChargePayment (new eventId), `attempts++`, push the deadline | → COMPENSATING: RefundPayment |
| INVENTORY_PENDING | Re-send ReserveStock, `attempts++`, push the deadline | → COMPENSATING: ReleaseStock + RefundPayment |
| COMPENSATING | Re-send outstanding compensations | After `max-compensation-attempts` → COMPENSATION_FAILED |

Defaults (`@ConfigurationProperties("saga")`):

| Property | Default | e2e tests (`*ModeE2EIT`) |
|---|---|---|
| `step-timeout` | 10s | 2s |
| `max-step-attempts` | 6 (about 60s total, enough to restart a container in the chaos demo) | 3 |
| `max-compensation-attempts` | 10 | 5 |
| `sweep-interval` | 2s | 500ms |

Re-sending is safe because commands are idempotent by sagaId (ADR-0005).

**Implementation (Phase 5a):** `SagaStateMachine.onTimeout` (pure, unit-tested) decides, and
`SagaOrchestrator.handleTimeouts` applies the decisions inside one transaction over
`SagaRepository.lockDue` (`… FOR UPDATE SKIP LOCKED`). `SagaTimeoutSweeper` (reliable mode only)
schedules that every `saga.sweep-interval`. `step_attempts` counts sends of the current step, and in
COMPENSATING it counts compensation rounds. The `compensation_attempts` column from V1 is therefore
unused, and is kept to avoid a migration that only drops it.

**Measured, F6** (inventory down; e2e config: 2 s step timeout, 3 attempts, 500 ms sweep):
```
17:50:24.983 PAYMENT_PENDING -> INVENTORY_PENDING on PaymentCharged (attempt 1)
17:50:27.123 INVENTORY_PENDING -> INVENTORY_PENDING on Timeout (attempt 2)
17:50:29.154 INVENTORY_PENDING -> INVENTORY_PENDING on Timeout (attempt 3)
17:50:31.178 INVENTORY_PENDING -> COMPENSATING on Timeout, awaiting [ReleaseStock, RefundPayment]
17:50:31.540 COMPENSATING -> COMPENSATING on PaymentRefunded, awaiting [ReleaseStock]
   (inventory restarts: reserves once, answers 2 resends from the stored outcome, then releases)
17:50:31.949 COMPENSATING -> COMPENSATED on StockReleased
```

## Consequences

- A slow but healthy service just causes an extra (deduped) command.
- The timeouts set the trade-off between how fast we give up and how many false timeouts we get. 60s is tuned for the demo.
- A saga that has given up compensates, even if the original step later succeeds. The late-success
  rule in ADR-0008 cleans that up.

## Alternatives rejected

- **A single overall saga deadline:** you can't tell which step hung, and there's no chance to re-send.
- **Delayed or scheduled messages on Kafka:** Kafka has no native delay, so you'd build a scheduler
  on top. That's more moving parts than one indexed SQL query.
- **No timeouts, waiting forever:** naive mode does this, and it's failure mode F6.
