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

| Property | Default | Test profile |
|---|---|---|
| `step-timeout` | 10s | 500ms |
| `max-step-attempts` | 6 (about 60s total, enough to restart a container in the chaos demo) | 3 |
| `max-compensation-attempts` | 10 | 3 |
| `sweep-interval` | 2s | 100ms |

Re-sending is safe because commands are idempotent by sagaId (ADR-0005).

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
