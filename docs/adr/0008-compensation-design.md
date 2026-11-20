# ADR-0008: Compensation design, and what happens when a compensation fails

- Status: Accepted
- Date: 2026-11-20

## Context

A saga has no rollback. To undo a completed step, we run a **compensating action**: a new forward
action that semantically reverses the earlier one. Compensations run in a messy world. The original
step may or may not have happened (timeouts), a compensation may arrive before the step it undoes,
and a compensation itself can fail.

## Decision

| Forward step | Compensation | Idempotent and order-safe because |
|---|---|---|
| ChargePayment | RefundPayment | Looks up `payment` by sagaId (row lock). CHARGED → refund, write a negative `ledger` entry, mark REFUNDED. Already REFUNDED or DECLINED → reply `PaymentRefunded` without moving money. Missing → insert a **REFUNDED tombstone** (no amount) and reply `PaymentRefunded(paymentId=null)`. |
| ReserveStock | ReleaseStock | Looks up `reservation` by sagaId. RESERVED → add the quantity back, mark RELEASED. Missing → insert a **RELEASED tombstone**. Already RELEASED → no-op. |

**Tombstone:** if a late `ReserveStock` arrives after `ReleaseStock` for the same saga, it finds the
RELEASED row and replies `StockUnavailable(reason=SAGA_CANCELLED)` without touching stock. The same
idea protects payment: a `ChargePayment` that finds a REFUNDED row (a tombstone or a real refund) replies
`PaymentDeclined(reason=SAGA_CANCELLED)` without charging.

When can a compensation overtake its forward step? Normally it can't. Both travel on the same topic with
the same key (ADR-0002), so Kafka delivers the step first. Tombstones cover the exceptions: a forward step
that went to the DLT and is **replayed** later, or a resend that was still in flight. The cost is one row
per cancelled saga. (Migrations `V2__*_tombstone.sql` relax NOT NULL on the columns a tombstone can't fill.)

**Compensations are retryable, never declinable.** A refund of a real charge has to succeed
eventually, so the fake gateway can fail a refund only transiently (`payment.refund-fail-times`:
the first N refunds throw a retryable `GatewayUnavailableException`).

**What we compensate:**
- `StockUnavailable` → RefundPayment.
- A step timeout with attempts exhausted → compensate **everything that may have happened**:
  RefundPayment, plus ReleaseStock if a ReserveStock was ever sent. We can't know whether a timed-out
  step ran, and idempotent no-ops make over-compensating safe.
- A late success after cancellation (e.g. `StockReserved` while COMPENSATING or COMPENSATED) → send
  ReleaseStock, unless a ReleaseStock is already pending. It travels behind the reservation on the
  same partition, so it will undo it. A terminal saga stays terminal, and only the release goes out.
  A COMPENSATING saga also adds ReleaseStock to its pending list.

**When a compensation fails:**
1. Kafka blocking retries in the consumer (ADR-0007).
2. If retries run out, the record goes to the DLT, and no reply arrives. The COMPENSATING saga's deadline
   expires, and the sweeper re-sends the outstanding compensation with a new eventId, deduped by sagaId.
   It does this up to `saga.max-compensation-attempts` (10).
3. If that's exhausted too, the saga goes to **COMPENSATION_FAILED** (terminal), the order goes to
   **CANCELLED_NEEDS_ATTENTION**, and an ERROR line is logged with sagaId, orderId and the
   outstanding compensations. Automation stops here on purpose. If retries haven't fixed it, the
   cause is a bug or an outage that needs a human, and retrying forever would hide it. In
   production, this state would page someone and appear on a dashboard.

## Consequences

- Every compensation handler must be idempotent and handle "nothing to undo".
- Over-compensation sends extra messages, which are harmless no-ops.
- There's an explicit, visible terminal state for unrecoverable cases instead of silent inconsistency.

## Alternatives rejected

- **Compensate only steps we know succeeded:** after a timeout we don't know, and guessing wrong
  leaves money taken or stock held.
- **Let compensations reply "failed" and retry from the orchestrator immediately:** this duplicates the
  Kafka retry layer and tight-loops during an outage. Deadline-driven re-sends give natural backoff.
- **Retry compensations forever:** this hides real bugs and gives no signal to a human.
