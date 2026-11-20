# ADR-0005: Inbox and idempotent consumers

- Status: Accepted
- Date: 2026-11-20

## Context

With at-least-once delivery (ADR-0004, ADR-0006), every consumer will sometimes see the same
message twice. There are two sources of duplicates:

1. **Transport duplicates:** the same record, with the same `eventId`, is delivered again. This
   happens when a relay re-sends or a consumer crashes before committing its offset.
2. **Logical retries:** the orchestrator's sweeper re-sends a command with a **new** `eventId`
   because the reply didn't arrive in time (ADR-0009).

Without protection, a duplicate `ChargePayment` charges twice and a duplicate `ReserveStock`
decrements stock twice. Naive mode demonstrates both (failure mode F3).

## Decision

Use two layers. Both are checked inside the consumer's local DB transaction.

1. **Inbox (transport dedup):**
   `inbox(event_id UUID PK, consumer VARCHAR, received_at)`.
   The handler first runs `INSERT … ON CONFLICT DO NOTHING`. If no row was inserted, the message was
   already processed: skip the business logic and commit the offset.
2. **Business idempotency (logical dedup), keyed by `sagaId`:**
   - Payment: `payment.saga_id` is UNIQUE. `ChargePayment` for a saga that already has a payment row
     replies with the **stored** outcome (CHARGED or DECLINED) and doesn't touch the gateway or
     ledger. `RefundPayment` on a REFUNDED payment, or on no payment at all, replies `PaymentRefunded`
     without moving money.
   - Inventory: `reservation.saga_id` is UNIQUE. A repeated `ReserveStock` replies with the stored
     outcome. `ReleaseStock` with no reservation writes a RELEASED **tombstone** (ADR-0008).
   - Orchestrator: replies go through the inbox, and the state machine also ignores a reply that
     doesn't fit the current state.

**Where it lives:** `IdempotencyGuard` in each service (`firstDelivery(envelope)` and
`checkBusinessKeys()`), called first thing inside the handler's transaction. In reliable mode it's the
`Inbox` implementation (`INSERT … ON CONFLICT (event_id) DO NOTHING`, true if one row was inserted).
In naive mode it's `TrustEveryDelivery` (always true / false).

**Measured (Phase 4b, `ReliableModeE2EIT` vs `NaiveModeE2EIT`):**

| Scenario | Naive | Reliable |
|---|---|---|
| F3a: same ChargePayment + ReserveStock records redelivered | `DOUBLE_CHARGE chargeRows=2 net=22.00`, `WRONG_CHARGE net=22.00 amount=11.00`, `DOUBLE_RESERVATION held=2 ordered=1` | 0 violations. Inbox: "duplicate delivery of ChargePayment … skipped" (and ReserveStock) |
| F3b: same commands, new eventIds | the same three violations (net=24.00 for a 12.00 order) | 0 violations. "charge for this saga already CHARGED: replying with the stored outcome" (and for stock) |

**Offsets:** `enable.auto.commit=false` and `AckMode.RECORD`. The listener runs inside the DB
transaction, and the offset is committed after the DB commit. A crash in between causes a
redelivery, which the inbox absorbs.

## Consequences

- Replaying any message any number of times leaves the same final state. The e2e suite asserts
  this.
- The inbox grows without bound. In production you'd add a TTL or purge, sized beyond the longest
  possible redelivery window.
- The reply to a duplicate isn't sent again from the inbox path. The original reply is already in
  the outbox and will be delivered. Logical retries (new eventId) do produce a fresh reply,
  carrying the stored outcome.

## Alternatives rejected

- **Inbox only:** it misses logical retries, which carry a new eventId.
- **Business key only:** it works, but every handler then has to reason about every duplicate. The inbox
  gives a cheap, uniform first line of defense, and it's the pattern being taught.
- **Kafka exactly-once:** see ADR-0006.
