# ADR-0003: Saga state persistence and stuck-saga detection

- Status: Accepted
- Date: 2026-11-20

## Context

The orchestrator has to remember where each saga is. If order-service crashes or restarts,
in-flight sagas must resume, not vanish. We also have to notice sagas that stop making progress.

## Decision

The `saga_instance` table in `order_db` holds:

| Column | Meaning |
|---|---|
| `saga_id` (PK), `order_id` | Identity |
| `state` | `PAYMENT_PENDING`, `INVENTORY_PENDING`, `COMPENSATING`, `COMPLETED`, `FAILED`, `COMPENSATED`, `COMPENSATION_FAILED` |
| `current_step`, `step_attempts` | What we're waiting for, and how many times we've sent it |
| `compensations_pending` (jsonb) | Which compensations still need an ack |
| `deadline_at` | When the current step counts as stuck (ADR-0009) |
| `version` | Optimistic lock: two concurrent transitions can't both win |
| `created_at`, `updated_at` | Audit |

Every transition, together with the order status change and the outbox row for the next command,
is written in **one local transaction**. A crash therefore leaves one of two states: the old state
with no command, or the new state with the command queued.

An append-only `saga_transition(saga_id, from_state, to_state, trigger, at)` table records history
for debugging and for the README's trace demo.

**Stuck detection:** `SagaTimeoutSweeper` runs every `saga.sweep-interval` (2s) and selects due
sagas with:

```sql
SELECT * FROM saga_instance
WHERE state IN ('PAYMENT_PENDING','INVENTORY_PENDING','COMPENSATING')
  AND deadline_at < :now
ORDER BY deadline_at
LIMIT 50
FOR UPDATE SKIP LOCKED;
```

`SKIP LOCKED` means a saga that is being handled by a reply listener right now is skipped rather
than blocked. Time comes from an injected `java.time.Clock`, so tests control it.

## Consequences

- The saga survives restarts, so it resumes from the database.
- Each transition costs one DB write. That's fine for a learning repo.
- State is a snapshot, not a full history. History lives in `saga_transition` for humans only.

## Alternatives rejected

- **In-memory state:** a crash loses every in-flight saga, which is exactly the failure we're demonstrating.
- **Event-sourced saga** (rebuild state by replaying a saga event log): more instructive about
  history, but it roughly doubles the orchestrator code.
- **Kafka Streams state store:** a framework abstraction that hides the mechanics this repo exists to show.
