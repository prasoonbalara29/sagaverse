# ADR-0001: Service boundaries, state ownership and orchestration

- Status: Accepted
- Date: 2026-11-20

## Context

Placing an order touches three concerns: the order itself, taking money, and reserving stock.
Each one lives in its own service with its own database, so no single ACID transaction can cover
all three. We need to decide two things: which service owns which data, and who coordinates the
steps.

## Decision

**State ownership** (each table lives in exactly one database):

| Service | Database | Tables | Notes |
|---|---|---|---|
| order-service | `order_db` | `orders`, `saga_instance`, `saga_transition`, `outbox`, `inbox` | Only REST entry point; hosts the orchestrator |
| payment-service | `payment_db` | `payment` (1 row per sagaId), `ledger` (append-only money movements), `outbox`, `inbox` | Fake gateway: `payment.fail-rate`, `payment.decline-amount-over`, `payment.refund-fail-rate` |
| inventory-service | `inventory_db` | `stock(sku, available)`, `reservation` (1 row per sagaId), `outbox`, `inbox` | `inventory.out-of-stock-skus`, `inventory.processing-delay` |

**Coordination:** use **orchestration**. A `SagaOrchestrator` inside order-service holds the
state machine. It sends commands (`ChargePayment`, `ReserveStock`, and the compensations
`RefundPayment` and `ReleaseStock`) and reacts to replies. Payment and inventory don't know a
saga exists. They just execute commands idempotently and reply.

**No shared code** beyond the `events` contracts module. Each service keeps its own copy of
infrastructure such as the outbox, inbox and MDC helpers. Copying is cheaper than coupling:
a shared library would force every service to release together.

## Consequences

- Order state and saga state sit in the same database, so a transition and its order status
  change commit atomically.
- order-service has two jobs (order API + orchestrator). That's acceptable at this size.
- The saga's logic is in one class, so you can follow the whole flow in one place. That is the
  main learning benefit.
- The payment and inventory services can be tested in isolation: send a command, assert the reply.

## Alternatives rejected

- **Standalone orchestrator service.** It's cleaner separation at scale, but it adds a 4th
  process, and the orchestrator would then have to keep order status in sync through yet more
  messages.
- **Choreography** (services react to each other's events). There is no central place that
  describes the flow, and compensation logic gets spread across services. It's harder to follow,
  and the brief asks for orchestration.
- **A shared "common" library for the outbox and inbox.** Rejected for the coupling reason above.
