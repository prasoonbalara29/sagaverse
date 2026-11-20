# ADR-0002: Topic design, keys and per-order ordering

- Status: Accepted
- Date: 2026-11-20

## Context

Commands and their compensations for the same order must be processed in the order they were
sent. A `RefundPayment` handled before its `ChargePayment` would be a no-op, and the charge would
then go through unrefunded. Kafka only guarantees order **within a partition**.

## Decision

| Topic | Messages | Producer | Consumer |
|---|---|---|---|
| `payment.commands` | ChargePayment, RefundPayment | order | payment |
| `payment.replies` | PaymentCharged, PaymentDeclined, PaymentRefunded | payment | order |
| `inventory.commands` | ReserveStock, ReleaseStock | order | inventory |
| `inventory.replies` | StockReserved, StockUnavailable, StockReleased | inventory | order |
| `<each>.DLT` | Poison records (ADR-0007) | error handler | humans / replay |

- **Partitions:** 3 per topic, replication factor 1 (single broker). Three partitions are enough to
  show that ordering holds per key even when the topic isn't globally ordered.
- **Key:** `orderId` on every record. The default partitioner hashes the key, so every message
  for one order lands on the same partition and is consumed in produce order.
- **Produce order equals commit order.** Each service runs exactly one outbox relay, which publishes rows
  in `id` order and synchronously, so a later row is never sent before an earlier one (ADR-0004).
- **Creation:** a `topics-init` compose service runs `kafka-topics.sh --create --if-not-exists`.
  Broker auto-create is off, so a typo in a topic name fails loudly instead of creating a stray topic.
- **Commands and replies are separate topics.** Consumers read only what they handle, and the
  direction of every message is obvious from its topic name.

## Consequences

- Ordering is guaranteed only per order, and that's all we need.
- Adding partitions later remaps keys, which is unsafe while sagas are in flight. That's out of
  scope here, but noted.
- Running several relay instances would need per-key locking to keep ordering. We run one per
  service.

## Alternatives rejected

- **One topic per message type** (e.g. `payment.charge`, `payment.refund`): charge and refund
  would be on different topics with no relative ordering.
- **A single `saga` topic for everything:** every service would read and filter all traffic, which
  couples them, and the DLT would mix unrelated failures.
- **Key by sagaId:** equivalent today (one saga per order), but orderId is the business identity,
  and it stays correct if an order is ever retried with a new saga.
