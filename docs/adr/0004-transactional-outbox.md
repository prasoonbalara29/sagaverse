# ADR-0004: Transactional outbox on the producer side

- Status: Accepted
- Date: 2026-11-20

## Context: why a plain dual write is unsafe

A service handling a step must do two things: change its own database and send a message to
Kafka. The database and Kafka don't share a transaction, so whatever order you pick, one crash can
leave them disagreeing:

| Order | Crash point | Result |
|---|---|---|
| `db.commit()` then `kafka.send()` | after commit, before send | Order is PENDING, but no ChargePayment is ever sent. **Lost event; saga stuck forever.** |
| `kafka.send()` then `db.commit()` | after send, commit fails or rolls back | ChargePayment is sent for an order that doesn't exist. **Ghost command, real money taken.** |

The naive mode (ADR-0010) implements the first order: `DirectKafkaPublisher` sends from an after-commit
callback. `FaultInjector` (`fault.crash-before-publish`) throws right there to reproduce the lost event
(F4). In reliable mode the same hook sits in the relay before the Kafka send.

## Decision

Each service writes outgoing messages to its own `outbox` table **inside the same local
transaction** as its business change:

```
outbox(id BIGSERIAL PK, aggregate_id, topic, msg_key, event_id, payload JSONB,
       created_at, published_at NULL)
```

The `OutboxRelay` (`@Scheduled`, every `messaging.outbox.poll-interval-ms` = 200):
1. `SELECT … WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED`
2. For each row, in order: `kafkaTemplate.send(topic, key, payload).get()`. The send is
   synchronous, with `acks=all` and an idempotent producer.
3. Set `published_at = now()` for that row. On the first failed send, stop the batch (later rows must
   not overtake it) and commit what succeeded. The rest is retried on the next tick.

The payload column is JSONB, so Postgres normalizes key order and whitespace. Consumers parse JSON by
field name, so that's harmless, and numeric precision is preserved (`42.50` stays `42.50`).

If the relay crashes after step 2 but before step 3, the row is sent again on the next run, so
delivery is **at-least-once**. That's safe only because every consumer dedupes (ADR-0005).
Published rows are kept, which helps when learning and debugging. A cleanup job is listed as a
production to-do.

## Consequences

- Business change and intent-to-send are atomic, so no event is lost and no ghost command is sent.
- Up to about 200 ms of extra latency per hop.
- Duplicates are possible, and the consumer side must handle them.
- The relay is a single instance per service. Running more would need per-key ordering protection.

## Alternatives rejected

- **Kafka transactions / exactly-once:** they don't include the Postgres write, so the dual write
  remains. They're also a stated non-goal (ADR-0006).
- **CDC with Debezium tailing the outbox:** lower latency and no polling, but it needs Kafka
  Connect and Debezium infrastructure. That's the production answer, noted in the README.
- **`@TransactionalEventListener(AFTER_COMMIT)` sending to Kafka:** the send still happens after
  the commit, outside it, so the same crash window remains.
