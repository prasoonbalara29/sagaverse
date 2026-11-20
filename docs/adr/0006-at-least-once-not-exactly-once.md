# ADR-0006: At-least-once delivery with idempotent consumers, not Kafka exactly-once

- Status: Accepted
- Date: 2026-11-20

## Context

Kafka offers "exactly-once semantics" (EOS) through idempotent producers plus transactions, where
consume, produce and commit-offset happen atomically. It's tempting to think this removes the need
for an outbox and an inbox.

## Decision

Don't use Kafka transactions. Use **at-least-once** delivery everywhere, and make consumers
idempotent (ADR-0005). The producer still sets `enable.idempotence=true`. That only removes
broker-level duplicates from producer retries. It is not EOS.

## Why

- Kafka EOS is atomic **only across Kafka**. Our side effects live in Postgres: a payment row,
  a stock decrement. A Kafka transaction can't include a Postgres commit, so the dual-write problem
  (ADR-0004) remains.
- Idempotent consumers are needed anyway. Logical retries from the sweeper carry new eventIds, and
  EOS can't recognize those as duplicates.
- At-least-once with dedup is simpler to reason about and works with any broker. It's also the
  pattern you'll meet most often in production.

## Consequences

- Consumers must tolerate duplicates. Tests assert that replaying a message changes nothing.
- There's a small storage cost (inbox) and one extra insert per message.

## Alternatives rejected

- **Kafka transactions with `read_committed` consumers:** they still don't cover the DB write. They're also
  more config and more failure modes (zombie fencing, transactional.id management), and a stated non-goal.
- **At-most-once** (commit the offset before processing): it silently loses commands, which is unacceptable for money.
