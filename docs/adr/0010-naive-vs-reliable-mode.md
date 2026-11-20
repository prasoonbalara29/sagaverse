# ADR-0010: Naive and reliable modes, side by side

- Status: Accepted
- Date: 2026-11-20

## Context

The repo has to **show** the failures (double charge, lost event, stuck saga) before fixing them,
and keep both versions around for comparison.

## Decision

There's one codebase. Each service reads `saga.mode` = `naive` | `reliable`. `@ConditionalOnProperty`
swaps the strategy beans:

| Concern | naive | reliable |
|---|---|---|
| `MessagePublisher` | `DirectKafkaPublisher`: commit, then `kafka.send()` (dual write). Hosts the `FaultInjector` "crash after DB commit" hook. | `OutboxPublisher`: inserts into `outbox` in the same transaction. `OutboxRelay` publishes. |
| `IdempotencyGuard` | `TrustEveryDelivery`: always processes | `Inbox` (eventId) plus the sagaId business check |
| `SagaTimeoutSweeper` | Disabled | Enabled |

The schema is **the same in both modes**. Naive code simply doesn't use the outbox, inbox or
idempotency lookups. The `ledger` table is append-only with no unique key, so a naive double charge
shows up as two rows.

**Tests:** a shared `ScenarioRunner` and `InvariantChecker` hold these invariants:
- at most one charge ledger entry per saga, and the net ledger is zero for cancelled orders;
- stock conservation: `initial = available + Σ active reservations`;
- no orphan reservations for cancelled orders;
- no order still PENDING after its saga deadline.

`ReliableModeE2EIT` asserts **zero violations** for every scenario. `NaiveModeE2EIT` runs the same
scenarios and asserts that the checker reports the **specific expected violation** (e.g.
`DOUBLE_CHARGE saga=… ledgerRows=2`). `make verify` stays green, and the numbers from both feed the
README results table.

While building each fix, the reliable assertion is first run against naive mode, to show it fail
for real. That output is pasted into PROGRESS.md.

**Default:** `reliable` (each service's yml, `SagaProperties`, and `matchIfMissing` on the reliable
beans). Naive runs only when a test or demo asks for it explicitly.

## Consequences

- Reading the diff between the two bean implementations is the lesson.
- Slightly more wiring (conditional beans) than a single implementation.

## Alternatives rejected

- **Separate naive and reliable modules:** about twice the code, and the two would drift apart.
- **Git branches or tags for naive:** you can't run both in one test suite to compare them.
- **Naive tests tagged and excluded from verify (really red):** they'd rot unnoticed. Asserting the
  violation keeps the demonstration under test.
