# ADR-0007: Retry and dead-letter strategy for poison messages

- Status: Accepted
- Date: 2026-11-20

## Context

Some messages fail when processed. Some failures are **transient**: a DB blip, or a gateway that
fails at random. Others are **permanent**: unparseable JSON, an unknown message `type`, an
unsupported `version`, or an invalid payload. A permanent failure retried forever blocks its
partition, so every later order on that partition stalls behind it. This is called the "poison
pill".

## Decision

- **Retries:** Spring Kafka `DefaultErrorHandler` with **blocking** `ExponentialBackOffWithMaxRetries`:
  the first attempt plus 3 retries after 1s, 2s and 4s (`messaging.retry.*`, see `KafkaErrorHandling` in
  each service). Blocking means the partition waits, which keeps per-order ordering intact (ADR-0002).
- **Non-retryable** exceptions skip the backoff and go straight to the DLT. These are the
  `EnvelopeCodec.MessageRejectedException` subtypes: `MalformedMessageException` (bad JSON, missing
  envelope field, payload failing validation), `UnknownMessageTypeException` (including a message from the
  wrong family on a topic), and `UnsupportedVersionException`. Values are plain strings, so a bad record
  surfaces in the codec, not in the Kafka deserializer, and `ErrorHandlingDeserializer` isn't needed.
- **Dead letter:** `DeadLetterPublishingRecoverer` sends the record to `<topic>.DLT` (3 partitions,
  same key, same partition). Headers carry the original topic, partition and offset, and the exception
  (`kafka_dlt-exception-fqcn`, `kafka_dlt-exception-cause-fqcn`, `kafka_dlt-exception-message`). The offset is
  then committed, so the partition moves on.
- **Business recovery:** a command in the DLT never gets a reply. The saga's step deadline expires,
  and the sweeper re-sends the command or compensates (ADR-0009, ADR-0008). The business flow stays
  consistent even with poison input.
- **Replay:** `make dlt-replay TOPIC=<topic>` (`scripts/dlt-replay.sh`). It uses a consumer group
  `dlt-replay-<topic>` and `--max-messages` equal to the backlog at start, so each dead letter is
  re-sent once per run. The naive consumer-to-producer pipe was tried first and **looped**: a
  record that still failed went DLT → topic → DLT 95 times in 2 minutes. Automated replay is out
  of scope.

## Consequences

- A poison message isn't retried, but it isn't free either. **Measured: about 0.4 s of partition stall
  per poison record**, because after each failure the error handler seeks the partition back and
  re-polls. Measured with a backlog queued: 51 poison records dead-lettered in 20 s on one partition
  (about 2.5/s), while the payment-service group's lag stood at 43. A *transient* failure costs at
  most about 7 s (1 + 2 + 4) before it's moved aside.
- Without this handler, Spring Kafka's default retries 10 times with no delay, then logs and **skips**
  the record. Nothing is kept for inspection. `PoisonMessageE2EIT` showed exactly that before the fix.
- Transient failures that last longer than the backoff end up in the DLT too. The saga's own
  re-send logic then gives them another chance with a fresh message.

## Alternatives rejected

- **Non-blocking retry topics (`@RetryableTopic`):** the failing record leaves its partition, so
  later messages for the same orderId can be processed first. That breaks per-order ordering, for
  example a Refund overtaking a Charge.
- **Infinite retry:** a single poison pill stalls every order on its partition.
- **Log and skip:** silent data loss, and nothing is left to inspect.
