package io.sagaverse.events;

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.util.UUID;

/**
 * The versioned JSON envelope that wraps every message on every topic.
 *
 * <ul>
 *   <li>{@code eventId}: unique per message. Consumers dedupe on it (the inbox, ADR-0005).
 *   <li>{@code sagaId}: correlates every message of one saga, in logs and in business idempotency.
 *   <li>{@code type}: wire name of the payload, see {@link EventType}.
 *   <li>{@code version}: payload schema version. Consumers reject versions they don't understand.
 *   <li>{@code occurredAt}: when the producer created the message.
 *   <li>{@code payload}: the message body. Producers use a concrete {@link SagaPayload}. Consumers
 *       usually read it as a JSON tree first and convert it once {@code type} and {@code version}
 *       have been checked.
 * </ul>
 *
 * <p>The Kafka record key is always the {@code orderId} carried inside the payload (ADR-0002).
 *
 * @param <P> payload type
 */
public record EventEnvelope<P>(
    UUID eventId, UUID sagaId, String type, int version, Instant occurredAt, P payload) {

  /** The only payload schema version that exists today. */
  public static final int CURRENT_VERSION = 1;

  public EventEnvelope {
    requireNonNull(eventId, "eventId");
    requireNonNull(sagaId, "sagaId");
    requireNonNull(type, "type");
    requireNonNull(occurredAt, "occurredAt");
    requireNonNull(payload, "payload");
    if (version < 1) {
      throw new IllegalArgumentException("version must be >= 1, was " + version);
    }
  }

  /** Wraps a payload in a new envelope with a fresh eventId and the current version. */
  public static <P extends SagaPayload> EventEnvelope<P> of(
      UUID sagaId, P payload, Instant occurredAt) {
    return new EventEnvelope<>(
        UUID.randomUUID(),
        sagaId,
        EventType.of(payload).wireName(),
        CURRENT_VERSION,
        occurredAt,
        payload);
  }
}
