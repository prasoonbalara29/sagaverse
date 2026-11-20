package io.sagaverse.inventory.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.EventType;
import io.sagaverse.events.SagaPayload;
import org.springframework.stereotype.Component;

/**
 * Turns envelopes into wire JSON and back.
 *
 * <p>Decoding happens in two steps, the way any consumer should do it:
 *
 * <ol>
 *   <li>read the envelope with the payload as a raw JSON tree;
 *   <li>check {@code type} and {@code version}, then convert the payload to its record.
 * </ol>
 *
 * Anything that fails these checks is a {@link MessageRejectedException}. Retrying can't fix it, so
 * the error handler sends it to the dead-letter topic (ADR-0007).
 */
@Component
public class EnvelopeCodec {

  private static final TypeReference<EventEnvelope<JsonNode>> RAW = new TypeReference<>() {};

  private final ObjectMapper mapper;

  public EnvelopeCodec(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public String encode(EventEnvelope<? extends SagaPayload> envelope) {
    try {
      return mapper.writeValueAsString(envelope);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialize " + envelope.type(), e);
    }
  }

  public EventEnvelope<SagaPayload> decode(String json) {
    if (json == null || json.isBlank()) {
      throw new MalformedMessageException("empty record value", null);
    }
    EventEnvelope<JsonNode> raw;
    try {
      raw = mapper.readValue(json, RAW);
    } catch (JsonProcessingException e) {
      throw new MalformedMessageException("not a valid envelope: " + e.getOriginalMessage(), e);
    }
    EventType type =
        EventType.fromWireName(raw.type())
            .orElseThrow(() -> new UnknownMessageTypeException(raw.type()));
    if (raw.version() != EventEnvelope.CURRENT_VERSION) {
      throw new UnsupportedVersionException(raw.type(), raw.version());
    }
    SagaPayload payload;
    try {
      payload = mapper.treeToValue(raw.payload(), type.payloadClass());
    } catch (JsonProcessingException e) {
      throw new MalformedMessageException(
          "invalid " + raw.type() + " payload: " + e.getOriginalMessage(), e);
    }
    return new EventEnvelope<>(
        raw.eventId(), raw.sagaId(), raw.type(), raw.version(), raw.occurredAt(), payload);
  }

  /**
   * Decodes and checks that the payload is of the family this topic carries, e.g. a reply type
   * published to a commands topic by mistake is rejected.
   */
  public <T extends SagaPayload> EventEnvelope<T> decode(String json, Class<T> expected) {
    EventEnvelope<SagaPayload> env = decode(json);
    if (!expected.isInstance(env.payload())) {
      throw new UnknownMessageTypeException(
          env.type() + " (not a " + expected.getSimpleName() + ")");
    }
    return new EventEnvelope<>(
        env.eventId(),
        env.sagaId(),
        env.type(),
        env.version(),
        env.occurredAt(),
        expected.cast(env.payload()));
  }

  /** A message that can never be processed, however often it is retried. */
  public abstract static class MessageRejectedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    MessageRejectedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** Unparseable JSON, a missing envelope field, or a payload that fails validation. */
  public static final class MalformedMessageException extends MessageRejectedException {
    private static final long serialVersionUID = 1L;

    MalformedMessageException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** A {@code type} this service has never heard of. */
  public static final class UnknownMessageTypeException extends MessageRejectedException {
    private static final long serialVersionUID = 1L;

    UnknownMessageTypeException(String type) {
      super("unknown message type: " + type, null);
    }
  }

  /** A {@code version} this service can't read. */
  public static final class UnsupportedVersionException extends MessageRejectedException {
    private static final long serialVersionUID = 1L;

    UnsupportedVersionException(String type, int version) {
      super("unsupported version " + version + " of " + type, null);
    }
  }
}
