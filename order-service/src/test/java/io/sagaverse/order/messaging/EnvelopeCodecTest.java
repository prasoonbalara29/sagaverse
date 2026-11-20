package io.sagaverse.order.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.PaymentMessages.PaymentCharged;
import io.sagaverse.order.messaging.EnvelopeCodec.MalformedMessageException;
import io.sagaverse.order.messaging.EnvelopeCodec.UnknownMessageTypeException;
import io.sagaverse.order.messaging.EnvelopeCodec.UnsupportedVersionException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EnvelopeCodecTest {

  /** Same settings as the Spring-configured mapper (application.yml + AppConfig). */
  private final EnvelopeCodec codec =
      new EnvelopeCodec(
          new ObjectMapper()
              .registerModule(new JavaTimeModule())
              .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
              .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
              .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true)));

  private static final String VALID_PAYLOAD =
      "{\"orderId\":\"o-1\",\"paymentId\":\"" + UUID.randomUUID() + "\",\"amount\":42.50}";

  private static String envelope(String type, int version, String payload) {
    return "{\"eventId\":\""
        + UUID.randomUUID()
        + "\",\"sagaId\":\""
        + UUID.randomUUID()
        + "\",\"type\":\""
        + type
        + "\",\"version\":"
        + version
        + ",\"occurredAt\":\"2026-11-20T10:00:00Z\",\"payload\":"
        + payload
        + "}";
  }

  @Test
  void roundTripKeepsEveryFieldAndExactMoney() {
    var sent =
        EventEnvelope.of(
            UUID.randomUUID(),
            new PaymentCharged("o-1", UUID.randomUUID(), new BigDecimal("42.50")),
            Instant.parse("2026-11-20T10:00:00Z"));

    var received = codec.decode(codec.encode(sent));

    assertThat(received).isEqualTo(sent);
  }

  @Test
  void unknownTypeIsRejected() {
    assertThatThrownBy(() -> codec.decode(envelope("ShipOrder", 1, "{}")))
        .isInstanceOf(UnknownMessageTypeException.class)
        .hasMessageContaining("ShipOrder");
  }

  @Test
  void futureVersionIsRejected() {
    assertThatThrownBy(() -> codec.decode(envelope("PaymentCharged", 2, VALID_PAYLOAD)))
        .isInstanceOf(UnsupportedVersionException.class)
        .hasMessageContaining("version 2");
  }

  @Test
  void invalidPayloadIsRejected() {
    assertThatThrownBy(() -> codec.decode(envelope("PaymentCharged", 1, "{\"orderId\":\"\"}")))
        .isInstanceOf(MalformedMessageException.class)
        .hasMessageContaining("invalid PaymentCharged payload");
  }

  @Test
  void garbageAndMissingFieldsAreRejected() {
    assertThatThrownBy(() -> codec.decode("not json"))
        .isInstanceOf(MalformedMessageException.class);
    assertThatThrownBy(() -> codec.decode("{\"type\":\"PaymentCharged\"}"))
        .isInstanceOf(MalformedMessageException.class);
    assertThatThrownBy(() -> codec.decode("")).isInstanceOf(MalformedMessageException.class);
  }
}
