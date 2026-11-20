package io.sagaverse.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.sagaverse.events.InventoryMessages.InventoryCommand;
import io.sagaverse.events.InventoryMessages.InventoryReply;
import io.sagaverse.events.InventoryMessages.ReleaseStock;
import io.sagaverse.events.InventoryMessages.ReserveStock;
import io.sagaverse.events.InventoryMessages.StockReleased;
import io.sagaverse.events.InventoryMessages.StockReserved;
import io.sagaverse.events.InventoryMessages.StockUnavailable;
import io.sagaverse.events.PaymentMessages.ChargePayment;
import io.sagaverse.events.PaymentMessages.PaymentCharged;
import io.sagaverse.events.PaymentMessages.PaymentCommand;
import io.sagaverse.events.PaymentMessages.PaymentDeclined;
import io.sagaverse.events.PaymentMessages.PaymentRefunded;
import io.sagaverse.events.PaymentMessages.PaymentReply;
import io.sagaverse.events.PaymentMessages.RefundPayment;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class EnvelopeTest {

  private static final Instant NOW = Instant.parse("2026-11-20T10:15:30Z");
  private static final String ORDER_ID = "order-1";

  /**
   * The same Jackson settings every service must use. Without USE_BIG_DECIMAL_FOR_FLOATS and exact
   * decimals, reading a payload as a JsonNode tree turns 42.50 into the double 42.5: precision is
   * lost on money. The round-trip test below caught exactly that.
   */
  private final ObjectMapper mapper =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

  /** One valid sample per type. The switch is exhaustive, so a new type forces a new sample. */
  private static SagaPayload sample(EventType type) {
    return switch (type) {
      case CHARGE_PAYMENT -> new ChargePayment(ORDER_ID, "cust-1", new BigDecimal("42.50"));
      case REFUND_PAYMENT -> new RefundPayment(ORDER_ID, "OUT_OF_STOCK");
      case PAYMENT_CHARGED ->
          new PaymentCharged(ORDER_ID, UUID.randomUUID(), new BigDecimal("42.50"));
      case PAYMENT_DECLINED -> new PaymentDeclined(ORDER_ID, "INSUFFICIENT_FUNDS");
      case PAYMENT_REFUNDED -> new PaymentRefunded(ORDER_ID, null);
      case RESERVE_STOCK -> new ReserveStock(ORDER_ID, "SKU-1", 2);
      case RELEASE_STOCK -> new ReleaseStock(ORDER_ID, "TIMEOUT");
      case STOCK_RESERVED -> new StockReserved(ORDER_ID, "SKU-1", 2);
      case STOCK_UNAVAILABLE ->
          new StockUnavailable(ORDER_ID, "SKU-1", StockUnavailable.OUT_OF_STOCK);
      case STOCK_RELEASED -> new StockReleased(ORDER_ID, true);
    };
  }

  @ParameterizedTest
  @EnumSource(EventType.class)
  void everyTypeRoundTripsThroughJsonTheWayAConsumerReadsIt(EventType type) throws Exception {
    SagaPayload payload = sample(type);
    EventEnvelope<SagaPayload> sent = EventEnvelope.of(UUID.randomUUID(), payload, NOW);

    String json = mapper.writeValueAsString(sent);

    // A consumer reads the envelope generically, checks type and version, then converts.
    EventEnvelope<JsonNode> received =
        mapper.readValue(json, new TypeReference<EventEnvelope<JsonNode>>() {});
    EventType receivedType = EventType.fromWireName(received.type()).orElseThrow();
    SagaPayload receivedPayload =
        mapper.treeToValue(received.payload(), receivedType.payloadClass());

    assertThat(receivedType).isEqualTo(type);
    assertThat(received.eventId()).isEqualTo(sent.eventId());
    assertThat(received.sagaId()).isEqualTo(sent.sagaId());
    assertThat(received.version()).isEqualTo(EventEnvelope.CURRENT_VERSION);
    assertThat(received.occurredAt()).isEqualTo(NOW);
    assertThat(receivedPayload).isEqualTo(payload);
    assertThat(receivedPayload.orderId()).isEqualTo(ORDER_ID);
  }

  @Test
  void wireFormatHasExactlyTheEnvelopeFields() throws Exception {
    EventEnvelope<SagaPayload> envelope =
        EventEnvelope.of(UUID.randomUUID(), sample(EventType.CHARGE_PAYMENT), NOW);

    JsonNode tree = mapper.readTree(mapper.writeValueAsString(envelope));

    List<String> fields = new ArrayList<>();
    tree.fieldNames().forEachRemaining(fields::add);
    assertThat(fields)
        .containsExactly("eventId", "sagaId", "type", "version", "occurredAt", "payload");
    assertThat(tree.get("type").asText()).isEqualTo("ChargePayment");
    assertThat(tree.get("occurredAt").asText()).isEqualTo("2026-11-20T10:15:30Z");
    assertThat(tree.get("payload").get("amount").decimalValue()).isEqualByComparingTo("42.50");
  }

  @Test
  void eachEnvelopeGetsAFreshEventId() {
    UUID sagaId = UUID.randomUUID();
    SagaPayload payload = sample(EventType.RESERVE_STOCK);

    assertThat(EventEnvelope.of(sagaId, payload, NOW).eventId())
        .isNotEqualTo(EventEnvelope.of(sagaId, payload, NOW).eventId());
  }

  @Test
  void catalogueCoversEveryPayloadRecordExactlyOnce() {
    List<Class<?>> leafRecords =
        Arrays.stream(SagaPayload.class.getPermittedSubclasses())
            .flatMap(group -> Arrays.stream(group.getPermittedSubclasses()))
            .toList();

    List<Class<?>> catalogued =
        Arrays.stream(EventType.values()).<Class<?>>map(EventType::payloadClass).toList();
    assertThat(catalogued).containsExactlyInAnyOrderElementsOf(leafRecords);
    assertThat(EventType.values())
        .allSatisfy(t -> assertThat(t.wireName()).isEqualTo(t.payloadClass().getSimpleName()));
  }

  @Test
  void commandsAndRepliesTravelOnTheirOwnTopics() {
    for (EventType type : EventType.values()) {
      Class<?> c = type.payloadClass();
      String expected =
          PaymentCommand.class.isAssignableFrom(c)
              ? Topics.PAYMENT_COMMANDS
              : PaymentReply.class.isAssignableFrom(c)
                  ? Topics.PAYMENT_REPLIES
                  : InventoryCommand.class.isAssignableFrom(c)
                      ? Topics.INVENTORY_COMMANDS
                      : InventoryReply.class.isAssignableFrom(c) ? Topics.INVENTORY_REPLIES : null;
      assertThat(type.topic()).as(type.name()).isEqualTo(expected);
    }
  }

  @Test
  void unknownWireNameIsEmptySoConsumersCanDeadLetterIt() {
    assertThat(EventType.fromWireName("ShipOrder")).isEmpty();
  }

  @Test
  void invalidPayloadIsRejectedDuringDeserialization() {
    String json = "{\"orderId\":\"order-1\",\"customerId\":\"c\",\"amount\":-1}";

    assertThatThrownBy(() -> mapper.readValue(json, ChargePayment.class))
        .isInstanceOf(JsonMappingException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .rootCause()
        .hasMessageContaining("amount must be positive");
  }

  @Test
  void envelopeRejectsMissingFieldsAndBadVersion() {
    UUID id = UUID.randomUUID();
    SagaPayload p = sample(EventType.STOCK_RELEASED);

    assertThatThrownBy(() -> new EventEnvelope<>(id, null, "StockReleased", 1, NOW, p))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("sagaId");
    assertThatThrownBy(() -> new EventEnvelope<>(id, id, "StockReleased", 0, NOW, p))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void topicsIncludeADeadLetterTopicForEachMainTopic() {
    assertThat(Topics.ALL).hasSize(8).containsAll(Topics.MAIN);
    assertThat(Topics.MAIN).allSatisfy(t -> assertThat(Topics.ALL).contains(t + ".DLT"));
  }
}
