package io.sagaverse.order;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.InventoryMessages.ReserveStock;
import io.sagaverse.events.InventoryMessages.StockReserved;
import io.sagaverse.events.PaymentMessages.ChargePayment;
import io.sagaverse.events.PaymentMessages.PaymentCharged;
import io.sagaverse.events.SagaPayload;
import io.sagaverse.events.Topics;
import io.sagaverse.order.messaging.EnvelopeCodec;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Drives the orchestrator through the happy path with real Postgres and Kafka. The test itself
 * plays payment-service and inventory-service: it reads the commands order-service sends and
 * answers with replies. The real services join in Phase 2b's e2e test.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class OrderSagaHappyPathIT {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Container @ServiceConnection
  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.2");

  @Autowired TestRestTemplate rest;
  @Autowired JdbcTemplate jdbc;
  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired EnvelopeCodec codec;

  private KafkaConsumer<String, String> commands;
  private final List<ConsumerRecord<String, String>> seen = new ArrayList<>();

  @BeforeAll
  static void createTopics() throws Exception {
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      var topics =
          Topics.ALL.stream().map(t -> new NewTopic(t, Topics.PARTITIONS, (short) 1)).toList();
      admin.createTopics(topics).all().get(30, SECONDS);
    }
  }

  @BeforeEach
  void subscribeToCommands() {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    commands = new KafkaConsumer<>(props);
    commands.subscribe(List.of(Topics.PAYMENT_COMMANDS, Topics.INVENTORY_COMMANDS));
  }

  @AfterEach
  void close() {
    commands.close();
  }

  @Test
  void happyPath_chargeThenReserve_confirmsTheOrder() {
    // 1. Place the order.
    var placed =
        rest.postForEntity(
            "/orders",
            Map.of("customerId", "cust-1", "sku", "SKU-1", "quantity", 2, "amount", "42.50"),
            JsonNode.class);
    assertThat(placed.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    String orderId = placed.getBody().path("orderId").asText();
    UUID sagaId = UUID.fromString(placed.getBody().path("sagaId").asText());

    assertThat(orderStatus(orderId)).isEqualTo("PENDING");
    assertThat(sagaState(sagaId)).isEqualTo("PAYMENT_PENDING");

    // 2. order-service charges the payment, keyed by orderId.
    var charge = awaitCommand(Topics.PAYMENT_COMMANDS, orderId, "ChargePayment");
    assertThat(charge.sagaId()).isEqualTo(sagaId);
    assertThat(charge.payload())
        .isEqualTo(new ChargePayment(orderId, "cust-1", new BigDecimal("42.50")));

    // 3. "payment-service" replies: charged. The saga moves on and reserves stock.
    reply(
        Topics.PAYMENT_REPLIES,
        sagaId,
        new PaymentCharged(orderId, UUID.randomUUID(), new BigDecimal("42.50")));
    var reserve = awaitCommand(Topics.INVENTORY_COMMANDS, orderId, "ReserveStock");
    assertThat(reserve.payload()).isEqualTo(new ReserveStock(orderId, "SKU-1", 2));
    assertThat(sagaState(sagaId)).isEqualTo("INVENTORY_PENDING");

    // 4. "inventory-service" replies: reserved. The saga completes and the order is confirmed.
    reply(Topics.INVENTORY_REPLIES, sagaId, new StockReserved(orderId, "SKU-1", 2));
    await().atMost(20, SECONDS).until(() -> "COMPLETED".equals(sagaState(sagaId)));
    assertThat(orderStatus(orderId)).isEqualTo("CONFIRMED");

    // 5. The GET view shows the full history, in order.
    JsonNode view = rest.getForObject("/orders/" + orderId, JsonNode.class);
    assertThat(view.path("status").asText()).isEqualTo("CONFIRMED");
    assertThat(view.path("saga").path("deadlineAt").isNull()).isTrue();
    List<String> history = new ArrayList<>();
    view.path("saga")
        .path("transitions")
        .forEach(t ->
            history.add(
                t.path("from").asText("null")
                    + " -> "
                    + t.path("to").asText()
                    + " on "
                    + t.path("trigger").asText()));
    assertThat(history)
        .containsExactly(
            "null -> PAYMENT_PENDING on OrderPlaced",
            "PAYMENT_PENDING -> INVENTORY_PENDING on PaymentCharged",
            "INVENTORY_PENDING -> COMPLETED on StockReserved");
  }

  @Test
  void invalidOrderIsRejectedWithoutStartingASaga() {
    Integer before = jdbc.queryForObject("SELECT count(*) FROM saga_instance", Integer.class);

    var resp =
        rest.postForEntity(
            "/orders",
            Map.of("customerId", "cust-1", "sku", "SKU-1", "quantity", 0, "amount", "42.50"),
            JsonNode.class);

    assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM saga_instance", Integer.class))
        .isEqualTo(before);
  }

  @Test
  void unknownOrderIs404() {
    assertThat(rest.getForEntity("/orders/nope", JsonNode.class).getStatusCode())
        .isEqualTo(HttpStatus.NOT_FOUND);
  }

  // --- helpers ---------------------------------------------------------------------------

  private EventEnvelope<SagaPayload> awaitCommand(String topic, String key, String type) {
    return await()
        .atMost(Duration.ofSeconds(20))
        .pollInSameThread()
        .until(
            () -> {
              commands.poll(Duration.ofMillis(200)).forEach(seen::add);
              return seen.stream()
                  .filter(r -> r.topic().equals(topic) && key.equals(r.key()))
                  .map(r -> codec.decode(r.value()))
                  .filter(e -> e.type().equals(type))
                  .findFirst();
            },
            Optional::isPresent)
        .orElseThrow();
  }

  private void reply(String topic, UUID sagaId, SagaPayload payload) {
    var envelope = EventEnvelope.of(sagaId, payload, Instant.now());
    kafka.send(topic, payload.orderId(), codec.encode(envelope)).join();
  }

  private String orderStatus(String orderId) {
    return jdbc.queryForObject(
        "SELECT status FROM orders WHERE order_id = ?", String.class, orderId);
  }

  private String sagaState(UUID sagaId) {
    return jdbc.queryForObject(
        "SELECT state FROM saga_instance WHERE saga_id = ?", String.class, sagaId);
  }
}
