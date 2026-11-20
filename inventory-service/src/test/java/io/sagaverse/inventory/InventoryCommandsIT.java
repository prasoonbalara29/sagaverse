package io.sagaverse.inventory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.InventoryMessages.ReserveStock;
import io.sagaverse.events.InventoryMessages.StockReserved;
import io.sagaverse.events.InventoryMessages.StockUnavailable;
import io.sagaverse.events.SagaPayload;
import io.sagaverse.events.Topics;
import io.sagaverse.inventory.messaging.EnvelopeCodec;
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
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * inventory-service handles ReserveStock end to end. The test plays the orchestrator: it sends
 * commands over real Kafka and reads the replies.
 */
@SpringBootTest(properties = InventoryServiceApplication.CONFIG_NAME)
@Testcontainers
class InventoryCommandsIT {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Container @ServiceConnection
  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.2");

  @Autowired JdbcTemplate jdbc;
  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired EnvelopeCodec codec;

  private KafkaConsumer<String, String> replies;
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
  void subscribeToReplies() {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    replies = new KafkaConsumer<>(props);
    replies.subscribe(List.of(Topics.INVENTORY_REPLIES));
  }

  @AfterEach
  void close() {
    replies.close();
  }

  @Test
  void reserveStock_decrementsStockAndRepliesStockReserved() {
    UUID sagaId = UUID.randomUUID();
    String orderId = "order-" + UUID.randomUUID();

    send(sagaId, new ReserveStock(orderId, "SKU-1", 2));

    var reply = awaitReply(orderId);
    assertThat(reply.sagaId()).isEqualTo(sagaId);
    assertThat(reply.payload()).isEqualTo(new StockReserved(orderId, "SKU-1", 2));
    assertThat(available("SKU-1")).isEqualTo(98);
    assertThat(
            jdbc.queryForMap(
                "SELECT sku, quantity, status FROM reservation WHERE saga_id = ?", sagaId))
        .containsEntry("sku", "SKU-1")
        .containsEntry("quantity", 2)
        .containsEntry("status", "RESERVED");
  }

  @Test
  void reserveStock_whenNotEnough_repliesStockUnavailableAndTouchesNothing() {
    UUID sagaId = UUID.randomUUID();
    String orderId = "order-" + UUID.randomUUID();

    send(sagaId, new ReserveStock(orderId, "SKU-EMPTY", 1));

    var reply = awaitReply(orderId);
    assertThat(reply.payload())
        .isEqualTo(new StockUnavailable(orderId, "SKU-EMPTY", StockUnavailable.OUT_OF_STOCK));
    assertThat(available("SKU-EMPTY")).isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM reservation WHERE saga_id = ?", Integer.class, sagaId))
        .isZero();
  }

  private int available(String sku) {
    return jdbc.queryForObject("SELECT available FROM stock WHERE sku = ?", Integer.class, sku);
  }

  // --- helpers ---------------------------------------------------------------------------

  private void send(UUID sagaId, SagaPayload command) {
    var envelope = EventEnvelope.of(sagaId, command, Instant.now());
    kafka.send(Topics.INVENTORY_COMMANDS, command.orderId(), codec.encode(envelope)).join();
  }

  private EventEnvelope<SagaPayload> awaitReply(String orderId) {
    return await()
        .atMost(Duration.ofSeconds(20))
        .pollInSameThread()
        .until(
            () -> {
              replies.poll(Duration.ofMillis(200)).forEach(seen::add);
              return seen.stream()
                  .filter(r -> orderId.equals(r.key()))
                  .map(r -> codec.decode(r.value()))
                  .findFirst();
            },
            Optional::isPresent)
        .orElseThrow();
  }
}
