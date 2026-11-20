package io.sagaverse.payment;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.PaymentMessages.ChargePayment;
import io.sagaverse.events.PaymentMessages.PaymentCharged;
import io.sagaverse.events.PaymentMessages.PaymentDeclined;
import io.sagaverse.events.PaymentMessages.PaymentRefunded;
import io.sagaverse.events.PaymentMessages.RefundPayment;
import io.sagaverse.events.SagaPayload;
import io.sagaverse.events.Topics;
import io.sagaverse.payment.messaging.EnvelopeCodec;
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
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * payment-service handles ChargePayment end to end. The test plays the orchestrator: it sends
 * commands over real Kafka and reads the replies.
 */
@SpringBootTest(
    properties = {PaymentServiceApplication.CONFIG_NAME, "payment.decline-amount-over=100.00"})
@Testcontainers
class PaymentCommandsIT {

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
    replies.subscribe(List.of(Topics.PAYMENT_REPLIES));
  }

  @AfterEach
  void close() {
    replies.close();
  }

  @Test
  void chargePayment_chargesOnceAndRepliesPaymentCharged() {
    UUID sagaId = UUID.randomUUID();
    String orderId = "order-" + UUID.randomUUID();

    send(sagaId, new ChargePayment(orderId, "cust-1", new BigDecimal("42.50")));

    var reply = awaitReply(orderId, "PaymentCharged");
    assertThat(reply.sagaId()).isEqualTo(sagaId);
    assertThat(reply.payload()).isInstanceOf(PaymentCharged.class);
    var charged = (PaymentCharged) reply.payload();
    assertThat(charged.amount()).isEqualByComparingTo("42.50");

    assertThat(
            jdbc.queryForMap(
                "SELECT payment_id, status, amount FROM payment WHERE saga_id = ?", sagaId))
        .containsEntry("payment_id", charged.paymentId())
        .containsEntry("status", "CHARGED")
        .containsEntry("amount", new BigDecimal("42.50"));
    assertThat(jdbc.queryForList("SELECT entry_type, amount FROM ledger WHERE saga_id = ?", sagaId))
        .containsExactly(Map.of("entry_type", "CHARGE", "amount", new BigDecimal("42.50")));
  }

  @Test
  void chargeOverTheLimit_isDeclined_andNoMoneyMoves() {
    UUID sagaId = UUID.randomUUID();
    String orderId = "order-" + UUID.randomUUID();

    send(sagaId, new ChargePayment(orderId, "cust-1", new BigDecimal("500.00")));

    assertThat(awaitReply(orderId, "PaymentDeclined").payload())
        .isEqualTo(new PaymentDeclined(orderId, "AMOUNT_LIMIT_EXCEEDED"));
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM payment WHERE saga_id = ?", String.class, sagaId))
        .isEqualTo("DECLINED");
    assertThat(ledgerRows(sagaId)).isZero();
  }

  @Test
  void refundAfterCharge_refundsOnce_evenIfDeliveredTwice() {
    UUID sagaId = UUID.randomUUID();
    String orderId = "order-" + UUID.randomUUID();
    send(sagaId, new ChargePayment(orderId, "cust-1", new BigDecimal("10.00")));
    var charged = (PaymentCharged) awaitReply(orderId, "PaymentCharged").payload();

    send(sagaId, new RefundPayment(orderId, "OUT_OF_STOCK"));
    send(sagaId, new RefundPayment(orderId, "OUT_OF_STOCK"));

    await().atMost(20, SECONDS).pollInSameThread().until(() -> refundReplies(orderId) == 2);
    assertThat(awaitReply(orderId, "PaymentRefunded").payload())
        .isEqualTo(new PaymentRefunded(orderId, charged.paymentId()));
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM payment WHERE saga_id = ?", String.class, sagaId))
        .isEqualTo("REFUNDED");
    assertThat(
            jdbc.queryForList(
                "SELECT entry_type, amount FROM ledger WHERE saga_id = ? ORDER BY id", sagaId))
        .containsExactly(
            Map.of("entry_type", "CHARGE", "amount", new BigDecimal("10.00")),
            Map.of("entry_type", "REFUND", "amount", new BigDecimal("-10.00")));
  }

  @Test
  void refundBeforeAnyCharge_writesTombstone_soALateChargeIsDeclined() {
    UUID sagaId = UUID.randomUUID();
    String orderId = "order-" + UUID.randomUUID();

    send(sagaId, new RefundPayment(orderId, "TIMEOUT"));
    assertThat(awaitReply(orderId, "PaymentRefunded").payload())
        .isEqualTo(new PaymentRefunded(orderId, null));

    send(sagaId, new ChargePayment(orderId, "cust-1", new BigDecimal("10.00")));
    assertThat(awaitReply(orderId, "PaymentDeclined").payload())
        .isEqualTo(new PaymentDeclined(orderId, "SAGA_CANCELLED"));
    assertThat(ledgerRows(sagaId)).isZero();
  }

  private int ledgerRows(UUID sagaId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM ledger WHERE saga_id = ?", Integer.class, sagaId);
  }

  private long refundReplies(String orderId) {
    replies.poll(Duration.ofMillis(200)).forEach(seen::add);
    return seen.stream()
        .filter(r -> orderId.equals(r.key()))
        .filter(r -> codec.decode(r.value()).type().equals("PaymentRefunded"))
        .count();
  }

  // --- helpers ---------------------------------------------------------------------------

  private void send(UUID sagaId, SagaPayload command) {
    var envelope = EventEnvelope.of(sagaId, command, Instant.now());
    kafka.send(Topics.PAYMENT_COMMANDS, command.orderId(), codec.encode(envelope)).join();
  }

  private EventEnvelope<SagaPayload> awaitReply(String orderId, String type) {
    return await()
        .atMost(Duration.ofSeconds(20))
        .pollInSameThread()
        .until(
            () -> {
              replies.poll(Duration.ofMillis(200)).forEach(seen::add);
              return seen.stream()
                  .filter(r -> orderId.equals(r.key()))
                  .map(r -> codec.decode(r.value()))
                  .filter(e -> e.type().equals(type))
                  .findFirst();
            },
            Optional::isPresent)
        .orElseThrow();
  }
}
