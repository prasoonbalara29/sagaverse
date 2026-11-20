package io.sagaverse.e2e;

import static io.sagaverse.e2e.SagaverseCluster.Service.INVENTORY;
import static io.sagaverse.e2e.SagaverseCluster.Service.ORDER;
import static io.sagaverse.e2e.SagaverseCluster.Service.PAYMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Failure paths F1 and F2 from the scenario, with all three real services.
 *
 * <ul>
 *   <li>F1 payment declined: order CANCELLED, nothing else happens.
 *   <li>F2 out of stock: payment refunded, stock untouched, order CANCELLED.
 * </ul>
 */
@Testcontainers
class FailurePathsE2EIT {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.2");

  static SagaverseCluster cluster;
  static RestClient orders;

  @BeforeAll
  static void startServices() {
    cluster =
        new SagaverseCluster(POSTGRES, KAFKA, "reliable")
            .with(PAYMENT, "payment.decline-amount-over", "100.00")
            .startAll();
    orders = RestClient.create(cluster.baseUrl(ORDER));
  }

  @AfterAll
  static void stopServices() {
    cluster.close();
  }

  /**
   * F1. Expected events: ChargePayment, then PaymentDeclined. Then nothing: no ReserveStock and no
   * compensation.
   */
  @Test
  void f1_paymentDeclined_cancelsOrder_andNothingElseHappens() {
    int stockBefore = available("SKU-1");
    var placed = place("SKU-1", 1, "500.00");

    awaitOutcome(placed, "CANCELLED/FAILED");

    assertThat(paymentStatus(placed.sagaId())).isEqualTo("DECLINED");
    assertThat(ledger(placed.sagaId())).isEmpty();
    assertThat(reservations(placed.sagaId())).isZero();
    assertThat(available("SKU-1")).isEqualTo(stockBefore);
  }

  /**
   * F2. Expected events: ChargePayment, PaymentCharged, ReserveStock, StockUnavailable,
   * RefundPayment, PaymentRefunded.
   */
  @Test
  void f2_outOfStock_refundsPayment_leavesStockUntouched_cancelsOrder() {
    var placed = place("SKU-EMPTY", 1, "10.00");

    awaitOutcome(placed, "CANCELLED/COMPENSATED");

    assertThat(paymentStatus(placed.sagaId())).isEqualTo("REFUNDED");
    assertThat(ledger(placed.sagaId()))
        .containsExactly(
            Map.of("entry_type", "CHARGE", "amount", new BigDecimal("10.00")),
            Map.of("entry_type", "REFUND", "amount", new BigDecimal("-10.00")));
    assertThat(reservations(placed.sagaId())).isZero();
    assertThat(available("SKU-EMPTY")).isZero();
  }

  // --- helpers ---------------------------------------------------------------------------

  record Placed(String orderId, UUID sagaId) {}

  private static Placed place(String sku, int qty, String amount) {
    JsonNode body =
        orders
            .post()
            .uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .body(Map.of("customerId", "cust-e2e", "sku", sku, "quantity", qty, "amount", amount))
            .retrieve()
            .body(JsonNode.class);
    return new Placed(body.path("orderId").asText(), UUID.fromString(body.path("sagaId").asText()));
  }

  /** Waits for "orderStatus/sagaState". On failure the message shows where the saga got stuck. */
  private static void awaitOutcome(Placed p, String expected) {
    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(() -> {
          JsonNode o =
              orders.get().uri("/orders/{id}", p.orderId()).retrieve().body(JsonNode.class);
          String actual = o.path("status").asText() + "/" + o.path("saga").path("state").asText();
          assertThat(actual)
              .as("order/saga for %s; payment ledger=%s", p.orderId(), ledger(p.sagaId()))
              .isEqualTo(expected);
        });
  }

  private static String paymentStatus(UUID sagaId) {
    return cluster
        .db(PAYMENT)
        .queryForObject("SELECT status FROM payment WHERE saga_id = ?", String.class, sagaId);
  }

  private static List<Map<String, Object>> ledger(UUID sagaId) {
    return cluster
        .db(PAYMENT)
        .queryForList(
            "SELECT entry_type, amount FROM ledger WHERE saga_id = ? ORDER BY id", sagaId);
  }

  private static int reservations(UUID sagaId) {
    return cluster
        .db(INVENTORY)
        .queryForObject(
            "SELECT count(*) FROM reservation WHERE saga_id = ? AND status = 'RESERVED'",
            Integer.class,
            sagaId);
  }

  private static int available(String sku) {
    return cluster
        .db(INVENTORY)
        .queryForObject("SELECT available FROM stock WHERE sku = ?", Integer.class, sku);
  }
}
