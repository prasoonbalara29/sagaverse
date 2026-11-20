package io.sagaverse.e2e;

import static io.sagaverse.e2e.SagaverseCluster.Service.INVENTORY;
import static io.sagaverse.e2e.SagaverseCluster.Service.ORDER;
import static io.sagaverse.e2e.SagaverseCluster.Service.PAYMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Duration;
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
 * All three real services, real Kafka, real Postgres: place an order over HTTP and check that every
 * database ends up consistent.
 *
 * <p>Expected event sequence: ChargePayment → PaymentCharged → ReserveStock → StockReserved.
 */
@Testcontainers
class HappyPathE2EIT {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.2");

  static SagaverseCluster cluster;
  static RestClient orders;

  @BeforeAll
  static void startServices() {
    cluster = new SagaverseCluster(POSTGRES, KAFKA, "naive").startAll();
    orders = RestClient.create(cluster.baseUrl(ORDER));
  }

  @AfterAll
  static void stopServices() {
    cluster.close();
  }

  @Test
  void placeOrder_chargesReservesAndConfirms_acrossAllThreeDatabases() {
    int stockBefore = available("SKU-2");

    JsonNode placed =
        orders
            .post()
            .uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .body(
                Map.of("customerId", "cust-e2e", "sku", "SKU-2", "quantity", 3, "amount", "19.99"))
            .retrieve()
            .body(JsonNode.class);
    String orderId = placed.path("orderId").asText();
    UUID sagaId = UUID.fromString(placed.path("sagaId").asText());

    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> "CONFIRMED".equals(order(orderId).path("status").asText()));

    // order_db: saga completed, with the full happy-path history.
    var o = order(orderId);
    assertThat(o.path("saga").path("state").asText()).isEqualTo("COMPLETED");
    assertThat(o.path("saga").path("transitions")).hasSize(3);

    // payment_db: exactly one charge.
    var payments = cluster.db(PAYMENT);
    assertThat(payments.queryForMap("SELECT status, amount FROM payment WHERE saga_id = ?", sagaId))
        .containsEntry("status", "CHARGED")
        .containsEntry("amount", new BigDecimal("19.99"));
    assertThat(
            payments.queryForList(
                "SELECT entry_type, amount FROM ledger WHERE saga_id = ?", sagaId))
        .containsExactly(Map.of("entry_type", "CHARGE", "amount", new BigDecimal("19.99")));

    // inventory_db: exactly three units held for this saga.
    var inventory = cluster.db(INVENTORY);
    assertThat(available("SKU-2")).isEqualTo(stockBefore - 3);
    assertThat(
            inventory.queryForMap(
                "SELECT sku, quantity, status FROM reservation WHERE saga_id = ?", sagaId))
        .containsEntry("sku", "SKU-2")
        .containsEntry("quantity", 3)
        .containsEntry("status", "RESERVED");
  }

  private static JsonNode order(String orderId) {
    return orders.get().uri("/orders/{id}", orderId).retrieve().body(JsonNode.class);
  }

  private static int available(String sku) {
    return cluster
        .db(INVENTORY)
        .queryForObject("SELECT available FROM stock WHERE sku = ?", Integer.class, sku);
  }
}
