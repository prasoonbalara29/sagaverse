package io.sagaverse.e2e;

import static io.sagaverse.e2e.SagaverseCluster.Service.ORDER;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.awaitility.core.ConditionTimeoutException;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

/**
 * The failure scenarios from the brief, each driven the same way in naive and reliable mode
 * (ADR-0010). A scenario returns the sagaId it created. The test then asks {@link InvariantChecker}
 * what broke.
 */
final class ScenarioRunner {

  /** How long a scenario waits for its saga to finish before reporting what it sees. */
  static final Duration SETTLE = Duration.ofSeconds(10);

  private static final List<String> TERMINAL =
      List.of("COMPLETED", "FAILED", "COMPENSATED", "COMPENSATION_FAILED");

  private final SagaverseCluster cluster;

  ScenarioRunner(SagaverseCluster cluster) {
    this.cluster = cluster;
  }

  /**
   * F4, lost event after a crash. order-service commits the new order and saga, then "dies" before
   * the ChargePayment reaches Kafka (fault {@code fault.crash-before-publish}). It's restarted
   * healthy.
   *
   * <p>Naive: the command only ever existed in the dead process's memory, so the order hangs.
   * Reliable: the command was committed to the outbox with the order, and the restarted relay sends
   * it.
   */
  /**
   * @param sagaId the saga the scenario created
   * @param unsentAtCrash outbox rows for the saga not yet published when the process "died": the
   *     command's only durable copy. Reliable mode has 1, naive mode 0 (it never had one).
   */
  record CrashOutcome(UUID sagaId, int unsentAtCrash) {}

  CrashOutcome lostEventAfterCommit() {
    cluster.with(ORDER, "fault.crash-before-publish", true).restart(ORDER);
    String customer = "f4-" + UUID.randomUUID();
    try {
      placeOrder(customer, "SKU-1", 1, "7.00");
    } catch (RestClientResponseException e) {
      // Naive: the crash surfaces in the request thread, after the commit. The order exists anyway.
    }
    UUID sagaId = sagaOf(customer);
    // With the fault on, nothing can have reached Kafka yet. Whatever survives the crash is in the
    // DB.
    cluster.stop(ORDER);
    int unsent =
        cluster
            .db(ORDER)
            .queryForObject(
                "SELECT count(*) FROM outbox WHERE saga_id = ? AND published_at IS NULL",
                Integer.class,
                sagaId);

    cluster.with(ORDER, "fault.crash-before-publish", false).start(ORDER);
    awaitSettled(sagaId);
    return new CrashOutcome(sagaId, unsent);
  }

  // --- helpers ---------------------------------------------------------------------------

  private void placeOrder(String customer, String sku, int qty, String amount) {
    RestClient.create(cluster.baseUrl(ORDER))
        .post()
        .uri("/orders")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("customerId", customer, "sku", sku, "quantity", qty, "amount", amount))
        .retrieve()
        .body(JsonNode.class);
  }

  /** Looks the saga up in order_db, because a crashed request may never have returned its ids. */
  private UUID sagaOf(String customer) {
    return cluster
        .db(ORDER)
        .queryForObject(
            "SELECT s.saga_id FROM saga_instance s JOIN orders o ON o.order_id = s.order_id"
                + " WHERE o.customer_id = ?",
            UUID.class,
            customer);
  }

  /**
   * Waits until the saga is terminal, or until {@link #SETTLE} has passed. Either way the test then
   * inspects the outcome. A saga that never settles is exactly what some scenarios demonstrate.
   */
  void awaitSettled(UUID sagaId) {
    try {
      await().atMost(SETTLE).until(() -> TERMINAL.contains(state(sagaId)));
    } catch (ConditionTimeoutException stillRunning) {
      // reported by InvariantChecker as STUCK_SAGA
    }
  }

  String state(UUID sagaId) {
    return cluster
        .db(ORDER)
        .queryForObject("SELECT state FROM saga_instance WHERE saga_id = ?", String.class, sagaId);
  }
}
