package io.sagaverse.e2e;

import static io.sagaverse.e2e.SagaverseCluster.Service.INVENTORY;
import static io.sagaverse.e2e.SagaverseCluster.Service.ORDER;
import static io.sagaverse.e2e.SagaverseCluster.Service.PAYMENT;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.sagaverse.events.Topics;
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

  /** F1: amount above payment.decline-amount-over (the tests set 100.00). */
  UUID paymentDeclined() {
    return placeAndSettle("f1-", "SKU-1", 1, "500.00");
  }

  /** F2: SKU-EMPTY has no stock, so the charge must be refunded. */
  UUID outOfStock() {
    return placeAndSettle("f2-", "SKU-EMPTY", 1, "10.00");
  }

  /**
   * F3a, redelivery: once the saga has completed, the exact ChargePayment and ReserveStock records
   * (same bytes, same eventId) are delivered again. That's what Kafka does when a consumer dies
   * before committing its offset.
   */
  UUID duplicateRedelivery() {
    UUID sagaId = placeAndSettle("f3a-", "SKU-1", 1, "11.00");
    replayCommands(sagaId, false);
    return sagaId;
  }

  /**
   * F3b, logical resend: the same commands again, but each in a <b>new</b> envelope (new eventId,
   * same sagaId). That's what a retrying orchestrator does (the Phase 5 sweeper). An eventId inbox
   * alone can't catch it.
   */
  UUID logicalResend() {
    UUID sagaId = placeAndSettle("f3b-", "SKU-1", 1, "12.00");
    replayCommands(sagaId, true);
    return sagaId;
  }

  /**
   * @param sagaId the saga the scenario created
   * @param attemptsWhileDown how many times the stuck step had been sent while inventory was down
   * @param stateWhileDown saga state just before inventory came back
   * @param violationsWhileDown invariant violations just before inventory came back
   */
  record OutageOutcome(
      UUID sagaId,
      int attemptsWhileDown,
      String stateWhileDown,
      List<InvariantChecker.Violation> violationsWhileDown) {}

  /**
   * F5: inventory-service is down for longer than one step timeout, but less than the whole step
   * budget, then comes back. Kafka keeps the unread ReserveStock, so the saga can finish. The
   * difference between modes is that reliable mode also resends in the meantime, and must still
   * reserve only once.
   */
  OutageOutcome inventoryBriefOutage() {
    cluster.stop(INVENTORY);
    UUID sagaId = placeWithoutSettling("f5-", "SKU-2", 1, "8.00");
    await().atMost(SETTLE).until(() -> "INVENTORY_PENDING".equals(state(sagaId)));
    // Keep inventory down past one step timeout (2 s in tests), so the deadline really passes.
    await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(4)).until(() -> true);
    var outcome = snapshot(sagaId);
    cluster.start(INVENTORY);
    awaitSettled(sagaId);
    return outcome;
  }

  /**
   * F6: inventory-service stays down past the whole step budget (3 attempts x 2 s in tests).
   * Reliable mode gives up and compensates: it refunds right away, and the stock release waits for
   * inventory. Naive mode just waits. Then inventory comes back.
   */
  OutageOutcome inventoryLongOutage() {
    cluster.stop(INVENTORY);
    UUID sagaId = placeWithoutSettling("f6-", "SKU-2", 1, "9.00");
    try {
      // Reliable: wait until the refund is confirmed and only the stock release is outstanding.
      await()
          .atMost(SETTLE.plusSeconds(5))
          .until(() -> List.of("ReleaseStock").equals(pending(sagaId)));
    } catch (ConditionTimeoutException naiveJustWaits) {
      // Naive: nothing happens while inventory is down. That's the point.
    }
    var outcome = snapshot(sagaId);
    cluster.start(INVENTORY);
    awaitSettled(sagaId);
    return outcome;
  }

  /**
   * F8: the payment gateway fails the first {@code failures} refunds of an out-of-stock saga. A
   * refund can't be declined, only delayed (ADR-0008). Afterwards payment-service is restarted
   * healthy. Waits up to {@code settle} for the saga to finish.
   */
  UUID refundFailsTransiently(int failures, Duration settle) {
    cluster.with(PAYMENT, "payment.refund-fail-times", failures).restart(PAYMENT);
    try {
      UUID sagaId = placeWithoutSettling("f8-", "SKU-EMPTY", 1, "10.00");
      try {
        await().atMost(settle).until(() -> TERMINAL.contains(state(sagaId)));
      } catch (ConditionTimeoutException stillCompensating) {
        // reported by InvariantChecker as STUCK_SAGA
      }
      return sagaId;
    } finally {
      cluster.with(PAYMENT, "payment.refund-fail-times", 0).restart(PAYMENT);
    }
  }

  /**
   * F9: an out-of-stock saga is cancelled (COMPENSATED). Then the SKU is restocked, and a resend of
   * its ReserveStock (new eventId, like a sweeper resend still in flight) finally arrives. It
   * succeeds now, and the orchestrator must give the stock back.
   */
  UUID lateReservationAfterCancellation() {
    var inventory = cluster.db(INVENTORY);
    UUID sagaId = placeAndSettle("f9-", "SKU-EMPTY", 1, "10.00");
    inventory.update("UPDATE stock SET available = 5 WHERE sku = 'SKU-EMPTY'");
    try {
      String orderId = orderOf(sagaId);
      var kafka = new KafkaTool(cluster.bootstrapServers());
      var original = kafka.find(Topics.INVENTORY_COMMANDS, orderId, "ReserveStock");
      kafka.send(Topics.INVENTORY_COMMANDS, orderId, KafkaTool.withNewEventId(original.value()));
      // Reserve, then the orchestrator's ReleaseStock, end with the reservation RELEASED.
      await()
          .atMost(SETTLE)
          .until(() ->
              "RELEASED"
                  .equals(
                      inventory.query(
                          "SELECT status FROM reservation WHERE saga_id = ?",
                          rs -> rs.next() ? rs.getString(1) : null,
                          sagaId)));
    } catch (ConditionTimeoutException leftReserved) {
      // reported by InvariantChecker as ORPHAN_RESERVATION
    }
    return sagaId;
  }

  /** Undo F9's restock so other scenarios still see SKU-EMPTY as empty. */
  void emptySkuEmpty() {
    cluster
        .db(INVENTORY)
        .update(
            "UPDATE stock SET available = 0 WHERE sku = 'SKU-EMPTY' AND NOT EXISTS"
                + " (SELECT 1 FROM reservation WHERE sku = 'SKU-EMPTY' AND status = 'RESERVED')");
  }

  // --- helpers ---------------------------------------------------------------------------

  private OutageOutcome snapshot(UUID sagaId) {
    return new OutageOutcome(
        sagaId, attempts(sagaId), state(sagaId), new InvariantChecker(cluster).check(sagaId));
  }

  private UUID placeWithoutSettling(String prefix, String sku, int qty, String amount) {
    String customer = prefix + UUID.randomUUID();
    placeOrder(customer, sku, qty, amount);
    return sagaOf(customer);
  }

  int attempts(UUID sagaId) {
    return cluster
        .db(ORDER)
        .queryForObject(
            "SELECT step_attempts FROM saga_instance WHERE saga_id = ?", Integer.class, sagaId);
  }

  List<String> pending(UUID sagaId) {
    String json =
        cluster
            .db(ORDER)
            .queryForObject(
                "SELECT compensations_pending::text FROM saga_instance WHERE saga_id = ?",
                String.class,
                sagaId);
    return json.equals("[]") ? List.of() : List.of(json.replaceAll("[\\[\\]\" ]", "").split(","));
  }

  private UUID placeAndSettle(String prefix, String sku, int qty, String amount) {
    String customer = prefix + UUID.randomUUID();
    placeOrder(customer, sku, qty, amount);
    UUID sagaId = sagaOf(customer);
    awaitSettled(sagaId);
    return sagaId;
  }

  private void replayCommands(UUID sagaId, boolean newEventId) {
    String orderId = orderOf(sagaId);
    var kafka = new KafkaTool(cluster.bootstrapServers());
    for (String[] tt :
        new String[][] {
          {Topics.PAYMENT_COMMANDS, "ChargePayment"}, {Topics.INVENTORY_COMMANDS, "ReserveStock"}
        }) {
      var original = kafka.find(tt[0], orderId, tt[1]);
      String value = newEventId ? KafkaTool.withNewEventId(original.value()) : original.value();
      kafka.send(tt[0], original.key(), value);
    }
    kafka.awaitConsumed("payment-service", Topics.PAYMENT_COMMANDS);
    kafka.awaitConsumed("inventory-service", Topics.INVENTORY_COMMANDS);
  }

  private String orderOf(UUID sagaId) {
    return cluster
        .db(ORDER)
        .queryForObject(
            "SELECT order_id FROM saga_instance WHERE saga_id = ?", String.class, sagaId);
  }

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
