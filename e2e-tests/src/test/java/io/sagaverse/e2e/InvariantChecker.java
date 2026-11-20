package io.sagaverse.e2e;

import static io.sagaverse.e2e.SagaverseCluster.Service.INVENTORY;
import static io.sagaverse.e2e.SagaverseCluster.Service.ORDER;
import static io.sagaverse.e2e.SagaverseCluster.Service.PAYMENT;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The business rules that must hold across all three databases once a saga has had time to finish,
 * whatever went wrong on the way (ADR-0010). Naive mode breaks them, reliable mode must not.
 *
 * <p>It reads each service's DB directly, the way an auditor would. That's fine in a test. A
 * service never does it.
 */
final class InvariantChecker {

  /** A broken rule for one saga. {@code detail} carries the numbers, for the README table. */
  record Violation(Kind kind, UUID sagaId, String detail) {
    @Override
    public String toString() {
      return kind + " saga=" + sagaId + " " + detail;
    }
  }

  enum Kind {
    /** More than one CHARGE ledger entry for one saga. */
    DOUBLE_CHARGE,
    /** Order CANCELLED but money is still taken (net ledger is not zero). */
    UNREFUNDED_CHARGE,
    /** Order CONFIRMED but the net charge is not the order amount. */
    WRONG_CHARGE,
    /** More stock held for the saga than the order asked for. */
    DOUBLE_RESERVATION,
    /** Order CANCELLED but stock is still held. */
    ORPHAN_RESERVATION,
    /** The saga is not in a terminal state and its step deadline has passed. */
    STUCK_SAGA
  }

  private final SagaverseCluster cluster;

  InvariantChecker(SagaverseCluster cluster) {
    this.cluster = cluster;
  }

  List<Violation> check(UUID sagaId) {
    List<Violation> out = new ArrayList<>();
    Map<String, Object> saga =
        cluster
            .db(ORDER)
            .queryForMap(
                """
                SELECT s.state, s.deadline_at, o.status, o.amount, o.quantity
                  FROM saga_instance s JOIN orders o ON o.order_id = s.order_id
                 WHERE s.saga_id = ?
                """,
                sagaId);
    String state = (String) saga.get("state");
    String status = (String) saga.get("status");
    BigDecimal amount = (BigDecimal) saga.get("amount");
    int quantity = (Integer) saga.get("quantity");
    Timestamp deadline = (Timestamp) saga.get("deadline_at");

    var ledger =
        cluster
            .db(PAYMENT)
            .queryForMap(
                """
                SELECT count(*) FILTER (WHERE entry_type = 'CHARGE') AS charges,
                       coalesce(sum(amount), 0) AS net
                  FROM ledger WHERE saga_id = ?
                """,
                sagaId);
    long charges = ((Number) ledger.get("charges")).longValue();
    BigDecimal net = (BigDecimal) ledger.get("net");

    Integer held =
        cluster
            .db(INVENTORY)
            .queryForObject(
                "SELECT coalesce(sum(quantity), 0) FROM reservation"
                    + " WHERE saga_id = ? AND status = 'RESERVED'",
                Integer.class,
                sagaId);

    if (charges > 1) {
      out.add(new Violation(Kind.DOUBLE_CHARGE, sagaId, "chargeRows=" + charges + " net=" + net));
    }
    if (status.startsWith("CANCELLED") && net.signum() != 0) {
      out.add(new Violation(Kind.UNREFUNDED_CHARGE, sagaId, "net=" + net));
    }
    if (status.equals("CONFIRMED") && net.compareTo(amount) != 0) {
      out.add(new Violation(Kind.WRONG_CHARGE, sagaId, "net=" + net + " amount=" + amount));
    }
    if (held > quantity) {
      out.add(
          new Violation(Kind.DOUBLE_RESERVATION, sagaId, "held=" + held + " ordered=" + quantity));
    }
    if (status.startsWith("CANCELLED") && held > 0) {
      out.add(new Violation(Kind.ORPHAN_RESERVATION, sagaId, "held=" + held));
    }
    boolean terminal =
        List.of("COMPLETED", "FAILED", "COMPENSATED", "COMPENSATION_FAILED").contains(state);
    if (!terminal && deadline != null && deadline.toInstant().isBefore(Instant.now())) {
      out.add(
          new Violation(
              Kind.STUCK_SAGA,
              sagaId,
              "state=" + state + " order=" + status + " deadline=" + deadline.toInstant()));
    }
    return out;
  }
}
