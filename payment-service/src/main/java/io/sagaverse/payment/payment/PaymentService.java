package io.sagaverse.payment.payment;

import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.PaymentMessages.ChargePayment;
import io.sagaverse.events.PaymentMessages.PaymentCharged;
import io.sagaverse.events.PaymentMessages.PaymentCommand;
import io.sagaverse.events.PaymentMessages.PaymentDeclined;
import io.sagaverse.events.PaymentMessages.PaymentRefunded;
import io.sagaverse.events.PaymentMessages.RefundPayment;
import io.sagaverse.events.SagaPayload;
import io.sagaverse.payment.messaging.IdempotencyGuard;
import io.sagaverse.payment.messaging.MessagePublisher;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Executes payment commands. It knows nothing about sagas. It just moves (fake) money and replies.
 *
 * <p><b>Naive mode:</b> ChargePayment has no idempotency. Every delivery writes another CHARGE
 * ledger row, so a duplicate charges the customer twice (failure mode F3, fixed in Phase 4).
 *
 * <p><b>RefundPayment is idempotent in every mode</b> (ADR-0008). It is keyed by sagaId, refunds at
 * most once, and if nothing was charged it writes a tombstone so a late ChargePayment is declined.
 */
@Service
public class PaymentService {

  /** Decline reason when a charge arrives for a saga that was already refunded (tombstone). */
  public static final String SAGA_CANCELLED = "SAGA_CANCELLED";

  private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

  private final JdbcClient jdbc;
  private final TransactionTemplate tx;
  private final MessagePublisher publisher;
  private final FakeGateway gateway;
  private final IdempotencyGuard guard;
  private final Clock clock;

  public PaymentService(
      JdbcClient jdbc,
      TransactionTemplate tx,
      MessagePublisher publisher,
      FakeGateway gateway,
      IdempotencyGuard guard,
      Clock clock) {
    this.jdbc = jdbc;
    this.tx = tx;
    this.publisher = publisher;
    this.gateway = gateway;
    this.guard = guard;
    this.clock = clock;
  }

  public void handle(EventEnvelope<PaymentCommand> command) {
    tx.executeWithoutResult(status -> {
      if (!guard.firstDelivery(command)) {
        log.info(
            "duplicate delivery of {} (eventId={}) skipped", command.type(), command.eventId());
        return;
      }
      SagaPayload reply =
          switch (command.payload()) {
            case ChargePayment charge -> charge(command.sagaId(), charge);
            case RefundPayment refund -> refund(command.sagaId(), refund);
          };
      publisher.publish(command.sagaId(), List.of(reply));
    });
  }

  private SagaPayload charge(UUID sagaId, ChargePayment c) {
    var now = Timestamp.from(clock.instant());

    Optional<Payment> existing = find(sagaId);
    if (existing.map(p -> p.status().equals("REFUNDED")).orElse(false)) {
      log.info("declining charge: saga was already refunded (tombstone)");
      return new PaymentDeclined(c.orderId(), SAGA_CANCELLED);
    }
    if (guard.checkBusinessKeys() && existing.isPresent()) {
      // Reliable: this saga was already charged (or declined). Repeat the answer, don't move money.
      Payment p = existing.get();
      log.info("charge for this saga already {}: replying with the stored outcome", p.status());
      return p.status().equals("CHARGED")
          ? new PaymentCharged(c.orderId(), p.paymentId(), p.amount())
          : new PaymentDeclined(c.orderId(), p.reason() == null ? p.status() : p.reason());
    }

    Optional<String> declined = gateway.decline(c);
    if (declined.isPresent()) {
      upsertPayment(UUID.randomUUID(), sagaId, c, "DECLINED", declined.get(), now);
      log.info("declined {} for {}: {}", c.amount(), c.customerId(), declined.get());
      return new PaymentDeclined(c.orderId(), declined.get());
    }

    // The money movement. Append-only, so a second charge is visible as a second row.
    insertLedger(sagaId, c.orderId(), c.customerId(), "CHARGE", c.amount(), now);
    // Naive mode reaches here on every duplicate: the upsert hides it, and the ledger shows it.
    UUID paymentId = upsertPayment(UUID.randomUUID(), sagaId, c, "CHARGED", null, now);
    log.info("charged {} to {}", c.amount(), c.customerId());
    return new PaymentCharged(c.orderId(), paymentId, c.amount());
  }

  private SagaPayload refund(UUID sagaId, RefundPayment r) {
    var now = Timestamp.from(clock.instant());
    Optional<Payment> existing = findForUpdate(sagaId);

    if (existing.isEmpty()) {
      // Nothing was charged (yet). Leave a tombstone so a late charge can't take money.
      jdbc.sql(
              """
              INSERT INTO payment (payment_id, saga_id, order_id, status, reason, created_at, updated_at)
              VALUES (:id, :saga, :order, 'REFUNDED', 'REFUNDED_BEFORE_CHARGE', :now, :now)
              """)
          .param("id", UUID.randomUUID())
          .param("saga", sagaId)
          .param("order", r.orderId())
          .param("now", now)
          .update();
      log.info("refund with nothing charged: tombstone written");
      return new PaymentRefunded(r.orderId(), null);
    }

    Payment p = existing.get();
    switch (p.status()) {
      case "CHARGED" -> {
        insertLedger(sagaId, r.orderId(), p.customerId(), "REFUND", p.amount().negate(), now);
        jdbc.sql(
                "UPDATE payment SET status = 'REFUNDED', reason = :reason, updated_at = :now"
                    + " WHERE saga_id = :saga")
            .param("reason", r.reason())
            .param("now", now)
            .param("saga", sagaId)
            .update();
        log.info("refunded {} to {} ({})", p.amount(), p.customerId(), r.reason());
        return new PaymentRefunded(r.orderId(), p.paymentId());
      }
      case "REFUNDED" -> {
        log.info("refund already done; replying again without moving money");
        return new PaymentRefunded(r.orderId(), p.amount() == null ? null : p.paymentId());
      }
      default -> {
        log.info("nothing to refund (payment {})", p.status());
        return new PaymentRefunded(r.orderId(), null);
      }
    }
  }

  // --- persistence --------------------------------------------------------------------------

  private record Payment(
      UUID paymentId,
      String status,
      @Nullable String reason,
      @Nullable String customerId,
      @Nullable BigDecimal amount) {}

  private Optional<Payment> find(UUID sagaId) {
    return jdbc.sql(
            "SELECT payment_id, status, reason, customer_id, amount FROM payment WHERE saga_id = :s")
        .param("s", sagaId)
        .query(PaymentService::mapPayment)
        .optional();
  }

  /** Row lock: two refunds for the same saga can't both see CHARGED. */
  private Optional<Payment> findForUpdate(UUID sagaId) {
    return jdbc.sql(
            "SELECT payment_id, status, reason, customer_id, amount FROM payment WHERE saga_id = :s"
                + " FOR UPDATE")
        .param("s", sagaId)
        .query(PaymentService::mapPayment)
        .optional();
  }

  private static Payment mapPayment(java.sql.ResultSet rs, int row) throws java.sql.SQLException {
    return new Payment(
        rs.getObject("payment_id", UUID.class),
        rs.getString("status"),
        rs.getString("reason"),
        rs.getString("customer_id"),
        rs.getBigDecimal("amount"));
  }

  private void insertLedger(
      UUID sagaId,
      String orderId,
      @Nullable String customerId,
      String type,
      BigDecimal amount,
      Timestamp now) {
    jdbc.sql(
            """
            INSERT INTO ledger (saga_id, order_id, customer_id, entry_type, amount, created_at)
            VALUES (:saga, :order, :customer, :type, :amount, :now)
            """)
        .param("saga", sagaId)
        .param("order", orderId)
        .param("customer", customerId)
        .param("type", type)
        .param("amount", amount)
        .param("now", now)
        .update();
  }

  private UUID upsertPayment(
      UUID id,
      UUID sagaId,
      ChargePayment c,
      String status,
      @Nullable String reason,
      Timestamp now) {
    return jdbc.sql(
            """
            INSERT INTO payment
              (payment_id, saga_id, order_id, customer_id, amount, status, reason, created_at, updated_at)
            VALUES (:id, :saga, :order, :customer, :amount, :status, :reason, :now, :now)
            ON CONFLICT (saga_id) DO UPDATE SET updated_at = EXCLUDED.updated_at
            RETURNING payment_id
            """)
        .param("id", id)
        .param("saga", sagaId)
        .param("order", c.orderId())
        .param("customer", c.customerId())
        .param("amount", c.amount())
        .param("status", status)
        .param("reason", reason)
        .param("now", now)
        .query(UUID.class)
        .single();
  }
}
