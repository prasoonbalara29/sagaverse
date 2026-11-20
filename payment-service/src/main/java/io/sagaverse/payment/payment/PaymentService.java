package io.sagaverse.payment.payment;

import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.PaymentMessages.ChargePayment;
import io.sagaverse.events.PaymentMessages.PaymentCharged;
import io.sagaverse.events.PaymentMessages.PaymentCommand;
import io.sagaverse.events.PaymentMessages.RefundPayment;
import io.sagaverse.payment.messaging.MessagePublisher;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Executes payment commands. It knows nothing about sagas. It just moves (fake) money and replies.
 *
 * <p><b>Naive mode (Phase 2):</b> there is no idempotency. Every delivery of a ChargePayment writes
 * another CHARGE ledger row. A duplicate delivery charges the customer twice (failure mode F3,
 * demonstrated and fixed in Phase 4).
 */
@Service
public class PaymentService {

  private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

  private final JdbcClient jdbc;
  private final TransactionTemplate tx;
  private final MessagePublisher publisher;
  private final Clock clock;

  public PaymentService(
      JdbcClient jdbc, TransactionTemplate tx, MessagePublisher publisher, Clock clock) {
    this.jdbc = jdbc;
    this.tx = tx;
    this.publisher = publisher;
    this.clock = clock;
  }

  public void handle(EventEnvelope<PaymentCommand> command) {
    tx.executeWithoutResult(status -> {
      switch (command.payload()) {
        case ChargePayment charge -> charge(command.sagaId(), charge);
        case RefundPayment refund ->
            log.warn("RefundPayment is not implemented until Phase 3; ignoring");
      }
    });
  }

  private void charge(UUID sagaId, ChargePayment c) {
    var now = Timestamp.from(clock.instant());

    // The money movement. Append-only, so a second charge is visible as a second row.
    jdbc.sql(
            """
            INSERT INTO ledger (saga_id, order_id, customer_id, entry_type, amount, created_at)
            VALUES (:saga, :order, :customer, 'CHARGE', :amount, :now)
            """)
        .param("saga", sagaId)
        .param("order", c.orderId())
        .param("customer", c.customerId())
        .param("amount", c.amount())
        .param("now", now)
        .update();

    // The per-saga summary row. Naive: upsert blindly, so a duplicate doesn't fail and nothing
    // notices it already happened.
    UUID paymentId =
        jdbc.sql(
                """
                INSERT INTO payment
                  (payment_id, saga_id, order_id, customer_id, amount, status, created_at, updated_at)
                VALUES (:id, :saga, :order, :customer, :amount, 'CHARGED', :now, :now)
                ON CONFLICT (saga_id) DO UPDATE SET updated_at = EXCLUDED.updated_at
                RETURNING payment_id
                """)
            .param("id", UUID.randomUUID())
            .param("saga", sagaId)
            .param("order", c.orderId())
            .param("customer", c.customerId())
            .param("amount", c.amount())
            .param("now", now)
            .query(UUID.class)
            .single();

    log.info("charged {} to {}", c.amount(), c.customerId());
    publisher.publish(sagaId, List.of(new PaymentCharged(c.orderId(), paymentId, c.amount())));
  }
}
