package io.sagaverse.inventory.stock;

import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.InventoryMessages.InventoryCommand;
import io.sagaverse.events.InventoryMessages.ReleaseStock;
import io.sagaverse.events.InventoryMessages.ReserveStock;
import io.sagaverse.events.InventoryMessages.StockReserved;
import io.sagaverse.events.InventoryMessages.StockUnavailable;
import io.sagaverse.events.SagaPayload;
import io.sagaverse.inventory.messaging.MessagePublisher;
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
 * Executes inventory commands. It knows nothing about sagas. It reserves stock and replies.
 *
 * <p><b>Naive mode (Phase 2):</b> there is no idempotency. Every delivery of a ReserveStock
 * decrements stock again, so a duplicate delivery reserves twice (failure mode F3, fixed in Phase
 * 4).
 */
@Service
public class InventoryService {

  private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

  private final JdbcClient jdbc;
  private final TransactionTemplate tx;
  private final MessagePublisher publisher;
  private final Clock clock;

  public InventoryService(
      JdbcClient jdbc, TransactionTemplate tx, MessagePublisher publisher, Clock clock) {
    this.jdbc = jdbc;
    this.tx = tx;
    this.publisher = publisher;
    this.clock = clock;
  }

  public void handle(EventEnvelope<InventoryCommand> command) {
    tx.executeWithoutResult(status -> {
      switch (command.payload()) {
        case ReserveStock reserve -> reserve(command.sagaId(), reserve);
        case ReleaseStock release ->
            log.warn("ReleaseStock is not implemented until Phase 3; ignoring");
      }
    });
  }

  private void reserve(UUID sagaId, ReserveStock r) {
    var now = Timestamp.from(clock.instant());

    // Atomic check-and-decrement: one statement, so two concurrent reservations can't both take the
    // last unit.
    int updated =
        jdbc.sql(
                """
                UPDATE stock SET available = available - :qty, updated_at = :now
                 WHERE sku = :sku AND available >= :qty
                """)
            .param("qty", r.quantity())
            .param("sku", r.sku())
            .param("now", now)
            .update();

    SagaPayload reply;
    if (updated == 0) {
      reply = new StockUnavailable(r.orderId(), r.sku(), StockUnavailable.OUT_OF_STOCK);
      log.info("cannot reserve {} x {}: out of stock", r.quantity(), r.sku());
    } else {
      // Naive: upsert blindly and add up the quantity, so a duplicate silently doubles the hold.
      jdbc.sql(
              """
              INSERT INTO reservation (saga_id, order_id, sku, quantity, status, created_at, updated_at)
              VALUES (:saga, :order, :sku, :qty, 'RESERVED', :now, :now)
              ON CONFLICT (saga_id) DO UPDATE
                SET quantity = reservation.quantity + EXCLUDED.quantity, updated_at = EXCLUDED.updated_at
              """)
          .param("saga", sagaId)
          .param("order", r.orderId())
          .param("sku", r.sku())
          .param("qty", r.quantity())
          .param("now", now)
          .update();
      reply = new StockReserved(r.orderId(), r.sku(), r.quantity());
      log.info("reserved {} x {}", r.quantity(), r.sku());
    }
    publisher.publish(sagaId, List.of(reply));
  }
}
