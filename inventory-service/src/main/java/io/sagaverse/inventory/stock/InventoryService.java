package io.sagaverse.inventory.stock;

import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.InventoryMessages.InventoryCommand;
import io.sagaverse.events.InventoryMessages.ReleaseStock;
import io.sagaverse.events.InventoryMessages.ReserveStock;
import io.sagaverse.events.InventoryMessages.StockReleased;
import io.sagaverse.events.InventoryMessages.StockReserved;
import io.sagaverse.events.InventoryMessages.StockUnavailable;
import io.sagaverse.events.SagaPayload;
import io.sagaverse.inventory.messaging.MessagePublisher;
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
 * Executes inventory commands. It knows nothing about sagas. It reserves stock and replies.
 *
 * <p><b>Naive mode:</b> ReserveStock has no idempotency. Every delivery decrements stock again, so
 * a duplicate reserves twice (failure mode F3, fixed in Phase 4).
 *
 * <p><b>ReleaseStock is idempotent in every mode</b> (ADR-0008). It releases at most once, and if
 * nothing was reserved it writes a RELEASED tombstone so a late ReserveStock is refused.
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
      SagaPayload reply =
          switch (command.payload()) {
            case ReserveStock reserve -> reserve(command.sagaId(), reserve);
            case ReleaseStock release -> release(command.sagaId(), release);
          };
      publisher.publish(command.sagaId(), List.of(reply));
    });
  }

  private SagaPayload reserve(UUID sagaId, ReserveStock r) {
    var now = Timestamp.from(clock.instant());

    if (findForUpdate(sagaId).map(x -> x.status().equals("RELEASED")).orElse(false)) {
      log.info("refusing reservation: saga was already released (tombstone)");
      return new StockUnavailable(r.orderId(), r.sku(), StockUnavailable.SAGA_CANCELLED);
    }

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
    if (updated == 0) {
      log.info("cannot reserve {} x {}: out of stock", r.quantity(), r.sku());
      return new StockUnavailable(r.orderId(), r.sku(), StockUnavailable.OUT_OF_STOCK);
    }

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
    log.info("reserved {} x {}", r.quantity(), r.sku());
    return new StockReserved(r.orderId(), r.sku(), r.quantity());
  }

  private SagaPayload release(UUID sagaId, ReleaseStock r) {
    var now = Timestamp.from(clock.instant());
    Optional<Reservation> existing = findForUpdate(sagaId);

    if (existing.isEmpty()) {
      jdbc.sql(
              """
              INSERT INTO reservation (saga_id, order_id, sku, quantity, status, created_at, updated_at)
              VALUES (:saga, :order, NULL, 0, 'RELEASED', :now, :now)
              """)
          .param("saga", sagaId)
          .param("order", r.orderId())
          .param("now", now)
          .update();
      log.info("release with nothing reserved: tombstone written");
      return new StockReleased(r.orderId(), false);
    }

    Reservation res = existing.get();
    if (res.status().equals("RELEASED")) {
      log.info("already released; replying again without touching stock");
      return new StockReleased(r.orderId(), false);
    }

    jdbc.sql("UPDATE stock SET available = available + :qty, updated_at = :now WHERE sku = :sku")
        .param("qty", res.quantity())
        .param("now", now)
        .param("sku", res.sku())
        .update();
    jdbc.sql("UPDATE reservation SET status = 'RELEASED', updated_at = :now WHERE saga_id = :saga")
        .param("now", now)
        .param("saga", sagaId)
        .update();
    log.info("released {} x {} ({})", res.quantity(), res.sku(), r.reason());
    return new StockReleased(r.orderId(), true);
  }

  private record Reservation(@Nullable String sku, int quantity, String status) {}

  /** Row lock: a reserve and a release for the same saga are serialized. */
  private Optional<Reservation> findForUpdate(UUID sagaId) {
    return jdbc.sql("SELECT sku, quantity, status FROM reservation WHERE saga_id = :s FOR UPDATE")
        .param("s", sagaId)
        .query((rs, row) ->
            new Reservation(rs.getString("sku"), rs.getInt("quantity"), rs.getString("status")))
        .optional();
  }
}
