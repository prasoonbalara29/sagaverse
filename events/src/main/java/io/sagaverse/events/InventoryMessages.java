package io.sagaverse.events;

import static io.sagaverse.events.Contracts.requireNonBlank;

/** Contracts exchanged between order-service (orchestrator) and inventory-service. */
public final class InventoryMessages {

  private InventoryMessages() {}

  /** Sent by the orchestrator on {@link Topics#INVENTORY_COMMANDS}. */
  public sealed interface InventoryCommand extends SagaPayload permits ReserveStock, ReleaseStock {}

  /** Sent by inventory-service on {@link Topics#INVENTORY_REPLIES}. */
  public sealed interface InventoryReply extends SagaPayload
      permits StockReserved, StockUnavailable, StockReleased {}

  /** Reserve {@code quantity} units of {@code sku} for this order. */
  public record ReserveStock(String orderId, String sku, int quantity) implements InventoryCommand {
    public ReserveStock {
      requireNonBlank(orderId, "orderId");
      requireNonBlank(sku, "sku");
      Contracts.requirePositive(quantity, "quantity");
    }
  }

  /**
   * Compensation for {@link ReserveStock}. Idempotent. If nothing was reserved yet, inventory
   * records a RELEASED tombstone so a late ReserveStock for the same saga is refused (ADR-0008).
   */
  public record ReleaseStock(String orderId, String reason) implements InventoryCommand {
    public ReleaseStock {
      requireNonBlank(orderId, "orderId");
      requireNonBlank(reason, "reason");
    }
  }

  /** The stock is reserved. */
  public record StockReserved(String orderId, String sku, int quantity) implements InventoryReply {
    public StockReserved {
      requireNonBlank(orderId, "orderId");
      requireNonBlank(sku, "sku");
      Contracts.requirePositive(quantity, "quantity");
    }
  }

  /**
   * The stock could not be reserved. This is a business outcome, not an error.
   *
   * @param reason {@link #OUT_OF_STOCK} or {@link #SAGA_CANCELLED}
   */
  public record StockUnavailable(String orderId, String sku, String reason)
      implements InventoryReply {
    public static final String OUT_OF_STOCK = "OUT_OF_STOCK";
    public static final String SAGA_CANCELLED = "SAGA_CANCELLED";

    public StockUnavailable {
      requireNonBlank(orderId, "orderId");
      requireNonBlank(sku, "sku");
      requireNonBlank(reason, "reason");
    }
  }

  /**
   * The release is done.
   *
   * @param released {@code true} if a reservation was given back, {@code false} if there was
   *     nothing to release (a tombstone was written instead)
   */
  public record StockReleased(String orderId, boolean released) implements InventoryReply {
    public StockReleased {
      requireNonBlank(orderId, "orderId");
    }
  }
}
