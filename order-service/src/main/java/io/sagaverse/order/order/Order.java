package io.sagaverse.order.order;

import java.math.BigDecimal;
import java.time.Instant;

/** One row of {@code orders}: what the customer asked for and where it stands. */
public record Order(
    String orderId,
    String customerId,
    String sku,
    int quantity,
    BigDecimal amount,
    Status status,
    Instant createdAt,
    Instant updatedAt) {

  /** Order status as the customer sees it. The saga state carries the detail. */
  public enum Status {
    PENDING,
    CONFIRMED,
    CANCELLED,
    /** Compensation failed: money or stock may still be held. Needs a human (ADR-0008). */
    CANCELLED_NEEDS_ATTENTION
  }

  public Order withStatus(Status newStatus, Instant at) {
    return new Order(orderId, customerId, sku, quantity, amount, newStatus, createdAt, at);
  }
}
