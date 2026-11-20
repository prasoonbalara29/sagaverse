package io.sagaverse.order.saga;

/** States of the orchestrated saga. See the state diagram in README.md. */
public enum SagaState {
  /** ChargePayment sent, waiting for the payment reply. */
  PAYMENT_PENDING,
  /** Payment charged, ReserveStock sent, waiting for the inventory reply. */
  INVENTORY_PENDING,
  /** A later step failed. Compensations sent, waiting for their acks. */
  COMPENSATING,
  /** Payment charged and stock reserved. Order CONFIRMED. */
  COMPLETED,
  /** Payment declined. Nothing to undo. Order CANCELLED. */
  FAILED,
  /** Every completed step was undone. Order CANCELLED. */
  COMPENSATED,
  /** Compensations kept failing. Needs a human (ADR-0008). */
  COMPENSATION_FAILED;

  /** Terminal states never change again, and the sweeper ignores them. */
  public boolean isTerminal() {
    return switch (this) {
      case PAYMENT_PENDING, INVENTORY_PENDING, COMPENSATING -> false;
      case COMPLETED, FAILED, COMPENSATED, COMPENSATION_FAILED -> true;
    };
  }
}
