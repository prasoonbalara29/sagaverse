package io.sagaverse.events;

import static io.sagaverse.events.Contracts.requireNonBlank;
import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.util.UUID;

/** Contracts exchanged between order-service (orchestrator) and payment-service. */
public final class PaymentMessages {

  private PaymentMessages() {}

  /** Sent by the orchestrator on {@link Topics#PAYMENT_COMMANDS}. */
  public sealed interface PaymentCommand extends SagaPayload permits ChargePayment, RefundPayment {}

  /** Sent by payment-service on {@link Topics#PAYMENT_REPLIES}. */
  public sealed interface PaymentReply extends SagaPayload
      permits PaymentCharged, PaymentDeclined, PaymentRefunded {}

  /** Take {@code amount} from the customer. */
  public record ChargePayment(String orderId, String customerId, BigDecimal amount)
      implements PaymentCommand {
    public ChargePayment {
      requireNonBlank(orderId, "orderId");
      requireNonBlank(customerId, "customerId");
      requireNonNull(amount, "amount");
      if (amount.signum() <= 0) {
        throw new IllegalArgumentException("amount must be positive, was " + amount);
      }
    }
  }

  /**
   * Compensation for {@link ChargePayment}. Idempotent: refunding something that was never charged,
   * or was already refunded, is a no-op that still replies {@link PaymentRefunded}.
   */
  public record RefundPayment(String orderId, String reason) implements PaymentCommand {
    public RefundPayment {
      requireNonBlank(orderId, "orderId");
      requireNonBlank(reason, "reason");
    }
  }

  /** The charge succeeded. */
  public record PaymentCharged(String orderId, UUID paymentId, BigDecimal amount)
      implements PaymentReply {
    public PaymentCharged {
      requireNonBlank(orderId, "orderId");
      requireNonNull(paymentId, "paymentId");
      requireNonNull(amount, "amount");
    }
  }

  /** The charge was declined. This is a business outcome, not an error. */
  public record PaymentDeclined(String orderId, String reason) implements PaymentReply {
    public PaymentDeclined {
      requireNonBlank(orderId, "orderId");
      requireNonBlank(reason, "reason");
    }
  }

  /**
   * The refund is done.
   *
   * @param paymentId the refunded payment, or {@code null} when there was nothing to refund
   */
  public record PaymentRefunded(String orderId, UUID paymentId) implements PaymentReply {
    public PaymentRefunded {
      requireNonBlank(orderId, "orderId");
    }
  }
}
