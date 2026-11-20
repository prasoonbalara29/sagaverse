package io.sagaverse.payment.payment;

import io.sagaverse.events.PaymentMessages.ChargePayment;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

/** Stands in for a card processor. No real money and no network, just rules from config. */
@Component
@EnableConfigurationProperties(PaymentProperties.class)
public class FakeGateway {

  public static final String AMOUNT_LIMIT_EXCEEDED = "AMOUNT_LIMIT_EXCEEDED";
  public static final String GATEWAY_DECLINED = "GATEWAY_DECLINED";

  private final PaymentProperties props;
  private final AtomicInteger refundFailuresLeft;

  public FakeGateway(PaymentProperties props) {
    this.props = props;
    this.refundFailuresLeft = new AtomicInteger(props.refundFailTimes());
  }

  /**
   * Gives the money back. Fails transiently while {@code payment.refund-fail-times} failures are
   * left. The exception is retryable, so Kafka retries, then the DLT, then the orchestrator resends
   * (F8).
   */
  public void refund(String customerId, java.math.BigDecimal amount) {
    if (refundFailuresLeft.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
      throw new GatewayUnavailableException(
          "refund of " + amount + " to " + customerId + " failed: gateway unavailable (injected)");
    }
  }

  /** A transient gateway failure. Retrying later can succeed. */
  public static final class GatewayUnavailableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    GatewayUnavailableException(String message) {
      super(message);
    }
  }

  /** Returns the decline reason, or empty if the charge is approved. */
  public Optional<String> decline(ChargePayment charge) {
    if (charge.amount().compareTo(props.declineAmountOver()) > 0) {
      return Optional.of(AMOUNT_LIMIT_EXCEEDED);
    }
    if (props.failRate() > 0 && ThreadLocalRandom.current().nextDouble() < props.failRate()) {
      return Optional.of(GATEWAY_DECLINED);
    }
    return Optional.empty();
  }
}
