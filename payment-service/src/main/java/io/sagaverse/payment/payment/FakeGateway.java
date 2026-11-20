package io.sagaverse.payment.payment;

import io.sagaverse.events.PaymentMessages.ChargePayment;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

/** Stands in for a card processor. No real money and no network, just rules from config. */
@Component
@EnableConfigurationProperties(PaymentProperties.class)
public class FakeGateway {

  public static final String AMOUNT_LIMIT_EXCEEDED = "AMOUNT_LIMIT_EXCEEDED";
  public static final String GATEWAY_DECLINED = "GATEWAY_DECLINED";

  private final PaymentProperties props;

  public FakeGateway(PaymentProperties props) {
    this.props = props;
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
