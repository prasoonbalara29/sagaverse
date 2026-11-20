package io.sagaverse.payment.payment;

import java.math.BigDecimal;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Knobs of the fake payment gateway ({@code payment.*}), used to trigger failure paths.
 *
 * @param declineAmountOver charges above this amount are declined (deterministic, used by tests)
 * @param failRate probability in [0, 1] that the gateway declines any charge (for demos)
 */
@ConfigurationProperties("payment")
public record PaymentProperties(
    @DefaultValue("1000.00") BigDecimal declineAmountOver, @DefaultValue("0.0") double failRate) {

  public PaymentProperties {
    if (failRate < 0 || failRate > 1) {
      throw new IllegalArgumentException("payment.fail-rate must be in [0, 1], was " + failRate);
    }
  }
}
