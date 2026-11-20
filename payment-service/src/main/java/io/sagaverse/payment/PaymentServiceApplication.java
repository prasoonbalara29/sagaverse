package io.sagaverse.payment;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** Fake payment gateway: charges and refunds. */
@SpringBootApplication
public class PaymentServiceApplication {

  /**
   * This service reads {@code payment-service.yml}, not {@code application.yml}. Every service jar
   * can then sit on one classpath (the e2e tests boot all three in one JVM) without one service's
   * config shadowing another's.
   */
  public static final String CONFIG_NAME = "spring.config.name=payment-service";

  /** The application as the e2e tests and {@link #main} both start it. */
  public static SpringApplicationBuilder application() {
    return new SpringApplicationBuilder(PaymentServiceApplication.class).properties(CONFIG_NAME);
  }

  public static void main(String[] args) {
    application().run(args);
  }
}
