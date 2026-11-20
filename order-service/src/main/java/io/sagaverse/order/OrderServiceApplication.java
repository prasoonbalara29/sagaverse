package io.sagaverse.order;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** REST entry point and saga orchestrator. */
@SpringBootApplication
public class OrderServiceApplication {

  /**
   * This service reads {@code order-service.yml}, not {@code application.yml}. Every service jar
   * can then sit on one classpath (the e2e tests boot all three in one JVM) without one service's
   * config shadowing another's.
   */
  public static final String CONFIG_NAME = "spring.config.name=order-service";

  /** The application as the e2e tests and {@link #main} both start it. */
  public static SpringApplicationBuilder application() {
    return new SpringApplicationBuilder(OrderServiceApplication.class).properties(CONFIG_NAME);
  }

  public static void main(String[] args) {
    application().run(args);
  }
}
