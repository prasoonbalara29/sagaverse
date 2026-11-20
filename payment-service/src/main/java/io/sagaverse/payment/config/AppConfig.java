package io.sagaverse.payment.config;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Clock;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Wiring shared by the whole service. Deliberately a copy of order-service's (ADR-0001). */
@Configuration
public class AppConfig {

  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  /** Exact BigDecimal tree nodes: money must not pass through double (ADR-0011). */
  @Bean
  Jackson2ObjectMapperBuilderCustomizer exactDecimalNodes() {
    return builder ->
        builder.postConfigurer(m -> m.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true)));
  }
}
