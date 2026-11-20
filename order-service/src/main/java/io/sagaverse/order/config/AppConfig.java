package io.sagaverse.order.config;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.sagaverse.order.saga.SagaProperties;
import java.time.Clock;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Wiring shared by the whole service. */
@Configuration
@EnableConfigurationProperties(SagaProperties.class)
public class AppConfig {

  /** All "now" values go through this, so tests can control time (ADR-0003). */
  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  /**
   * Payloads are read as JSON trees before conversion. Exact BigDecimal nodes keep 42.50 from
   * becoming the double 42.5 (ADR-0011).
   */
  @Bean
  Jackson2ObjectMapperBuilderCustomizer exactDecimalNodes() {
    return builder ->
        builder.postConfigurer(m -> m.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true)));
  }
}
