package io.sagaverse.order.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KafkaErrorHandlingTest {

  @Test
  void dlqLogLinesGetTheSagaIdWhenTheRecordStillContainsOne() {
    String json =
        "{\"eventId\":\"x\", \"sagaId\" : \"0b8f1c1e-7a52-4a7c-9c43-5f7ad1f8a1b2\",\"type\":\"?\"}";

    assertThat(KafkaErrorHandling.sagaIdOf(json)).isEqualTo("0b8f1c1e-7a52-4a7c-9c43-5f7ad1f8a1b2");
  }

  @Test
  void garbageOrNonStringValuesGiveUnknown() {
    assertThat(KafkaErrorHandling.sagaIdOf("not json {")).isEqualTo("unknown");
    assertThat(KafkaErrorHandling.sagaIdOf(null)).isEqualTo("unknown");
    assertThat(KafkaErrorHandling.sagaIdOf(42)).isEqualTo("unknown");
  }
}
