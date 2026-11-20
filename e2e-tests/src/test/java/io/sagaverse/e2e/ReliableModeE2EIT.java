package io.sagaverse.e2e;

import static io.sagaverse.e2e.SagaverseCluster.Service.ORDER;
import static io.sagaverse.e2e.SagaverseCluster.Service.PAYMENT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Reliable mode: outbox, inbox and idempotent consumers (ADR-0004, ADR-0005, ADR-0010). Every
 * scenario must leave <b>zero</b> invariant violations. {@link NaiveModeE2EIT} runs the same
 * scenarios and shows what breaks without these fixes.
 */
@Testcontainers
class ReliableModeE2EIT {

  static final String MODE = "reliable";

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.2");

  static SagaverseCluster cluster;
  static ScenarioRunner scenarios;
  static InvariantChecker invariants;

  @BeforeAll
  static void start() {
    cluster =
        new SagaverseCluster(POSTGRES, KAFKA, MODE)
            .with(ORDER, "saga.step-timeout", "2s")
            .with(PAYMENT, "payment.decline-amount-over", "100.00")
            .startAll();
    scenarios = new ScenarioRunner(cluster);
    invariants = new InvariantChecker(cluster);
  }

  @AfterAll
  static void stop() {
    cluster.close();
  }

  @Test
  void f1_paymentDeclined_failsCleanly() {
    UUID sagaId = scenarios.paymentDeclined();

    assertThat(invariants.check(sagaId)).isEmpty();
    assertThat(scenarios.state(sagaId)).isEqualTo("FAILED");
  }

  @Test
  void f2_outOfStock_refundsAndCompensates() {
    UUID sagaId = scenarios.outOfStock();

    assertThat(invariants.check(sagaId)).isEmpty();
    assertThat(scenarios.state(sagaId)).isEqualTo("COMPENSATED");
  }

  @Test
  void f3a_redeliveredCommands_haveNoEffect() {
    UUID sagaId = scenarios.duplicateRedelivery();

    assertThat(invariants.check(sagaId)).isEmpty();
  }

  @Test
  void f3b_resentCommandsWithNewEventIds_haveNoEffect() {
    UUID sagaId = scenarios.logicalResend();

    assertThat(invariants.check(sagaId)).isEmpty();
  }

  @Test
  void f4_crashBetweenCommitAndPublish_sagaStillCompletes() {
    var crash = scenarios.lostEventAfterCommit();
    UUID sagaId = crash.sagaId();

    assertThat(crash.unsentAtCrash())
        .as("ChargePayment survived the crash in the outbox")
        .isEqualTo(1);
    assertThat(invariants.check(sagaId)).isEmpty();
    assertThat(scenarios.state(sagaId)).isEqualTo("COMPLETED");
  }
}
