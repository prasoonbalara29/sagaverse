package io.sagaverse.e2e;

import static io.sagaverse.e2e.SagaverseCluster.Service.ORDER;
import static io.sagaverse.e2e.SagaverseCluster.Service.PAYMENT;
import static org.assertj.core.api.Assertions.assertThat;

import io.sagaverse.e2e.InvariantChecker.Kind;
import io.sagaverse.e2e.InvariantChecker.Violation;
import java.time.Duration;
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
            .with(ORDER, "saga.max-step-attempts", 3)
            .with(ORDER, "saga.max-compensation-attempts", 5)
            .with(ORDER, "saga.sweep-interval", "500ms")
            .with(PAYMENT, "payment.decline-amount-over", "100.00")
            .with(PAYMENT, "messaging.retry.initial-interval", "100ms")
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
  void f5_inventoryBrieflyDown_resendsThenCompletes_reservingOnce() {
    var outage = scenarios.inventoryBriefOutage();

    assertThat(outage.attemptsWhileDown()).as("sweeper resent ReserveStock").isGreaterThan(1);
    assertThat(scenarios.state(outage.sagaId())).isEqualTo("COMPLETED");
    assertThat(invariants.check(outage.sagaId())).isEmpty();
  }

  @Test
  void f6_inventoryDownPastDeadline_givesUpAndCompensates() {
    var outage = scenarios.inventoryLongOutage();

    assertThat(outage.violationsWhileDown()).isEmpty();
    assertThat(outage.stateWhileDown()).isEqualTo("COMPENSATING");
    assertThat(scenarios.state(outage.sagaId())).isEqualTo("COMPENSATED");
    assertThat(invariants.check(outage.sagaId())).isEmpty();
  }

  @Test
  void f8_refundFailsForAWhile_isRetriedUntilItSucceeds() {
    UUID sagaId = scenarios.refundFailsTransiently(5, Duration.ofSeconds(20));

    assertThat(scenarios.state(sagaId)).isEqualTo("COMPENSATED");
    assertThat(invariants.check(sagaId)).isEmpty();
  }

  @Test
  void f8_refundNeverSucceeds_stopsAtCompensationFailed_andFlagsTheMoney() {
    UUID sagaId = scenarios.refundFailsTransiently(1_000, Duration.ofSeconds(40));

    assertThat(scenarios.state(sagaId)).isEqualTo("COMPENSATION_FAILED");
    assertThat(invariants.check(sagaId))
        .as("a human must refund this one")
        .extracting(Violation::kind)
        .containsExactly(Kind.UNREFUNDED_CHARGE);
  }

  @Test
  void f9_lateReservationAfterCancellation_isReleased() {
    try {
      UUID sagaId = scenarios.lateReservationAfterCancellation();

      assertThat(invariants.check(sagaId)).isEmpty();
    } finally {
      scenarios.emptySkuEmpty();
    }
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
