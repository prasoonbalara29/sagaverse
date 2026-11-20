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
 * Naive mode (dual write, no dedup) keeps its bugs on purpose (ADR-0010). These tests run the same
 * scenarios as {@link ReliableModeE2EIT} and assert that the <b>specific</b> violation happens, so
 * the demonstration itself stays under test.
 */
@Testcontainers
class NaiveModeE2EIT {

  static final String MODE = "naive";

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

  /** F1 and F2 don't depend on delivery guarantees, so naive mode gets them right too. */
  @Test
  void f1_paymentDeclined_isCorrectEvenInNaiveMode() {
    UUID sagaId = scenarios.paymentDeclined();

    assertThat(invariants.check(sagaId)).isEmpty();
    assertThat(scenarios.state(sagaId)).isEqualTo("FAILED");
  }

  @Test
  void f2_outOfStock_isCorrectEvenInNaiveMode() {
    UUID sagaId = scenarios.outOfStock();

    assertThat(invariants.check(sagaId)).isEmpty();
    assertThat(scenarios.state(sagaId)).isEqualTo("COMPENSATED");
  }

  @Test
  void f3a_redeliveredCommands_chargeAndReserveTwice() {
    UUID sagaId = scenarios.duplicateRedelivery();

    assertThat(invariants.check(sagaId))
        .extracting(Violation::kind)
        .containsExactlyInAnyOrder(Kind.DOUBLE_CHARGE, Kind.WRONG_CHARGE, Kind.DOUBLE_RESERVATION);
  }

  @Test
  void f3b_resentCommandsWithNewEventIds_chargeAndReserveTwice() {
    UUID sagaId = scenarios.logicalResend();

    assertThat(invariants.check(sagaId))
        .extracting(Violation::kind)
        .containsExactlyInAnyOrder(Kind.DOUBLE_CHARGE, Kind.WRONG_CHARGE, Kind.DOUBLE_RESERVATION);
  }

  /**
   * F5 is correct in naive mode too. Kafka kept the unread ReserveStock, so once inventory is back
   * the saga finishes. Naive mode just never resends.
   */
  @Test
  void f5_inventoryBrieflyDown_completesOnceItsBack_withoutResending() {
    var outage = scenarios.inventoryBriefOutage();

    assertThat(outage.attemptsWhileDown()).isEqualTo(1);
    assertThat(scenarios.state(outage.sagaId())).isEqualTo("COMPLETED");
    assertThat(invariants.check(outage.sagaId())).isEmpty();
  }

  /**
   * F6: while inventory is down, the order hangs in PENDING with the customer charged, for as long
   * as the outage lasts. Nothing gives up and nothing refunds.
   */
  @Test
  void f6_inventoryDownPastDeadline_hangsForAsLongAsTheOutage() {
    var outage = scenarios.inventoryLongOutage();

    assertThat(outage.stateWhileDown()).isEqualTo("INVENTORY_PENDING");
    assertThat(outage.attemptsWhileDown()).isEqualTo(1);
    assertThat(outage.violationsWhileDown())
        .extracting(Violation::kind)
        .containsExactly(Kind.STUCK_SAGA);
  }

  /**
   * F8: the refund outlasts Kafka's retries and lands in the DLT. Nothing resends it, so the saga
   * sits in COMPENSATING with the customer's money taken.
   */
  @Test
  void f8_refundFailsForAWhile_isNeverRetried_moneyStaysTaken() {
    UUID sagaId = scenarios.refundFailsTransiently(5, Duration.ofSeconds(10));

    assertThat(scenarios.state(sagaId)).isEqualTo("COMPENSATING");
    assertThat(invariants.check(sagaId))
        .extracting(Violation::kind)
        .containsExactly(Kind.STUCK_SAGA);
  }

  /**
   * F9 is handled in naive mode too: the late-reservation rule lives in the state machine, which
   * both modes share.
   */
  @Test
  void f9_lateReservationAfterCancellation_isReleasedEvenInNaiveMode() {
    try {
      UUID sagaId = scenarios.lateReservationAfterCancellation();

      assertThat(invariants.check(sagaId)).isEmpty();
    } finally {
      scenarios.emptySkuEmpty();
    }
  }

  @Test
  void f4_crashBetweenCommitAndPublish_losesTheCommand_sagaHangsForever() {
    var crash = scenarios.lostEventAfterCommit();
    UUID sagaId = crash.sagaId();

    assertThat(crash.unsentAtCrash())
        .as("naive mode keeps no durable copy of the command")
        .isZero();
    assertThat(invariants.check(sagaId))
        .extracting(Violation::kind)
        .containsExactly(Kind.STUCK_SAGA);
    assertThat(scenarios.state(sagaId)).isEqualTo("PAYMENT_PENDING");
    assertThat(
            cluster
                .db(PAYMENT)
                .queryForObject(
                    "SELECT count(*) FROM payment WHERE saga_id = ?", Integer.class, sagaId))
        .as("ChargePayment never reached payment-service")
        .isZero();
  }
}
