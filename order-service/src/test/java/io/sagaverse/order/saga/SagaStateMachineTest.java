package io.sagaverse.order.saga;

import static org.assertj.core.api.Assertions.assertThat;

import io.sagaverse.events.InventoryMessages.ReserveStock;
import io.sagaverse.events.InventoryMessages.StockReleased;
import io.sagaverse.events.InventoryMessages.StockReserved;
import io.sagaverse.events.InventoryMessages.StockUnavailable;
import io.sagaverse.events.PaymentMessages.ChargePayment;
import io.sagaverse.events.PaymentMessages.PaymentCharged;
import io.sagaverse.events.PaymentMessages.PaymentDeclined;
import io.sagaverse.events.PaymentMessages.PaymentRefunded;
import io.sagaverse.events.PaymentMessages.RefundPayment;
import io.sagaverse.events.SagaPayload;
import io.sagaverse.order.order.Order;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** One test per transition, plus the cases that must be ignored. */
class SagaStateMachineTest {

  private static final Instant NOW = Instant.parse("2026-11-20T10:00:00Z");
  private static final Duration STEP_TIMEOUT = Duration.ofSeconds(10);

  private final SagaStateMachine machine =
      new SagaStateMachine(
          Clock.fixed(NOW, ZoneOffset.UTC),
          new SagaProperties(
              SagaProperties.Mode.NAIVE, STEP_TIMEOUT, 6, 10, Duration.ofSeconds(2)));

  private final Order order =
      new Order(
          "order-1", "cust-1", "SKU-1", 2, new BigDecimal("42.50"), Order.Status.PENDING, NOW, NOW);

  private static SagaInstance sagaIn(SagaState state, String... pending) {
    return new SagaInstance(
        UUID.randomUUID(), "order-1", state, "Whatever", 1, List.of(pending), NOW, 3, NOW, NOW);
  }

  @Test
  void start_waitsForPaymentAndSendsChargePayment() {
    var d = machine.start(order);

    assertThat(d.from()).isNull();
    assertThat(d.to()).isEqualTo(SagaState.PAYMENT_PENDING);
    assertThat(d.trigger()).isEqualTo("OrderPlaced");
    assertThat(d.nextStep()).isEqualTo("ChargePayment");
    assertThat(d.deadlineAt()).isEqualTo(NOW.plus(STEP_TIMEOUT));
    assertThat(d.orderStatus()).isEmpty();
    assertThat(d.commands())
        .containsExactly(new ChargePayment("order-1", "cust-1", new BigDecimal("42.50")));
    assertThat(d.ignored()).isFalse();
  }

  @Test
  void paymentPending_paymentCharged_movesToInventoryAndReservesStock() {
    var d =
        machine.onReply(
            sagaIn(SagaState.PAYMENT_PENDING),
            order,
            new PaymentCharged("order-1", UUID.randomUUID(), new BigDecimal("42.50")));

    assertThat(d.from()).isEqualTo(SagaState.PAYMENT_PENDING);
    assertThat(d.to()).isEqualTo(SagaState.INVENTORY_PENDING);
    assertThat(d.trigger()).isEqualTo("PaymentCharged");
    assertThat(d.nextStep()).isEqualTo("ReserveStock");
    assertThat(d.deadlineAt()).isEqualTo(NOW.plus(STEP_TIMEOUT));
    assertThat(d.orderStatus()).isEmpty();
    assertThat(d.commands()).containsExactly(new ReserveStock("order-1", "SKU-1", 2));
  }

  @Test
  void inventoryPending_stockReserved_completesAndConfirmsOrder() {
    var d =
        machine.onReply(
            sagaIn(SagaState.INVENTORY_PENDING), order, new StockReserved("order-1", "SKU-1", 2));

    assertThat(d.from()).isEqualTo(SagaState.INVENTORY_PENDING);
    assertThat(d.to()).isEqualTo(SagaState.COMPLETED);
    assertThat(d.to().isTerminal()).isTrue();
    assertThat(d.nextStep()).isNull();
    assertThat(d.deadlineAt()).isNull();
    assertThat(d.orderStatus()).contains(Order.Status.CONFIRMED);
    assertThat(d.commands()).isEmpty();
  }

  @Test
  void replyForAnotherState_isIgnored_outOfOrderStockReserved() {
    var d =
        machine.onReply(
            sagaIn(SagaState.PAYMENT_PENDING), order, new StockReserved("order-1", "SKU-1", 2));

    assertThat(d.ignored()).isTrue();
    assertThat(d.to()).isEqualTo(SagaState.PAYMENT_PENDING);
    assertThat(d.commands()).isEmpty();
    assertThat(d.ignoredReason()).contains("StockReserved").contains("PAYMENT_PENDING");
  }

  /** A duplicate PaymentCharged after the saga moved on must not reserve stock twice. */
  @ParameterizedTest
  @EnumSource(
      value = SagaState.class,
      names = {"INVENTORY_PENDING", "COMPLETED"})
  void duplicatePaymentCharged_afterMovingOn_isIgnored(SagaState state) {
    var d =
        machine.onReply(
            sagaIn(state),
            order,
            new PaymentCharged("order-1", UUID.randomUUID(), new BigDecimal("42.50")));

    assertThat(d.ignored()).isTrue();
    assertThat(d.commands()).isEmpty();
  }

  // --- F1: payment declined ----------------------------------------------------------------

  @Test
  void paymentPending_paymentDeclined_failsAndCancelsOrder_withNothingToUndo() {
    var d =
        machine.onReply(
            sagaIn(SagaState.PAYMENT_PENDING),
            order,
            new PaymentDeclined("order-1", "AMOUNT_LIMIT_EXCEEDED"));

    assertThat(d.to()).isEqualTo(SagaState.FAILED);
    assertThat(d.to().isTerminal()).isTrue();
    assertThat(d.orderStatus()).contains(Order.Status.CANCELLED);
    assertThat(d.commands()).isEmpty();
    assertThat(d.compensationsPending()).isEmpty();
    assertThat(d.deadlineAt()).isNull();
  }

  // --- F2: out of stock -> refund ----------------------------------------------------------

  @Test
  void inventoryPending_stockUnavailable_startsCompensatingByRefunding() {
    var d =
        machine.onReply(
            sagaIn(SagaState.INVENTORY_PENDING),
            order,
            new StockUnavailable("order-1", "SKU-1", StockUnavailable.OUT_OF_STOCK));

    assertThat(d.to()).isEqualTo(SagaState.COMPENSATING);
    assertThat(d.commands()).containsExactly(new RefundPayment("order-1", "OUT_OF_STOCK"));
    assertThat(d.compensationsPending()).containsExactly("RefundPayment");
    assertThat(d.nextStep()).isEqualTo("RefundPayment");
    assertThat(d.deadlineAt()).isEqualTo(NOW.plus(STEP_TIMEOUT));
    assertThat(d.orderStatus()).as("order stays PENDING until compensation is done").isEmpty();
  }

  @Test
  void compensating_lastCompensationAcked_isCompensatedAndOrderCancelled() {
    var d =
        machine.onReply(
            sagaIn(SagaState.COMPENSATING, "RefundPayment"),
            order,
            new PaymentRefunded("order-1", UUID.randomUUID()));

    assertThat(d.to()).isEqualTo(SagaState.COMPENSATED);
    assertThat(d.orderStatus()).contains(Order.Status.CANCELLED);
    assertThat(d.compensationsPending()).isEmpty();
    assertThat(d.commands()).isEmpty();
    assertThat(d.deadlineAt()).isNull();
  }

  @Test
  void compensating_oneOfTwoAcked_keepsWaitingForTheOther() {
    var d =
        machine.onReply(
            sagaIn(SagaState.COMPENSATING, "ReleaseStock", "RefundPayment"),
            order,
            new StockReleased("order-1", true));

    assertThat(d.ignored()).isFalse();
    assertThat(d.to()).isEqualTo(SagaState.COMPENSATING);
    assertThat(d.compensationsPending()).containsExactly("RefundPayment");
    assertThat(d.nextStep()).isEqualTo("RefundPayment");
    assertThat(d.deadlineAt()).as("deadline unchanged").isEqualTo(NOW);
    assertThat(d.orderStatus()).isEmpty();
  }

  @Test
  void compensating_duplicateAckForAlreadyAckedCompensation_isIgnored() {
    var d =
        machine.onReply(
            sagaIn(SagaState.COMPENSATING, "ReleaseStock"),
            order,
            new PaymentRefunded("order-1", UUID.randomUUID()));

    assertThat(d.ignored()).isTrue();
  }

  @Test
  void declineOrUnavailableInTheWrongState_isIgnored() {
    SagaPayload declined = new PaymentDeclined("order-1", "X");
    SagaPayload unavailable =
        new StockUnavailable("order-1", "SKU-1", StockUnavailable.OUT_OF_STOCK);

    assertThat(machine.onReply(sagaIn(SagaState.INVENTORY_PENDING), order, declined).ignored())
        .isTrue();
    assertThat(machine.onReply(sagaIn(SagaState.PAYMENT_PENDING), order, unavailable).ignored())
        .isTrue();
    assertThat(machine.onReply(sagaIn(SagaState.COMPENSATED), order, unavailable).ignored())
        .isTrue();
  }

  @Test
  void onlyCompletedFailedCompensatedAndCompensationFailedAreTerminal() {
    assertThat(SagaState.values())
        .filteredOn(SagaState::isTerminal)
        .containsExactlyInAnyOrder(
            SagaState.COMPLETED,
            SagaState.FAILED,
            SagaState.COMPENSATED,
            SagaState.COMPENSATION_FAILED);
  }
}
