package io.sagaverse.order.saga;

import io.sagaverse.events.EventType;
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
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * The orchestrator's brain, as a pure function: (current saga, incoming reply) → {@link Decision}.
 *
 * <p>It doesn't touch the database or Kafka. {@link SagaOrchestrator} applies the decision. Keeping
 * it pure means every transition can be unit-tested without containers.
 *
 * <p>Implemented so far: the happy path, payment declined (→ FAILED), and stock unavailable (→
 * COMPENSATING → COMPENSATED). Timeouts and late replies are added in Phase 5.
 */
@Component
public class SagaStateMachine {

  static final String REFUND_PAYMENT = EventType.REFUND_PAYMENT.wireName();
  static final String RELEASE_STOCK = EventType.RELEASE_STOCK.wireName();

  private final Clock clock;
  private final SagaProperties props;

  public SagaStateMachine(Clock clock, SagaProperties props) {
    this.clock = clock;
    this.props = props;
  }

  /** What to do when an order is placed: wait for payment, and charge it. */
  public Decision start(Order order) {
    var charge = new ChargePayment(order.orderId(), order.customerId(), order.amount());
    return Decision.move(
        null,
        SagaState.PAYMENT_PENDING,
        "OrderPlaced",
        EventType.of(charge).wireName(),
        deadline(),
        null,
        List.of(charge),
        List.of());
  }

  /** What to do when a reply arrives for this saga. */
  public Decision onReply(SagaInstance saga, Order order, SagaPayload reply) {
    String trigger = EventType.of(reply).wireName();
    SagaState state = saga.state();
    return switch (reply) {
      // --- happy path ------------------------------------------------------------------------
      case PaymentCharged charged when state == SagaState.PAYMENT_PENDING -> {
        var reserve = new ReserveStock(order.orderId(), order.sku(), order.quantity());
        yield Decision.move(
            state,
            SagaState.INVENTORY_PENDING,
            trigger,
            EventType.of(reserve).wireName(),
            deadline(),
            null,
            List.of(reserve),
            List.of());
      }
      case StockReserved reserved when state == SagaState.INVENTORY_PENDING ->
          Decision.move(
              state,
              SagaState.COMPLETED,
              trigger,
              null,
              null,
              Order.Status.CONFIRMED,
              List.of(),
              List.of());

      // --- F1: payment declined. Nothing was done, so there is nothing to undo.
      // -------------------
      case PaymentDeclined declined when state == SagaState.PAYMENT_PENDING ->
          Decision.move(
              state,
              SagaState.FAILED,
              trigger,
              null,
              null,
              Order.Status.CANCELLED,
              List.of(),
              List.of());

      // --- F2: out of stock. Payment was taken, so refund it. ---------------------------------
      case StockUnavailable unavailable when state == SagaState.INVENTORY_PENDING -> {
        var refund = new RefundPayment(order.orderId(), unavailable.reason());
        yield Decision.move(
            state,
            SagaState.COMPENSATING,
            trigger,
            REFUND_PAYMENT,
            deadline(),
            null,
            List.of(refund),
            List.of(REFUND_PAYMENT));
      }

      // --- compensation acks -------------------------------------------------------------------
      case PaymentRefunded refunded
          when state == SagaState.COMPENSATING
              && saga.compensationsPending().contains(REFUND_PAYMENT) ->
          acknowledge(saga, REFUND_PAYMENT, trigger);
      case StockReleased released
          when state == SagaState.COMPENSATING
              && saga.compensationsPending().contains(RELEASE_STOCK) ->
          acknowledge(saga, RELEASE_STOCK, trigger);

      default -> Decision.ignore(state, trigger, "no transition for " + trigger + " in " + state);
    };
  }

  /**
   * One compensation is confirmed. When none are left, the saga is COMPENSATED and the order
   * CANCELLED. Otherwise keep waiting for the rest.
   */
  private Decision acknowledge(SagaInstance saga, String compensation, String trigger) {
    List<String> remaining = new ArrayList<>(saga.compensationsPending());
    remaining.remove(compensation);
    if (remaining.isEmpty()) {
      return Decision.move(
          saga.state(),
          SagaState.COMPENSATED,
          trigger,
          null,
          null,
          Order.Status.CANCELLED,
          List.of(),
          List.of());
    }
    return Decision.move(
        saga.state(),
        SagaState.COMPENSATING,
        trigger,
        remaining.get(0),
        saga.deadlineAt(),
        null,
        List.of(),
        remaining);
  }

  private Instant deadline() {
    return clock.instant().plus(props.stepTimeout());
  }

  /**
   * The outcome of one step of the state machine.
   *
   * @param from state before; null when the saga is being created
   * @param to state after (equal to {@code from} when ignored, or when still compensating)
   * @param trigger what caused it: "OrderPlaced" or the reply's wire name
   * @param nextStep wire name of the command now awaited; null when terminal
   * @param deadlineAt when that step counts as stuck; null when terminal
   * @param orderStatus new order status, if it changes
   * @param commands commands to send, in order
   * @param compensationsPending compensations sent and not yet acknowledged, after this step
   * @param ignoredReason why the input was ignored; null when the decision should be applied
   */
  public record Decision(
      @Nullable SagaState from,
      SagaState to,
      String trigger,
      @Nullable String nextStep,
      @Nullable Instant deadlineAt,
      Optional<Order.Status> orderStatus,
      List<SagaPayload> commands,
      List<String> compensationsPending,
      @Nullable String ignoredReason) {

    static Decision move(
        @Nullable SagaState from,
        SagaState to,
        String trigger,
        @Nullable String nextStep,
        @Nullable Instant deadlineAt,
        @Nullable Order.Status orderStatus,
        List<SagaPayload> commands,
        List<String> compensationsPending) {
      return new Decision(
          from,
          to,
          trigger,
          nextStep,
          deadlineAt,
          Optional.ofNullable(orderStatus),
          List.copyOf(commands),
          List.copyOf(compensationsPending),
          null);
    }

    static Decision ignore(SagaState current, String trigger, String reason) {
      return new Decision(
          current, current, trigger, null, null, Optional.empty(), List.of(), List.of(), reason);
    }

    public boolean ignored() {
      return ignoredReason != null;
    }
  }
}
