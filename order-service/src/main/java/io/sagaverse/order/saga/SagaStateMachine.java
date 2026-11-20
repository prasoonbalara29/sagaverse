package io.sagaverse.order.saga;

import io.sagaverse.events.EventType;
import io.sagaverse.events.InventoryMessages.ReleaseStock;
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
 * The orchestrator's brain, as a pure function: (current saga, input) → {@link Decision}. The input
 * is either a reply or a timeout.
 *
 * <p>It doesn't touch the database or Kafka. {@link SagaOrchestrator} applies the decision. Keeping
 * it pure means every transition can be unit-tested without containers.
 */
@Component
public class SagaStateMachine {

  static final String REFUND_PAYMENT = EventType.REFUND_PAYMENT.wireName();
  static final String RELEASE_STOCK = EventType.RELEASE_STOCK.wireName();
  static final String TIMEOUT = "Timeout";

  private final Clock clock;
  private final SagaProperties props;

  public SagaStateMachine(Clock clock, SagaProperties props) {
    this.clock = clock;
    this.props = props;
  }

  /** What to do when an order is placed: wait for payment, and charge it. */
  public Decision start(Order order) {
    var charge = charge(order);
    return Decision.move(
        null,
        SagaState.PAYMENT_PENDING,
        "OrderPlaced",
        EventType.of(charge).wireName(),
        1,
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
        var reserve = reserve(order);
        yield Decision.move(
            state,
            SagaState.INVENTORY_PENDING,
            trigger,
            EventType.of(reserve).wireName(),
            1,
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
              0,
              null,
              Order.Status.CONFIRMED,
              List.of(),
              List.of());

      // --- F1: payment declined. Nothing was done, so there is nothing to undo. ---------------
      case PaymentDeclined declined when state == SagaState.PAYMENT_PENDING ->
          Decision.move(
              state,
              SagaState.FAILED,
              trigger,
              null,
              0,
              null,
              Order.Status.CANCELLED,
              List.of(),
              List.of());

      // --- F2: out of stock. Payment was taken, so refund it. -----------------------------------
      case StockUnavailable unavailable when state == SagaState.INVENTORY_PENDING ->
          compensate(
              state, trigger, List.of(new RefundPayment(order.orderId(), unavailable.reason())));

      // --- compensation acks -------------------------------------------------------------------
      case PaymentRefunded refunded
          when state == SagaState.COMPENSATING
              && saga.compensationsPending().contains(REFUND_PAYMENT) ->
          acknowledge(saga, REFUND_PAYMENT, trigger);
      case StockReleased released
          when state == SagaState.COMPENSATING
              && saga.compensationsPending().contains(RELEASE_STOCK) ->
          acknowledge(saga, RELEASE_STOCK, trigger);

      // --- F9: stock reserved after the saga gave up. Give it back. ---------------------------
      case StockReserved late when isCancelled(state) ->
          releaseLateReservation(saga, order, trigger);

      default -> Decision.ignore(state, trigger, "no transition for " + trigger + " in " + state);
    };
  }

  /**
   * The current step's deadline has passed (ADR-0009). Resend the step while attempts are left;
   * then give up and compensate everything that may have happened. A compensating saga resends its
   * outstanding compensations, and after {@code max-compensation-attempts} it stops and needs a
   * human (ADR-0008).
   */
  public Decision onTimeout(SagaInstance saga, Order order) {
    SagaState state = saga.state();
    int next = saga.stepAttempts() + 1;
    return switch (state) {
      case PAYMENT_PENDING ->
          next <= props.maxStepAttempts()
              ? resend(saga, List.of(charge(order)), next)
              // A charge may have happened without us hearing back: refund to be safe.
              : compensate(
                  state, TIMEOUT, List.of(new RefundPayment(order.orderId(), "PAYMENT_TIMEOUT")));
      case INVENTORY_PENDING ->
          next <= props.maxStepAttempts()
              ? resend(saga, List.of(reserve(order)), next)
              // Stock may or may not be held, and the payment certainly was taken: undo both.
              : compensate(
                  state,
                  TIMEOUT,
                  List.of(
                      new ReleaseStock(order.orderId(), "INVENTORY_TIMEOUT"),
                      new RefundPayment(order.orderId(), "INVENTORY_TIMEOUT")));
      case COMPENSATING ->
          next <= props.maxCompensationAttempts()
              ? resend(saga, compensations(saga, order), next)
              : Decision.move(
                  state,
                  SagaState.COMPENSATION_FAILED,
                  TIMEOUT,
                  null,
                  saga.stepAttempts(),
                  null,
                  Order.Status.CANCELLED_NEEDS_ATTENTION,
                  List.of(),
                  saga.compensationsPending());
      case COMPLETED, FAILED, COMPENSATED, COMPENSATION_FAILED ->
          Decision.ignore(state, TIMEOUT, "terminal");
    };
  }

  // --- building blocks -----------------------------------------------------------------------

  private static ChargePayment charge(Order order) {
    return new ChargePayment(order.orderId(), order.customerId(), order.amount());
  }

  private static ReserveStock reserve(Order order) {
    return new ReserveStock(order.orderId(), order.sku(), order.quantity());
  }

  private static boolean isCancelled(SagaState state) {
    return state == SagaState.COMPENSATING
        || state == SagaState.COMPENSATED
        || state == SagaState.FAILED
        || state == SagaState.COMPENSATION_FAILED;
  }

  /** Same state, same commands again, one more attempt, fresh deadline. */
  private Decision resend(SagaInstance saga, List<SagaPayload> commands, int attempt) {
    return Decision.move(
        saga.state(),
        saga.state(),
        TIMEOUT,
        saga.currentStep(),
        attempt,
        deadline(),
        null,
        commands,
        saga.compensationsPending());
  }

  private Decision compensate(SagaState from, String trigger, List<SagaPayload> compensations) {
    List<String> pending = compensations.stream().map(c -> EventType.of(c).wireName()).toList();
    return Decision.move(
        from,
        SagaState.COMPENSATING,
        trigger,
        pending.get(0),
        1,
        deadline(),
        null,
        compensations,
        pending);
  }

  /** The compensation commands still awaiting an ack, rebuilt for a resend. */
  private static List<SagaPayload> compensations(SagaInstance saga, Order order) {
    List<SagaPayload> out = new ArrayList<>();
    for (String name : saga.compensationsPending()) {
      if (name.equals(RELEASE_STOCK)) {
        out.add(new ReleaseStock(order.orderId(), "RETRY"));
      } else if (name.equals(REFUND_PAYMENT)) {
        out.add(new RefundPayment(order.orderId(), "RETRY"));
      }
    }
    return out;
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
          0,
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
        saga.stepAttempts(),
        saga.deadlineAt(),
        null,
        List.of(),
        remaining);
  }

  /**
   * F9. A reservation landed after the saga was cancelled, e.g. a resend that was still in flight.
   * If a ReleaseStock is already on its way (it travels behind the reservation on the same
   * partition), nothing to do. Otherwise send one. A still-compensating saga also waits for its
   * ack. A terminal saga stays terminal, and only the release goes out.
   */
  private Decision releaseLateReservation(SagaInstance saga, Order order, String trigger) {
    if (saga.compensationsPending().contains(RELEASE_STOCK)) {
      return Decision.ignore(saga.state(), trigger, "ReleaseStock already in flight");
    }
    var release = new ReleaseStock(order.orderId(), "LATE_RESERVATION");
    boolean compensating = saga.state() == SagaState.COMPENSATING;
    List<String> pending = new ArrayList<>(saga.compensationsPending());
    if (compensating) {
      pending.add(RELEASE_STOCK);
    }
    return Decision.move(
        saga.state(),
        saga.state(),
        trigger + "(late)",
        saga.currentStep(),
        saga.stepAttempts(),
        saga.deadlineAt(),
        null,
        List.of(release),
        pending);
  }

  private Instant deadline() {
    return clock.instant().plus(props.stepTimeout());
  }

  /**
   * The outcome of one step of the state machine.
   *
   * @param from state before; null when the saga is being created
   * @param to state after (equal to {@code from} when ignored, resending, or still compensating)
   * @param trigger what caused it: "OrderPlaced", "Timeout", or the reply's wire name
   * @param nextStep wire name of the command now awaited; null when terminal
   * @param stepAttempts how many times the awaited command has now been sent
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
      int stepAttempts,
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
        int stepAttempts,
        @Nullable Instant deadlineAt,
        @Nullable Order.Status orderStatus,
        List<SagaPayload> commands,
        List<String> compensationsPending) {
      return new Decision(
          from,
          to,
          trigger,
          nextStep,
          stepAttempts,
          deadlineAt,
          Optional.ofNullable(orderStatus),
          List.copyOf(commands),
          List.copyOf(compensationsPending),
          null);
    }

    static Decision ignore(SagaState current, String trigger, String reason) {
      return new Decision(
          current, current, trigger, null, 0, null, Optional.empty(), List.of(), List.of(), reason);
    }

    public boolean ignored() {
      return ignoredReason != null;
    }
  }
}
