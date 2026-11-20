package io.sagaverse.order.saga;

import io.sagaverse.events.EventType;
import io.sagaverse.events.InventoryMessages.ReserveStock;
import io.sagaverse.events.InventoryMessages.StockReserved;
import io.sagaverse.events.PaymentMessages.ChargePayment;
import io.sagaverse.events.PaymentMessages.PaymentCharged;
import io.sagaverse.events.SagaPayload;
import io.sagaverse.order.order.Order;
import java.time.Clock;
import java.time.Instant;
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
 * <p>Phase 2 implements the happy path only. Declines, compensations (Phase 3) and timeouts (Phase
 * 5) are added later. Until then, those replies fall through to "ignored".
 */
@Component
public class SagaStateMachine {

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
        List.of(charge));
  }

  /** What to do when a reply arrives for this saga. */
  public Decision onReply(SagaInstance saga, Order order, SagaPayload reply) {
    String trigger = EventType.of(reply).wireName();
    return switch (reply) {
      case PaymentCharged charged when saga.state() == SagaState.PAYMENT_PENDING -> {
        var reserve = new ReserveStock(order.orderId(), order.sku(), order.quantity());
        yield Decision.move(
            saga.state(),
            SagaState.INVENTORY_PENDING,
            trigger,
            EventType.of(reserve).wireName(),
            deadline(),
            null,
            List.of(reserve));
      }
      case StockReserved reserved when saga.state() == SagaState.INVENTORY_PENDING ->
          Decision.move(
              saga.state(),
              SagaState.COMPLETED,
              trigger,
              null,
              null,
              Order.Status.CONFIRMED,
              List.of());
      default ->
          Decision.ignore(
              saga.state(), trigger, "no transition for " + trigger + " in " + saga.state());
    };
  }

  private Instant deadline() {
    return clock.instant().plus(props.stepTimeout());
  }

  /**
   * The outcome of one step of the state machine.
   *
   * @param from state before; null when the saga is being created
   * @param to state after (equal to {@code from} when ignored)
   * @param trigger what caused it: "OrderPlaced" or the reply's wire name
   * @param nextStep wire name of the command now awaited; null when terminal
   * @param deadlineAt when that step counts as stuck; null when terminal
   * @param orderStatus new order status, if it changes
   * @param commands commands to send, in order
   * @param ignoredReason why the input was ignored; null when the saga moved
   */
  public record Decision(
      @Nullable SagaState from,
      SagaState to,
      String trigger,
      @Nullable String nextStep,
      @Nullable Instant deadlineAt,
      Optional<Order.Status> orderStatus,
      List<SagaPayload> commands,
      @Nullable String ignoredReason) {

    static Decision move(
        @Nullable SagaState from,
        SagaState to,
        String trigger,
        @Nullable String nextStep,
        @Nullable Instant deadlineAt,
        @Nullable Order.Status orderStatus,
        List<SagaPayload> commands) {
      return new Decision(
          from,
          to,
          trigger,
          nextStep,
          deadlineAt,
          Optional.ofNullable(orderStatus),
          List.copyOf(commands),
          null);
    }

    static Decision ignore(SagaState current, String trigger, String reason) {
      return new Decision(
          current, current, trigger, null, null, Optional.empty(), List.of(), reason);
    }

    public boolean ignored() {
      return ignoredReason != null;
    }
  }
}
