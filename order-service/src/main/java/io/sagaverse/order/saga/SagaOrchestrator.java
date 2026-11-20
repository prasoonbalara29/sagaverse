package io.sagaverse.order.saga;

import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.SagaPayload;
import io.sagaverse.order.messaging.IdempotencyGuard;
import io.sagaverse.order.messaging.MessagePublisher;
import io.sagaverse.order.order.Order;
import io.sagaverse.order.order.OrderRepository;
import io.sagaverse.order.saga.SagaStateMachine.Decision;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Applies {@link SagaStateMachine} decisions. One local transaction updates the saga row, the order
 * row and the history, and hands the outgoing commands to the {@link MessagePublisher}.
 */
@Service
public class SagaOrchestrator {

  private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

  private final TransactionTemplate tx;
  private final OrderRepository orders;
  private final SagaRepository sagas;
  private final SagaStateMachine machine;
  private final MessagePublisher publisher;
  private final IdempotencyGuard guard;
  private final Clock clock;

  public SagaOrchestrator(
      TransactionTemplate tx,
      OrderRepository orders,
      SagaRepository sagas,
      SagaStateMachine machine,
      MessagePublisher publisher,
      IdempotencyGuard guard,
      Clock clock) {
    this.tx = tx;
    this.orders = orders;
    this.sagas = sagas;
    this.machine = machine;
    this.publisher = publisher;
    this.guard = guard;
    this.clock = clock;
  }

  /** What the REST layer gets back. */
  public record PlacedOrder(String orderId, UUID sagaId, Order.Status status) {}

  /** Creates the order (PENDING) and starts its saga by charging the payment. */
  public PlacedOrder placeOrder(String customerId, String sku, int quantity, BigDecimal amount) {
    String orderId = UUID.randomUUID().toString();
    UUID sagaId = UUID.randomUUID();
    try (var a = MDC.putCloseable("sagaId", sagaId.toString());
        var b = MDC.putCloseable("orderId", orderId)) {
      Instant now = clock.instant();
      Order order =
          new Order(orderId, customerId, sku, quantity, amount, Order.Status.PENDING, now, now);
      Decision d = machine.start(order);

      tx.executeWithoutResult(status -> {
        orders.insert(order);
        sagas.insert(
            new SagaInstance(
                sagaId, orderId, d.to(), d.nextStep(), 1, List.of(), d.deadlineAt(), 0, now, now));
        sagas.recordTransition(sagaId, null, d.to(), d.trigger(), now);
        publisher.publish(sagaId, d.commands());
      });
      log.info("order placed, saga started in {}", d.to());
      return new PlacedOrder(orderId, sagaId, order.status());
    }
  }

  /** Advances the saga in response to a reply from payment or inventory. */
  public void onReply(EventEnvelope<? extends SagaPayload> reply) {
    Decision d =
        tx.execute(status -> {
          if (!guard.firstDelivery(reply)) {
            log.info(
                "duplicate delivery of {} (eventId={}) skipped", reply.type(), reply.eventId());
            return null;
          }
          var saga = sagas.findById(reply.sagaId()).orElse(null);
          if (saga == null) {
            log.warn("ignoring {}: unknown saga", reply.type());
            return null;
          }
          Order order = orders.findById(saga.orderId()).orElseThrow();
          Decision decision = machine.onReply(saga, order, reply.payload());
          if (decision.ignored()) {
            return decision;
          }
          Instant now = clock.instant();
          int attempts = decision.to() == saga.state() ? saga.stepAttempts() : 1;
          sagas.update(
              new SagaInstance(
                  saga.sagaId(),
                  saga.orderId(),
                  decision.to(),
                  decision.nextStep(),
                  attempts,
                  decision.compensationsPending(),
                  decision.deadlineAt(),
                  saga.version(),
                  saga.createdAt(),
                  now),
              saga.version());
          decision.orderStatus().ifPresent(s -> orders.updateStatus(order.orderId(), s, now));
          sagas.recordTransition(
              saga.sagaId(), decision.from(), decision.to(), decision.trigger(), now);
          publisher.publish(saga.sagaId(), decision.commands());
          return decision;
        });
    if (d == null) {
      return;
    }
    if (d.ignored()) {
      log.warn("ignored {}: {}", reply.type(), d.ignoredReason());
    } else {
      log.info(
          "saga {} -> {} on {}{}",
          d.from(),
          d.to(),
          d.trigger(),
          d.compensationsPending().isEmpty() ? "" : " (awaiting " + d.compensationsPending() + ")");
    }
  }
}
