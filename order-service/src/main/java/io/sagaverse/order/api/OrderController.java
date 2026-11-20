package io.sagaverse.order.api;

import io.sagaverse.order.order.Order;
import io.sagaverse.order.order.OrderRepository;
import io.sagaverse.order.saga.SagaInstance;
import io.sagaverse.order.saga.SagaOrchestrator;
import io.sagaverse.order.saga.SagaOrchestrator.PlacedOrder;
import io.sagaverse.order.saga.SagaRepository;
import io.sagaverse.order.saga.SagaRepository.Transition;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The system's only entry point. Placing an order returns 202 Accepted: the saga runs
 * asynchronously, so poll {@code GET /orders/{id}} to watch it finish.
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

  private final SagaOrchestrator orchestrator;
  private final OrderRepository orders;
  private final SagaRepository sagas;

  public OrderController(
      SagaOrchestrator orchestrator, OrderRepository orders, SagaRepository sagas) {
    this.orchestrator = orchestrator;
    this.orders = orders;
    this.sagas = sagas;
  }

  /** Request body for {@code POST /orders}. */
  public record PlaceOrderRequest(
      @NotBlank String customerId,
      @NotBlank String sku,
      @Positive int quantity,
      @NotNull @Positive @Digits(integer = 10, fraction = 2) BigDecimal amount) {}

  /** The order plus its saga's progress and full history, for watching a saga run. */
  public record OrderView(
      String orderId,
      String customerId,
      String sku,
      int quantity,
      BigDecimal amount,
      Order.Status status,
      @Nullable SagaView saga) {}

  /** Saga part of {@link OrderView}. */
  public record SagaView(
      UUID sagaId,
      String state,
      @Nullable String currentStep,
      int stepAttempts,
      @Nullable Instant deadlineAt,
      List<Transition> transitions) {}

  @PostMapping
  public ResponseEntity<PlacedOrder> place(@Valid @RequestBody PlaceOrderRequest req) {
    PlacedOrder placed =
        orchestrator.placeOrder(req.customerId(), req.sku(), req.quantity(), req.amount());
    return ResponseEntity.accepted()
        .location(URI.create("/orders/" + placed.orderId()))
        .body(placed);
  }

  @GetMapping("/{orderId}")
  public ResponseEntity<OrderView> get(@PathVariable String orderId) {
    return orders
        .findById(orderId)
        .map(o -> ResponseEntity.ok(view(o)))
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  private OrderView view(Order o) {
    SagaView saga = sagas.findLatestByOrderId(o.orderId()).map(this::sagaView).orElse(null);
    return new OrderView(
        o.orderId(), o.customerId(), o.sku(), o.quantity(), o.amount(), o.status(), saga);
  }

  private SagaView sagaView(SagaInstance s) {
    return new SagaView(
        s.sagaId(),
        s.state().name(),
        s.currentStep(),
        s.stepAttempts(),
        s.deadlineAt(),
        sagas.transitions(s.sagaId()));
  }
}
