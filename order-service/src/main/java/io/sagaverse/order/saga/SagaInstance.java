package io.sagaverse.order.saga;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.lang.Nullable;

/**
 * One row of {@code saga_instance}: where a saga currently is (ADR-0003).
 *
 * @param currentStep wire name of the command we are waiting on, e.g. "ChargePayment"; null once
 *     terminal
 * @param stepAttempts how many times the current step's command has been sent
 * @param compensationsPending compensations sent but not yet acknowledged, by wire name
 * @param deadlineAt when the current step counts as stuck; null once terminal
 * @param version optimistic-lock counter, incremented on every update
 */
public record SagaInstance(
    UUID sagaId,
    String orderId,
    SagaState state,
    @Nullable String currentStep,
    int stepAttempts,
    List<String> compensationsPending,
    @Nullable Instant deadlineAt,
    long version,
    Instant createdAt,
    Instant updatedAt) {

  public SagaInstance {
    compensationsPending = List.copyOf(compensationsPending);
  }
}
