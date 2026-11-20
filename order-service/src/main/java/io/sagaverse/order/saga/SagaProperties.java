package io.sagaverse.order.saga;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Saga tuning ({@code saga.*} in application.yml). See ADR-0009 and ADR-0010.
 *
 * @param mode naive (dual write, no dedup) or reliable (outbox, inbox, sweeper)
 * @param stepTimeout how long to wait for a step's reply before the saga counts as stuck
 * @param maxStepAttempts how many times a step command is sent before giving up and compensating
 * @param maxCompensationAttempts how many times a compensation is sent before COMPENSATION_FAILED
 * @param sweepInterval how often the timeout sweeper looks for stuck sagas
 */
@ConfigurationProperties("saga")
public record SagaProperties(
    @DefaultValue("naive") Mode mode,
    @DefaultValue("10s") Duration stepTimeout,
    @DefaultValue("6") int maxStepAttempts,
    @DefaultValue("10") int maxCompensationAttempts,
    @DefaultValue("2s") Duration sweepInterval) {

  /** ADR-0010. */
  public enum Mode {
    NAIVE,
    RELIABLE
  }
}
