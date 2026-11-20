package io.sagaverse.order.saga;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

/**
 * Every {@code saga.sweep-interval}, hands stuck sagas to the orchestrator (ADR-0003, ADR-0009).
 *
 * <p>Reliable mode only. Naive mode has no timeouts, so a saga waiting for a reply that will never
 * come waits forever (F6).
 */
@Component
@EnableScheduling
@ConditionalOnProperty(name = "saga.mode", havingValue = "reliable", matchIfMissing = true)
public class SagaTimeoutSweeper implements SchedulingConfigurer {

  private static final Logger log = LoggerFactory.getLogger(SagaTimeoutSweeper.class);
  private static final int BATCH = 50;

  private final SagaOrchestrator orchestrator;
  private final SagaProperties props;

  public SagaTimeoutSweeper(SagaOrchestrator orchestrator, SagaProperties props) {
    this.orchestrator = orchestrator;
    this.props = props;
  }

  @Override
  public void configureTasks(ScheduledTaskRegistrar registrar) {
    registrar.addFixedDelayTask(this::sweep, props.sweepInterval());
  }

  void sweep() {
    try {
      int handled = orchestrator.handleTimeouts(BATCH);
      if (handled > 0) {
        log.info("sweeper handled {} stuck saga(s)", handled);
      }
    } catch (RuntimeException e) {
      // e.g. an optimistic-lock clash with a reply. The next sweep tries again.
      log.warn("sweep failed, will retry: {}", e.toString());
    }
  }
}
