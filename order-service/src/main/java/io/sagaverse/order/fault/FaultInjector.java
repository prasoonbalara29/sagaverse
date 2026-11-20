package io.sagaverse.order.fault;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.stereotype.Component;

/**
 * Deliberate failure points, for the tests and demos that prove the dual-write problem (F4). They
 * are off by default, and nothing in a normal run turns them on.
 *
 * <p>{@code fault.crash-before-publish=true} makes the service "die" at the most dangerous moment:
 * after the DB transaction committed, before the message reached Kafka. In naive mode that's the
 * after-commit send, and in reliable mode it's the outbox relay. Either way, the process afterwards
 * loses whatever it held in memory.
 */
@Component
@EnableConfigurationProperties(FaultInjector.FaultProperties.class)
public class FaultInjector {

  private static final Logger log = LoggerFactory.getLogger(FaultInjector.class);

  /** {@code fault.*}. */
  @ConfigurationProperties("fault")
  public record FaultProperties(@DefaultValue("false") boolean crashBeforePublish) {}

  private final FaultProperties props;

  public FaultInjector(FaultProperties props) {
    this.props = props;
  }

  /** Call this after the DB commit and before sending to Kafka. */
  public void beforePublish(String what) {
    if (props.crashBeforePublish()) {
      log.error("FAULT INJECTED: simulated crash after commit, before publishing {}", what);
      throw new SimulatedCrashException(what);
    }
  }

  /** Stands for the process dying. Nothing after the throw point runs. */
  public static final class SimulatedCrashException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    SimulatedCrashException(String what) {
      super("simulated crash before publishing " + what);
    }
  }
}
