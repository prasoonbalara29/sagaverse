package io.sagaverse.payment.messaging;

import io.sagaverse.events.EventEnvelope;
import java.sql.Timestamp;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Consumer-side duplicate protection (ADR-0005), switched by {@code saga.mode} (ADR-0010). Call
 * both methods <b>inside</b> the handler's transaction.
 */
public interface IdempotencyGuard {

  /**
   * Layer 1, transport dedup. Returns true the first time this eventId is seen, and false for a
   * redelivery of the same message.
   */
  boolean firstDelivery(EventEnvelope<?> message);

  /**
   * Layer 2, business dedup. When true, handlers look up what they already did for this sagaId and
   * reply with that, instead of acting again. That catches resends with a new eventId.
   */
  boolean checkBusinessKeys();

  /** Naive mode: trusts every delivery. */
  @Component
  @ConditionalOnProperty(name = "saga.mode", havingValue = "naive")
  final class TrustEveryDelivery implements IdempotencyGuard {
    @Override
    public boolean firstDelivery(EventEnvelope<?> message) {
      return true;
    }

    @Override
    public boolean checkBusinessKeys() {
      return false;
    }
  }

  /**
   * Reliable mode: an {@code inbox} row per processed eventId. It's written in the same transaction
   * as the business change, so "processed" and "recorded as processed" can't disagree.
   */
  @Component
  @ConditionalOnProperty(name = "saga.mode", havingValue = "reliable", matchIfMissing = true)
  final class Inbox implements IdempotencyGuard {

    private final JdbcClient jdbc;
    private final Clock clock;
    private final String consumer;

    Inbox(JdbcClient jdbc, Clock clock, @Value("${spring.application.name}") String consumer) {
      this.jdbc = jdbc;
      this.clock = clock;
      this.consumer = consumer;
    }

    @Override
    public boolean firstDelivery(EventEnvelope<?> message) {
      int inserted =
          jdbc.sql(
                  """
                  INSERT INTO inbox (event_id, consumer, msg_type, received_at)
                  VALUES (:id, :consumer, :type, :at)
                  ON CONFLICT (event_id) DO NOTHING
                  """)
              .param("id", message.eventId())
              .param("consumer", consumer)
              .param("type", message.type())
              .param("at", Timestamp.from(clock.instant()))
              .update();
      return inserted == 1;
    }

    @Override
    public boolean checkBusinessKeys() {
      return true;
    }
  }
}
