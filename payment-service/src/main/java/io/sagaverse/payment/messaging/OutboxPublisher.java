package io.sagaverse.payment.messaging;

import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.EventType;
import io.sagaverse.events.SagaPayload;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * <b>Reliable mode: the transactional outbox</b> (ADR-0004). Instead of sending to Kafka, write the
 * message into the {@code outbox} table in the <b>same</b> transaction as the business change.
 * Either both commit or neither does. {@link OutboxRelay} sends it to Kafka afterwards, as often as
 * it takes.
 */
@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "reliable", matchIfMissing = true)
public class OutboxPublisher implements MessagePublisher {

  private final JdbcClient jdbc;
  private final EnvelopeCodec codec;
  private final Clock clock;

  public OutboxPublisher(JdbcClient jdbc, EnvelopeCodec codec, Clock clock) {
    this.jdbc = jdbc;
    this.codec = codec;
    this.clock = clock;
  }

  @Override
  public void publish(UUID sagaId, List<? extends SagaPayload> messages) {
    if (messages.isEmpty()) {
      return;
    }
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      // Without a transaction the outbox row is just another dual write.
      throw new IllegalStateException("OutboxPublisher must be called inside a transaction");
    }
    var now = Timestamp.from(clock.instant());
    for (SagaPayload payload : messages) {
      var envelope = EventEnvelope.of(sagaId, payload, clock.instant());
      jdbc.sql(
              """
              INSERT INTO outbox (event_id, saga_id, msg_key, topic, msg_type, payload, created_at)
              VALUES (:event, :saga, :key, :topic, :type, CAST(:payload AS jsonb), :now)
              """)
          .param("event", envelope.eventId())
          .param("saga", sagaId)
          .param("key", payload.orderId())
          .param("topic", EventType.of(payload).topic())
          .param("type", envelope.type())
          .param("payload", codec.encode(envelope))
          .param("now", now)
          .update();
    }
  }
}
