package io.sagaverse.inventory.messaging;

import io.sagaverse.events.EventEnvelope;
import io.sagaverse.events.EventType;
import io.sagaverse.events.SagaPayload;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * <b>Naive mode: the dual write.</b> The database commits first, then this sends to Kafka. If the
 * process dies between the two, the reply is lost and the saga hangs forever. That's failure mode
 * F4, kept on purpose (ADR-0004, ADR-0010).
 */
@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "naive")
public class DirectKafkaPublisher implements MessagePublisher {

  private static final Logger log = LoggerFactory.getLogger(DirectKafkaPublisher.class);

  private final KafkaTemplate<String, String> kafka;
  private final EnvelopeCodec codec;
  private final Clock clock;

  public DirectKafkaPublisher(
      KafkaTemplate<String, String> kafka, EnvelopeCodec codec, Clock clock) {
    this.kafka = kafka;
    this.codec = codec;
    this.clock = clock;
  }

  @Override
  public void publish(UUID sagaId, List<? extends SagaPayload> messages) {
    if (messages.isEmpty()) {
      return;
    }
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              // <-- a crash right here is the lost-event bug.
              sendAll(sagaId, messages);
            }
          });
    } else {
      sendAll(sagaId, messages);
    }
  }

  private void sendAll(UUID sagaId, List<? extends SagaPayload> messages) {
    for (SagaPayload payload : messages) {
      var envelope = EventEnvelope.of(sagaId, payload, clock.instant());
      String topic = EventType.of(payload).topic();
      try {
        kafka.send(topic, payload.orderId(), codec.encode(envelope)).get(10, TimeUnit.SECONDS);
        log.info("sent {} to {} (eventId={})", envelope.type(), topic, envelope.eventId());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted sending " + envelope.type(), e);
      } catch (ExecutionException | TimeoutException e) {
        // The DB already committed. Naive mode has no way to recover this message.
        log.error("LOST {} for saga {}: {}", envelope.type(), sagaId, e.toString());
        throw new IllegalStateException("send failed after commit", e);
      }
    }
  }
}
