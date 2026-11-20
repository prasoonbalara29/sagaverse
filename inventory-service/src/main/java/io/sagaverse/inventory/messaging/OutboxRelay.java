package io.sagaverse.inventory.messaging;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Moves committed outbox rows to Kafka (ADR-0004).
 *
 * <p>Every {@code messaging.outbox.poll-interval-ms} it locks a batch of unpublished rows in id
 * order, sends each one synchronously, and marks it published. If a send fails, it stops at that
 * row, so later rows never overtake it (ordering, ADR-0002), and retries on the next tick.
 *
 * <p>A crash after a send but before {@code published_at} is committed means that row is sent
 * again. That makes delivery <b>at-least-once</b>, and it's why every consumer dedupes (ADR-0005).
 */
@Component
@EnableScheduling
@ConditionalOnProperty(name = "saga.mode", havingValue = "reliable", matchIfMissing = true)
public class OutboxRelay {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

  private record Row(
      long id, UUID eventId, UUID sagaId, String key, String topic, String type, String payload) {}

  private final JdbcClient jdbc;
  private final TransactionTemplate tx;
  private final KafkaTemplate<String, String> kafka;
  private final Clock clock;

  public OutboxRelay(
      JdbcClient jdbc, TransactionTemplate tx, KafkaTemplate<String, String> kafka, Clock clock) {
    this.jdbc = jdbc;
    this.tx = tx;
    this.kafka = kafka;
    this.clock = clock;
  }

  @Scheduled(fixedDelayString = "${messaging.outbox.poll-interval-ms:200}")
  public void publishPending() {
    tx.executeWithoutResult(status -> {
      var rows =
          jdbc.sql(
                  """
                  SELECT id, event_id, saga_id, msg_key, topic, msg_type, payload::text AS payload
                    FROM outbox
                   WHERE published_at IS NULL
                   ORDER BY id
                   LIMIT 100
                     FOR UPDATE SKIP LOCKED
                  """)
              .query((rs, n) ->
                  new Row(
                      rs.getLong("id"),
                      rs.getObject("event_id", UUID.class),
                      rs.getObject("saga_id", UUID.class),
                      rs.getString("msg_key"),
                      rs.getString("topic"),
                      rs.getString("msg_type"),
                      rs.getString("payload")))
              .list();
      for (Row row : rows) {
        if (!send(row)) {
          break; // keep order: never send a later row before this one
        }
      }
    });
  }

  private boolean send(Row row) {
    try (var a = MDC.putCloseable("sagaId", row.sagaId().toString());
        var b = MDC.putCloseable("orderId", row.key())) {
      kafka.send(row.topic(), row.key(), row.payload()).get(10, TimeUnit.SECONDS);
      jdbc.sql("UPDATE outbox SET published_at = :now WHERE id = :id")
          .param("now", Timestamp.from(clock.instant()))
          .param("id", row.id())
          .update();
      log.info(
          "relayed {} to {} (eventId={}, outboxId={})",
          row.type(),
          row.topic(),
          row.eventId(),
          row.id());
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception e) {
      log.warn("relay of outbox row {} failed, will retry: {}", row.id(), e.toString());
      return false;
    }
  }
}
