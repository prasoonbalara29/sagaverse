package io.sagaverse.inventory.messaging;

import io.sagaverse.events.SagaPayload;
import java.util.List;
import java.util.UUID;

/**
 * Sends the replies a command handler produced. The handler calls this <b>inside</b> its database
 * transaction, and the implementation decides what that means (ADR-0010):
 *
 * <ul>
 *   <li>naive ({@link DirectKafkaPublisher}): send to Kafka right after the DB commits, which is a
 *       dual write;
 *   <li>reliable (Phase 4): insert into the outbox in the same transaction.
 * </ul>
 */
public interface MessagePublisher {

  void publish(UUID sagaId, List<? extends SagaPayload> messages);
}
