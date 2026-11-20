package io.sagaverse.order.messaging;

import io.sagaverse.events.Topics;
import io.sagaverse.order.saga.SagaOrchestrator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Feeds replies from payment and inventory into the orchestrator. */
@Component
public class ReplyListener {

  private final EnvelopeCodec codec;
  private final SagaOrchestrator orchestrator;

  public ReplyListener(EnvelopeCodec codec, SagaOrchestrator orchestrator) {
    this.codec = codec;
    this.orchestrator = orchestrator;
  }

  @KafkaListener(topics = {Topics.PAYMENT_REPLIES, Topics.INVENTORY_REPLIES})
  public void onReply(ConsumerRecord<String, String> record) {
    try (var orderId = MDC.putCloseable("orderId", record.key())) {
      var envelope = codec.decode(record.value());
      try (var sagaId = MDC.putCloseable("sagaId", envelope.sagaId().toString())) {
        orchestrator.onReply(envelope);
      }
    }
  }
}
