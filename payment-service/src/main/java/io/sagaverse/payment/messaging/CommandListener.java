package io.sagaverse.payment.messaging;

import io.sagaverse.events.PaymentMessages.PaymentCommand;
import io.sagaverse.events.Topics;
import io.sagaverse.payment.payment.PaymentService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumes {@code payment.commands}. */
@Component
public class CommandListener {

  private static final Logger log = LoggerFactory.getLogger(CommandListener.class);

  private final EnvelopeCodec codec;
  private final PaymentService payments;

  public CommandListener(EnvelopeCodec codec, PaymentService payments) {
    this.codec = codec;
    this.payments = payments;
  }

  @KafkaListener(topics = Topics.PAYMENT_COMMANDS)
  public void onCommand(ConsumerRecord<String, String> record) {
    try (var orderId = MDC.putCloseable("orderId", record.key())) {
      var command = codec.decode(record.value(), PaymentCommand.class);
      try (var sagaId = MDC.putCloseable("sagaId", command.sagaId().toString())) {
        log.info("received {} (eventId={})", command.type(), command.eventId());
        payments.handle(command);
      }
    }
  }
}
