package io.sagaverse.inventory.messaging;

import io.sagaverse.events.InventoryMessages.InventoryCommand;
import io.sagaverse.events.Topics;
import io.sagaverse.inventory.stock.InventoryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumes {@code inventory.commands}. */
@Component
public class CommandListener {

  private static final Logger log = LoggerFactory.getLogger(CommandListener.class);

  private final EnvelopeCodec codec;
  private final InventoryService inventory;

  public CommandListener(EnvelopeCodec codec, InventoryService inventory) {
    this.codec = codec;
    this.inventory = inventory;
  }

  @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
  public void onCommand(ConsumerRecord<String, String> record) {
    try (var orderId = MDC.putCloseable("orderId", record.key())) {
      var command = codec.decode(record.value(), InventoryCommand.class);
      try (var sagaId = MDC.putCloseable("sagaId", command.sagaId().toString())) {
        log.info("received {} (eventId={})", command.type(), command.eventId());
        inventory.handle(command);
      }
    }
  }
}
