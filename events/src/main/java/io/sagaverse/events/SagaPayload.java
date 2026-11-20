package io.sagaverse.events;

import io.sagaverse.events.InventoryMessages.InventoryCommand;
import io.sagaverse.events.InventoryMessages.InventoryReply;
import io.sagaverse.events.PaymentMessages.PaymentCommand;
import io.sagaverse.events.PaymentMessages.PaymentReply;

/**
 * Every message payload carries the orderId, which is also its Kafka key.
 *
 * <p>The hierarchy is sealed, so a {@code switch} over {@link PaymentCommand} (or any of the
 * others) is checked for exhaustiveness by the compiler. Adding a message type without handling it
 * becomes a compile error.
 */
public sealed interface SagaPayload
    permits PaymentCommand, PaymentReply, InventoryCommand, InventoryReply {

  /** Business identity of the order. Used as the Kafka record key. */
  String orderId();
}
