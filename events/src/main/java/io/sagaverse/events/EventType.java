package io.sagaverse.events;

import io.sagaverse.events.InventoryMessages.ReleaseStock;
import io.sagaverse.events.InventoryMessages.ReserveStock;
import io.sagaverse.events.InventoryMessages.StockReleased;
import io.sagaverse.events.InventoryMessages.StockReserved;
import io.sagaverse.events.InventoryMessages.StockUnavailable;
import io.sagaverse.events.PaymentMessages.ChargePayment;
import io.sagaverse.events.PaymentMessages.PaymentCharged;
import io.sagaverse.events.PaymentMessages.PaymentDeclined;
import io.sagaverse.events.PaymentMessages.PaymentRefunded;
import io.sagaverse.events.PaymentMessages.RefundPayment;
import java.util.Arrays;
import java.util.Optional;

/**
 * Catalogue of every message type: its wire name (the envelope's {@code type}), its payload class,
 * and the topic it travels on. This is contract metadata, not logic. It lets a consumer turn {@code
 * type} back into a class, and lets tests check the catalogue is complete.
 */
public enum EventType {
  CHARGE_PAYMENT("ChargePayment", ChargePayment.class, Topics.PAYMENT_COMMANDS),
  REFUND_PAYMENT("RefundPayment", RefundPayment.class, Topics.PAYMENT_COMMANDS),
  PAYMENT_CHARGED("PaymentCharged", PaymentCharged.class, Topics.PAYMENT_REPLIES),
  PAYMENT_DECLINED("PaymentDeclined", PaymentDeclined.class, Topics.PAYMENT_REPLIES),
  PAYMENT_REFUNDED("PaymentRefunded", PaymentRefunded.class, Topics.PAYMENT_REPLIES),
  RESERVE_STOCK("ReserveStock", ReserveStock.class, Topics.INVENTORY_COMMANDS),
  RELEASE_STOCK("ReleaseStock", ReleaseStock.class, Topics.INVENTORY_COMMANDS),
  STOCK_RESERVED("StockReserved", StockReserved.class, Topics.INVENTORY_REPLIES),
  STOCK_UNAVAILABLE("StockUnavailable", StockUnavailable.class, Topics.INVENTORY_REPLIES),
  STOCK_RELEASED("StockReleased", StockReleased.class, Topics.INVENTORY_REPLIES);

  private final String wireName;
  private final Class<? extends SagaPayload> payloadClass;
  private final String topic;

  EventType(String wireName, Class<? extends SagaPayload> payloadClass, String topic) {
    this.wireName = wireName;
    this.payloadClass = payloadClass;
    this.topic = topic;
  }

  public String wireName() {
    return wireName;
  }

  public Class<? extends SagaPayload> payloadClass() {
    return payloadClass;
  }

  public String topic() {
    return topic;
  }

  /** Looks up a wire name. Empty means unknown type, which consumers send to the DLT. */
  public static Optional<EventType> fromWireName(String wireName) {
    return Arrays.stream(values()).filter(t -> t.wireName.equals(wireName)).findFirst();
  }

  /** The type of a concrete payload. */
  public static EventType of(SagaPayload payload) {
    return Arrays.stream(values())
        .filter(t -> t.payloadClass == payload.getClass())
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("Unregistered payload " + payload.getClass()));
  }
}
