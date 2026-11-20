package io.sagaverse.events;

import java.util.List;

/**
 * Topic names (ADR-0002). Every record is keyed by orderId, so all messages for one order land on
 * the same partition and are consumed in the order they were produced.
 */
public final class Topics {

  private Topics() {}

  public static final String PAYMENT_COMMANDS = "payment.commands";
  public static final String PAYMENT_REPLIES = "payment.replies";
  public static final String INVENTORY_COMMANDS = "inventory.commands";
  public static final String INVENTORY_REPLIES = "inventory.replies";

  /** Poison records go to {@code <topic>.DLT} (ADR-0007). */
  public static final String DLT_SUFFIX = ".DLT";

  /** Partitions per topic. Replication factor is 1 (single broker). */
  public static final int PARTITIONS = 3;

  /** The four main topics. */
  public static final List<String> MAIN =
      List.of(PAYMENT_COMMANDS, PAYMENT_REPLIES, INVENTORY_COMMANDS, INVENTORY_REPLIES);

  /** Main topics plus their dead-letter topics, i.e. everything topics-init must create. */
  public static final List<String> ALL =
      List.of(
          PAYMENT_COMMANDS,
          PAYMENT_REPLIES,
          INVENTORY_COMMANDS,
          INVENTORY_REPLIES,
          dlt(PAYMENT_COMMANDS),
          dlt(PAYMENT_REPLIES),
          dlt(INVENTORY_COMMANDS),
          dlt(INVENTORY_REPLIES));

  public static String dlt(String topic) {
    return topic + DLT_SUFFIX;
  }
}
