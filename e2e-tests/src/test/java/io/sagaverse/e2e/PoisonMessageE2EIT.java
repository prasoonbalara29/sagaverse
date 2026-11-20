package io.sagaverse.e2e;

import static io.sagaverse.e2e.SagaverseCluster.Service.ORDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.sagaverse.events.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * F7, poison messages (ADR-0007). A record that can never be processed must go to {@code
 * <topic>.DLT} with the reason in its headers, without being retried for long and without blocking
 * the partition for the orders behind it.
 */
@Testcontainers
class PoisonMessageE2EIT {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.2");

  static SagaverseCluster cluster;
  static KafkaProducer<String, String> producer;

  @BeforeAll
  static void start() {
    cluster = new SagaverseCluster(POSTGRES, KAFKA, "naive").startAll();
    var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producer = new KafkaProducer<>(props);
  }

  @AfterAll
  static void stop() {
    producer.close();
    cluster.close();
  }

  @Test
  void malformedCommandsOnEveryPartition_goToTheDlt_andLaterOrdersStillComplete() throws Exception {
    String marker = "poison-" + UUID.randomUUID();
    for (int partition = 0; partition < Topics.PARTITIONS; partition++) {
      producer
          .send(new ProducerRecord<>(Topics.PAYMENT_COMMANDS, partition, marker, "not json {"))
          .get();
    }

    var dead = awaitDlt(Topics.dlt(Topics.PAYMENT_COMMANDS), marker, Topics.PARTITIONS);
    assertThat(dead)
        .allSatisfy(r -> assertThat(exceptionClass(r)).endsWith("MalformedMessageException"));
    assertThat(dead).extracting(ConsumerRecord::value).containsOnly("not json {");

    // Every partition of payment.commands had a poison pill, so this order shares one of them.
    assertThat(placeOrderAndAwaitStatus()).isEqualTo("CONFIRMED");
  }

  @Test
  void unknownTypeOnInventoryCommands_goesToTheDlt() throws Exception {
    String marker = "poison-" + UUID.randomUUID();
    producer
        .send(new ProducerRecord<>(Topics.INVENTORY_COMMANDS, marker, envelope("ShipOrder", 1)))
        .get();

    var dead = awaitDlt(Topics.dlt(Topics.INVENTORY_COMMANDS), marker, 1);
    assertThat(exceptionClass(dead.get(0))).endsWith("UnknownMessageTypeException");
  }

  @Test
  void futureVersionOnPaymentReplies_goesToTheDlt() throws Exception {
    String marker = "poison-" + UUID.randomUUID();
    producer
        .send(new ProducerRecord<>(Topics.PAYMENT_REPLIES, marker, envelope("PaymentDeclined", 2)))
        .get();

    var dead = awaitDlt(Topics.dlt(Topics.PAYMENT_REPLIES), marker, 1);
    assertThat(exceptionClass(dead.get(0))).endsWith("UnsupportedVersionException");
  }

  // --- helpers ---------------------------------------------------------------------------

  private static String envelope(String type, int version) {
    return """
    {"eventId":"%s","sagaId":"%s","type":"%s","version":%d,"occurredAt":"2026-11-20T10:00:00Z",
     "payload":{"orderId":"o-1","reason":"x"}}\
    """
        .formatted(UUID.randomUUID(), UUID.randomUUID(), type, version);
  }

  /** Reads the DLT until {@code count} records with this key arrive, within 15 s. */
  private static List<ConsumerRecord<String, String>> awaitDlt(String dlt, String key, int count) {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-reader-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    List<ConsumerRecord<String, String>> found = new ArrayList<>();
    try (var consumer = new KafkaConsumer<String, String>(props)) {
      consumer.subscribe(List.of(dlt));
      await()
          .atMost(Duration.ofSeconds(15))
          .pollInSameThread()
          .untilAsserted(() -> {
            consumer
                .poll(Duration.ofMillis(300))
                .forEach(r -> {
                  if (key.equals(r.key())) {
                    found.add(r);
                  }
                });
            assertThat(found).as("records with key %s on %s", key, dlt).hasSize(count);
          });
    }
    return found;
  }

  private static String exceptionClass(ConsumerRecord<String, String> r) {
    Header h = r.headers().lastHeader("kafka_dlt-exception-cause-fqcn");
    if (h == null) {
      h = r.headers().lastHeader("kafka_dlt-exception-fqcn");
    }
    return h == null ? "<no header>" : new String(h.value(), StandardCharsets.UTF_8);
  }

  private static String placeOrderAndAwaitStatus() {
    var orders = RestClient.create(cluster.baseUrl(ORDER));
    JsonNode placed =
        orders
            .post()
            .uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .body(Map.of("customerId", "cust-e2e", "sku", "SKU-1", "quantity", 1, "amount", "5.00"))
            .retrieve()
            .body(JsonNode.class);
    String orderId = placed.path("orderId").asText();
    await()
        .atMost(Duration.ofSeconds(30))
        .until(() ->
            !"PENDING"
                .equals(
                    orders
                        .get()
                        .uri("/orders/{id}", orderId)
                        .retrieve()
                        .body(JsonNode.class)
                        .path("status")
                        .asText()));
    return orders
        .get()
        .uri("/orders/{id}", orderId)
        .retrieve()
        .body(JsonNode.class)
        .path("status")
        .asText();
  }
}
