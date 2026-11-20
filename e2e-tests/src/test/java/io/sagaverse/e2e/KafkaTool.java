package io.sagaverse.e2e;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Small Kafka helpers for scenarios: find a record, re-send it, wait until a group has read
 * everything.
 */
final class KafkaTool {

  /** Exact decimals: re-serializing must not turn 11.00 into 11.0 (ADR-0011). */
  private static final ObjectMapper JSON =
      new ObjectMapper()
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);

  private final String bootstrap;

  KafkaTool(String bootstrap) {
    this.bootstrap = bootstrap;
  }

  /** The first record on {@code topic} with this key whose envelope type is {@code type}. */
  ConsumerRecord<String, String> find(String topic, String key, String type) {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "scenario-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    try (var consumer = new KafkaConsumer<String, String>(props)) {
      consumer.subscribe(List.of(topic));
      String marker = "\"type\":\"" + type + "\"";
      return await()
          .atMost(Duration.ofSeconds(20))
          .pollInSameThread()
          .until(
              () -> {
                for (var r : consumer.poll(Duration.ofMillis(300))) {
                  if (key.equals(r.key()) && r.value().replace(" ", "").contains(marker)) {
                    return Optional.of(r);
                  }
                }
                return Optional.<ConsumerRecord<String, String>>empty();
              },
              Optional::isPresent)
          .orElseThrow();
    }
  }

  void send(String topic, String key, String value) {
    var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (var producer = new KafkaProducer<String, String>(props)) {
      producer.send(new ProducerRecord<>(topic, key, value)).get(10, SECONDS);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Waits until consumer group {@code group} has committed offsets up to the end of every partition
   * of {@code topic}.
   */
  void awaitConsumed(String group, String topic) {
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap))) {
      await()
          .atMost(Duration.ofSeconds(30))
          .pollInSameThread()
          .until(() -> {
            Map<TopicPartition, OffsetAndMetadata> committed =
                admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
            var partitions =
                admin
                    .describeTopics(List.of(topic))
                    .allTopicNames()
                    .get()
                    .get(topic)
                    .partitions()
                    .stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
            var ends = admin.listOffsets(partitions).all().get();
            return ends.entrySet().stream()
                .allMatch(e -> {
                  var c = committed.get(e.getKey());
                  return e.getValue().offset() == 0
                      || (c != null && c.offset() >= e.getValue().offset());
                });
          });
    }
  }

  /** Same envelope, new eventId: a logical resend rather than a redelivery. */
  static String withNewEventId(String json) {
    try {
      ObjectNode node = (ObjectNode) JSON.readTree(json);
      node.put("eventId", UUID.randomUUID().toString());
      return JSON.writeValueAsString(node);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
