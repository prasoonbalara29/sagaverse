package io.sagaverse.payment.messaging;

import io.sagaverse.events.Topics;
import io.sagaverse.payment.messaging.EnvelopeCodec.MessageRejectedException;
import java.time.Duration;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Retry and dead-letter policy for every listener in this service (ADR-0007).
 *
 * <ul>
 *   <li>Transient failures: <b>blocking</b> exponential backoff on the same partition, which keeps
 *       per-order ordering (ADR-0002).
 *   <li>{@link MessageRejectedException} (malformed, unknown type, unsupported version): retrying
 *       can't help, so it goes straight to the DLT.
 *   <li>When retries run out: publish to {@code <topic>.DLT}, same key and partition, with the
 *       exception in the {@code kafka_dlt-*} headers. Then commit the offset and move on.
 * </ul>
 *
 * Spring Boot wires this {@code DefaultErrorHandler} bean into the listener container factory.
 * Deliberately a per-service copy (ADR-0001).
 */
@Configuration
@EnableConfigurationProperties(KafkaErrorHandling.RetryProperties.class)
public class KafkaErrorHandling {

  private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandling.class);

  /**
   * {@code messaging.retry.*}.
   *
   * @param initialInterval wait before the first retry
   * @param multiplier growth factor between retries
   * @param maxRetries retries after the first attempt, before the record is dead-lettered
   */
  @ConfigurationProperties("messaging.retry")
  public record RetryProperties(
      @DefaultValue("1s") Duration initialInterval,
      @DefaultValue("2.0") double multiplier,
      @DefaultValue("3") int maxRetries) {}

  @Bean
  DefaultErrorHandler kafkaErrorHandler(
      KafkaTemplate<String, String> template, RetryProperties retry) {
    var recoverer =
        new DeadLetterPublishingRecoverer(
            template,
            (record, ex) -> {
              log.error(
                  "dead-lettering {}-{}@{}: {}",
                  record.topic(),
                  record.partition(),
                  record.offset(),
                  ex.getCause() != null ? ex.getCause().toString() : ex.toString());
              return new TopicPartition(Topics.dlt(record.topic()), record.partition());
            });
    var backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
    backOff.setInitialInterval(retry.initialInterval().toMillis());
    backOff.setMultiplier(retry.multiplier());
    var handler = new DefaultErrorHandler(recoverer, backOff);
    handler.addNotRetryableExceptions(MessageRejectedException.class);
    return handler;
  }
}
