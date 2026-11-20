package io.sagaverse.inventory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Boots inventory-service against real Postgres and Kafka containers. Images resolve through the
 * mirror configured in testcontainers.properties.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class InventoryServiceApplicationIT {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Container @ServiceConnection
  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.2");

  @Autowired TestRestTemplate rest;
  @Autowired JdbcTemplate jdbc;
  @Autowired KafkaAdmin kafkaAdmin;

  @Test
  void healthIsUpIncludingTheDatabase() {
    JsonNode health = rest.getForObject("/actuator/health", JsonNode.class);

    assertThat(health.path("status").asText()).isEqualTo("UP");
    assertThat(health.path("components").path("db").path("status").asText()).isEqualTo("UP");
  }

  @Test
  void flywayCreatedExactlyThisServicesTables() {
    var tables =
        jdbc.queryForList(
            "SELECT table_name FROM information_schema.tables "
                + "WHERE table_schema = 'public' AND table_name <> 'flyway_schema_history'",
            String.class);

    assertThat(tables).containsExactlyInAnyOrder("stock", "reservation", "outbox", "inbox");
  }

  @Test
  void demoStockIsSeeded() {
    assertThat(jdbc.queryForMap("SELECT sku, available FROM stock WHERE sku = 'SKU-1'"))
        .containsEntry("available", 100);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM stock", Integer.class)).isEqualTo(3);
  }

  @Test
  void kafkaBrokerIsReachable() throws Exception {
    try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
      assertThat(admin.describeCluster().nodes().get(30, SECONDS)).hasSize(1);
    }
  }
}
