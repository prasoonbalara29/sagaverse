package io.sagaverse.e2e;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.sagaverse.events.Topics;
import io.sagaverse.inventory.InventoryServiceApplication;
import io.sagaverse.order.OrderServiceApplication;
import io.sagaverse.payment.PaymentServiceApplication;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The three real services, each in its own Spring context in this JVM, sharing one Kafka and one
 * Postgres (a separate database per service), as in compose (ADR-0011).
 *
 * <p>A service can be stopped and started again mid-test. Phase 5 uses that to simulate a crash. DB
 * access for assertions goes through {@link #db}, which bypasses the services, so you can look at a
 * stopped service's data too.
 */
final class SagaverseCluster implements AutoCloseable {

  /** The services under test. */
  enum Service {
    ORDER("order_db", OrderServiceApplication::application),
    PAYMENT("payment_db", PaymentServiceApplication::application),
    INVENTORY("inventory_db", InventoryServiceApplication::application);

    final String database;
    final Supplier<SpringApplicationBuilder> app;

    Service(String database, Supplier<SpringApplicationBuilder> app) {
      this.database = database;
      this.app = app;
    }
  }

  private final PostgreSQLContainer<?> postgres;
  private final KafkaContainer kafka;
  private final String mode;
  private final Map<Service, ConfigurableApplicationContext> running = new EnumMap<>(Service.class);
  private final Map<Service, JdbcTemplate> dbs = new EnumMap<>(Service.class);
  private final Map<Service, Map<String, Object>> overrides = new EnumMap<>(Service.class);

  /**
   * @param mode value of {@code saga.mode} for every service: "naive" or "reliable"
   */
  SagaverseCluster(PostgreSQLContainer<?> postgres, KafkaContainer kafka, String mode) {
    this.postgres = postgres;
    this.kafka = kafka;
    this.mode = mode;
    createDatabases();
    createTopics();
  }

  /** Extra config for one service, e.g. a failure trigger. Applies to the next {@link #start}. */
  SagaverseCluster with(Service s, String key, Object value) {
    overrides.computeIfAbsent(s, k -> new HashMap<>()).put(key, value);
    return this;
  }

  SagaverseCluster startAll() {
    for (Service s : Service.values()) {
      start(s);
    }
    return this;
  }

  void start(Service s) {
    Map<String, Object> props = new HashMap<>();
    props.put("server.port", 0);
    props.put("spring.datasource.url", jdbcUrl(s.database));
    props.put("spring.datasource.username", postgres.getUsername());
    props.put("spring.datasource.password", postgres.getPassword());
    props.put("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
    props.put("saga.mode", mode);
    props.putAll(overrides.getOrDefault(s, Map.of()));
    // Command-line style args beat the service's yml, so these win over its dev defaults.
    String[] args =
        props.entrySet().stream()
            .map(e -> "--" + e.getKey() + "=" + e.getValue())
            .toArray(String[]::new);
    running.put(s, s.app.get().run(args));
  }

  /**
   * Stops and starts one service, with the current overrides. This is how tests simulate a crash
   * and restart.
   */
  void restart(Service s) {
    stop(s);
    start(s);
  }

  void stop(Service s) {
    var ctx = running.remove(s);
    if (ctx != null) {
      ctx.close();
    }
  }

  String baseUrl(Service s) {
    var ctx = (WebServerApplicationContext) running.get(s);
    return "http://localhost:" + ctx.getWebServer().getPort();
  }

  /** Direct access to a service's database, for assertions only. */
  JdbcTemplate db(Service s) {
    return dbs.computeIfAbsent(
        s,
        key ->
            new JdbcTemplate(
                new DriverManagerDataSource(
                    jdbcUrl(key.database), postgres.getUsername(), postgres.getPassword())));
  }

  @Override
  public void close() {
    for (Service s : Service.values()) {
      stop(s);
    }
  }

  private String jdbcUrl(String database) {
    return "jdbc:postgresql://"
        + postgres.getHost()
        + ":"
        + postgres.getFirstMappedPort()
        + "/"
        + database;
  }

  private void createDatabases() {
    var admin =
        new JdbcTemplate(
            new DriverManagerDataSource(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()));
    for (Service s : Service.values()) {
      admin.execute("DROP DATABASE IF EXISTS " + s.database + " WITH (FORCE)");
      admin.execute("CREATE DATABASE " + s.database);
    }
  }

  private void createTopics() {
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      var existing = admin.listTopics().names().get(30, SECONDS);
      var missing =
          Topics.ALL.stream()
              .filter(t -> !existing.contains(t))
              .map(t -> new NewTopic(t, Topics.PARTITIONS, (short) 1))
              .toList();
      admin.createTopics(missing).all().get(30, SECONDS);
    } catch (Exception e) {
      throw new IllegalStateException("could not create topics", e);
    }
  }
}
