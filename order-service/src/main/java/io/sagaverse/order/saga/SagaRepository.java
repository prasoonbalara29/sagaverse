package io.sagaverse.order.saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;

/** Plain SQL over {@code saga_instance} and {@code saga_transition} (ADR-0003). */
@Repository
public class SagaRepository {

  private static final TypeReference<List<String>> STRINGS = new TypeReference<>() {};

  private final JdbcClient jdbc;
  private final ObjectMapper json;

  public SagaRepository(JdbcClient jdbc, ObjectMapper json) {
    this.jdbc = jdbc;
    this.json = json;
  }

  public void insert(SagaInstance s) {
    jdbc.sql(
            """
            INSERT INTO saga_instance
              (saga_id, order_id, state, current_step, step_attempts, compensations_pending,
               deadline_at, version, created_at, updated_at)
            VALUES (:id, :order, :state, :step, :attempts, CAST(:pending AS jsonb),
                    :deadline, :version, :created, :updated)
            """)
        .param("id", s.sagaId())
        .param("order", s.orderId())
        .param("state", s.state().name())
        .param("step", s.currentStep())
        .param("attempts", s.stepAttempts())
        .param("pending", toJson(s.compensationsPending()))
        .param("deadline", ts(s.deadlineAt()))
        .param("version", s.version())
        .param("created", ts(s.createdAt()))
        .param("updated", ts(s.updatedAt()))
        .update();
  }

  public Optional<SagaInstance> findById(UUID sagaId) {
    return jdbc.sql("SELECT * FROM saga_instance WHERE saga_id = :id")
        .param("id", sagaId)
        .query(this::map)
        .optional();
  }

  public Optional<SagaInstance> findLatestByOrderId(String orderId) {
    return jdbc.sql(
            "SELECT * FROM saga_instance WHERE order_id = :id ORDER BY created_at DESC LIMIT 1")
        .param("id", orderId)
        .query(this::map)
        .optional();
  }

  /**
   * Writes the new state if nobody else changed the row since we read it.
   *
   * @throws OptimisticLockingFailureException if {@code expectedVersion} is stale
   */
  public SagaInstance update(SagaInstance updated, long expectedVersion) {
    int rows =
        jdbc.sql(
                """
                UPDATE saga_instance
                   SET state = :state, current_step = :step, step_attempts = :attempts,
                       compensations_pending = CAST(:pending AS jsonb),
                       deadline_at = :deadline, version = version + 1, updated_at = :updated
                 WHERE saga_id = :id AND version = :expected
                """)
            .param("state", updated.state().name())
            .param("step", updated.currentStep())
            .param("attempts", updated.stepAttempts())
            .param("pending", toJson(updated.compensationsPending()))
            .param("deadline", ts(updated.deadlineAt()))
            .param("updated", ts(updated.updatedAt()))
            .param("id", updated.sagaId())
            .param("expected", expectedVersion)
            .update();
    if (rows != 1) {
      throw new OptimisticLockingFailureException(
          "saga "
              + updated.sagaId()
              + " changed concurrently (expected version "
              + expectedVersion
              + ")");
    }
    return findById(updated.sagaId()).orElseThrow();
  }

  public void recordTransition(
      UUID sagaId, @Nullable SagaState from, SagaState to, String trigger, Instant at) {
    jdbc.sql(
            """
            INSERT INTO saga_transition (saga_id, from_state, to_state, trigger, at)
            VALUES (:id, :from, :to, :trigger, :at)
            """)
        .param("id", sagaId)
        .param("from", from == null ? null : from.name())
        .param("to", to.name())
        .param("trigger", trigger)
        .param("at", ts(at))
        .update();
  }

  public List<Transition> transitions(UUID sagaId) {
    return jdbc.sql("SELECT * FROM saga_transition WHERE saga_id = :id ORDER BY id")
        .param("id", sagaId)
        .query((rs, row) ->
            new Transition(
                rs.getString("from_state"),
                rs.getString("to_state"),
                rs.getString("trigger"),
                rs.getTimestamp("at").toInstant()))
        .list();
  }

  /** One row of the append-only history. */
  public record Transition(@Nullable String from, String to, String trigger, Instant at) {}

  private static @Nullable Timestamp ts(@Nullable Instant i) {
    return i == null ? null : Timestamp.from(i);
  }

  private String toJson(List<String> values) {
    try {
      return json.writeValueAsString(values);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private List<String> fromJson(String value) {
    try {
      return json.readValue(value, STRINGS);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("bad compensations_pending: " + value, e);
    }
  }

  private SagaInstance map(ResultSet rs, int row) throws SQLException {
    Timestamp deadline = rs.getTimestamp("deadline_at");
    return new SagaInstance(
        rs.getObject("saga_id", UUID.class),
        rs.getString("order_id"),
        SagaState.valueOf(rs.getString("state")),
        rs.getString("current_step"),
        rs.getInt("step_attempts"),
        fromJson(rs.getString("compensations_pending")),
        deadline == null ? null : deadline.toInstant(),
        rs.getLong("version"),
        rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("updated_at").toInstant());
  }
}
