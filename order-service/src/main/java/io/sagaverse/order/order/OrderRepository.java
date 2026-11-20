package io.sagaverse.order.order;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Plain SQL over {@code orders}. Every write runs inside the caller's transaction. */
@Repository
public class OrderRepository {

  private final JdbcClient jdbc;

  public OrderRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  public void insert(Order o) {
    jdbc.sql(
            """
            INSERT INTO orders (order_id, customer_id, sku, quantity, amount, status, created_at, updated_at)
            VALUES (:id, :customer, :sku, :qty, :amount, :status, :created, :updated)
            """)
        .param("id", o.orderId())
        .param("customer", o.customerId())
        .param("sku", o.sku())
        .param("qty", o.quantity())
        .param("amount", o.amount())
        .param("status", o.status().name())
        .param("created", Timestamp.from(o.createdAt()))
        .param("updated", Timestamp.from(o.updatedAt()))
        .update();
  }

  public Optional<Order> findById(String orderId) {
    return jdbc.sql("SELECT * FROM orders WHERE order_id = :id")
        .param("id", orderId)
        .query(OrderRepository::map)
        .optional();
  }

  public void updateStatus(String orderId, Order.Status status, Instant at) {
    jdbc.sql("UPDATE orders SET status = :status, updated_at = :at WHERE order_id = :id")
        .param("status", status.name())
        .param("at", Timestamp.from(at))
        .param("id", orderId)
        .update();
  }

  private static Order map(ResultSet rs, int row) throws SQLException {
    return new Order(
        rs.getString("order_id"),
        rs.getString("customer_id"),
        rs.getString("sku"),
        rs.getInt("quantity"),
        rs.getBigDecimal("amount"),
        Order.Status.valueOf(rs.getString("status")),
        rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("updated_at").toInstant());
  }
}
