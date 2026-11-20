-- A RefundPayment that arrives when nothing was ever charged writes a tombstone: a payment row with
-- status REFUNDED and no amount. A late (e.g. replayed from the DLT) ChargePayment for that saga then
-- sees the tombstone and is declined instead of taking money (ADR-0008).
-- The inline CHECK (amount > 0) already passes for NULL.
ALTER TABLE payment ALTER COLUMN amount DROP NOT NULL;
ALTER TABLE payment ALTER COLUMN customer_id DROP NOT NULL;
