-- payment-service schema (payment_db). See ADR-0001, ADR-0005, ADR-0008.

-- One row per saga: the stored outcome of ChargePayment. Repeated commands for the same saga reply
-- with this outcome instead of charging again (business idempotency, reliable mode).
CREATE TABLE payment (
    payment_id  UUID           PRIMARY KEY,
    saga_id     UUID           NOT NULL UNIQUE,
    order_id    VARCHAR(64)    NOT NULL,
    customer_id VARCHAR(64)    NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    -- CHARGED, DECLINED, REFUNDED
    status      VARCHAR(16)    NOT NULL,
    reason      VARCHAR(128),
    created_at  TIMESTAMPTZ    NOT NULL,
    updated_at  TIMESTAMPTZ    NOT NULL
);

-- Append-only money movements. Deliberately has no unique key, so a naive double charge shows up
-- as two CHARGE rows for one saga. That's what the InvariantChecker looks for (ADR-0010).
CREATE TABLE ledger (
    id          BIGSERIAL      PRIMARY KEY,
    saga_id     UUID           NOT NULL,
    order_id    VARCHAR(64)    NOT NULL,
    customer_id VARCHAR(64)    NOT NULL,
    -- CHARGE (positive amount) or REFUND (negative amount)
    entry_type  VARCHAR(16)    NOT NULL,
    amount      NUMERIC(12, 2) NOT NULL,
    created_at  TIMESTAMPTZ    NOT NULL
);

CREATE INDEX ledger_saga_idx ON ledger (saga_id);

-- Transactional outbox (ADR-0004). Used in reliable mode only.
CREATE TABLE outbox (
    id           BIGSERIAL    PRIMARY KEY,
    event_id     UUID         NOT NULL UNIQUE,
    saga_id      UUID         NOT NULL,
    msg_key      VARCHAR(64)  NOT NULL,
    topic        VARCHAR(128) NOT NULL,
    msg_type     VARCHAR(64)  NOT NULL,
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Inbox for transport-level dedup (ADR-0005). Used in reliable mode only.
CREATE TABLE inbox (
    event_id    UUID        PRIMARY KEY,
    consumer    VARCHAR(64) NOT NULL,
    msg_type    VARCHAR(64) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);
