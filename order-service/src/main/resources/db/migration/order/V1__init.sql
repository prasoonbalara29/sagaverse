-- order-service schema (order_db). See ADR-0001, ADR-0003, ADR-0004, ADR-0005.

-- The business record the customer sees.
CREATE TABLE orders (
    order_id    VARCHAR(64)    PRIMARY KEY,
    customer_id VARCHAR(64)    NOT NULL,
    sku         VARCHAR(64)    NOT NULL,
    quantity    INT            NOT NULL CHECK (quantity > 0),
    amount      NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    -- PENDING, CONFIRMED, CANCELLED, CANCELLED_NEEDS_ATTENTION
    status      VARCHAR(32)    NOT NULL,
    created_at  TIMESTAMPTZ    NOT NULL,
    updated_at  TIMESTAMPTZ    NOT NULL
);

-- Orchestrator state: one row per saga, updated in the same transaction as the order and the
-- outbox row for the next command (ADR-0003).
CREATE TABLE saga_instance (
    saga_id               UUID        PRIMARY KEY,
    order_id              VARCHAR(64) NOT NULL REFERENCES orders (order_id),
    -- PAYMENT_PENDING, INVENTORY_PENDING, COMPENSATING,
    -- COMPLETED, FAILED, COMPENSATED, COMPENSATION_FAILED
    state                 VARCHAR(32) NOT NULL,
    current_step          VARCHAR(32),
    step_attempts         INT         NOT NULL DEFAULT 0,
    compensation_attempts INT         NOT NULL DEFAULT 0,
    -- Compensations sent but not yet acknowledged, e.g. ["RefundPayment","ReleaseStock"]
    compensations_pending JSONB       NOT NULL DEFAULT '[]'::jsonb,
    -- When the current step counts as stuck (ADR-0009). NULL in terminal states.
    deadline_at           TIMESTAMPTZ,
    -- Optimistic lock: two concurrent transitions cannot both win.
    version               BIGINT      NOT NULL DEFAULT 0,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL
);

CREATE INDEX saga_instance_order_idx ON saga_instance (order_id);

-- The sweeper's query only ever looks at non-terminal sagas ordered by deadline.
CREATE INDEX saga_instance_due_idx ON saga_instance (deadline_at)
    WHERE state IN ('PAYMENT_PENDING', 'INVENTORY_PENDING', 'COMPENSATING');

-- Append-only history, for humans and for the trace demo. Never read by the state machine.
CREATE TABLE saga_transition (
    id         BIGSERIAL   PRIMARY KEY,
    saga_id    UUID        NOT NULL REFERENCES saga_instance (saga_id),
    from_state VARCHAR(32),
    to_state   VARCHAR(32) NOT NULL,
    trigger    VARCHAR(64) NOT NULL,
    at         TIMESTAMPTZ NOT NULL
);

CREATE INDEX saga_transition_saga_idx ON saga_transition (saga_id, id);

-- Transactional outbox (ADR-0004). Used in reliable mode only; same table in every service.
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
