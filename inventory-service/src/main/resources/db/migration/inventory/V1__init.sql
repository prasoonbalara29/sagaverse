-- inventory-service schema (inventory_db). See ADR-0001, ADR-0005, ADR-0008.

CREATE TABLE stock (
    sku        VARCHAR(64) PRIMARY KEY,
    available  INT         NOT NULL CHECK (available >= 0),
    updated_at TIMESTAMPTZ NOT NULL
);

-- One row per saga. RESERVED holds stock; RELEASED gives it back. A RELEASED row written before any
-- reservation is a tombstone: a late ReserveStock for that saga is refused (ADR-0008).
CREATE TABLE reservation (
    saga_id    UUID        PRIMARY KEY,
    order_id   VARCHAR(64) NOT NULL,
    sku        VARCHAR(64) NOT NULL,
    -- 0 for a tombstone (nothing was ever reserved)
    quantity   INT         NOT NULL CHECK (quantity >= 0),
    -- RESERVED, RELEASED
    status     VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX reservation_order_idx ON reservation (order_id);

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

-- Demo catalogue, so the stack is usable straight after `make up`. A real system would load stock
-- through an API, not a migration.
INSERT INTO stock (sku, available, updated_at) VALUES
    ('SKU-1', 100, now()),
    ('SKU-2', 10, now()),
    ('SKU-EMPTY', 0, now());
