-- Order service schema for order_db.
-- Applied automatically on service startup.

CREATE TABLE IF NOT EXISTS orders (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL,
    restaurant_id   UUID NOT NULL,
    items           JSONB NOT NULL,        -- [{item_id, quantity, price}]
    total_amount    DECIMAL(10, 2) NOT NULL,
    status          VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    payment_simulate VARCHAR(20),          -- propagated from X-Payment-Simulate header
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_user ON orders (user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);

-- Transactional outbox: business row + outbox row in the same transaction.
CREATE TABLE IF NOT EXISTS outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic           VARCHAR(255) NOT NULL,
    key             VARCHAR(255),
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (created_at) WHERE published_at IS NULL;

-- Consumer idempotency: prevents duplicate processing of Kafka events.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        UUID PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
