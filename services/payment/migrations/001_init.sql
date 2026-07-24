-- Payment service schema for payment_db.
-- Applied automatically on service startup.

CREATE TABLE IF NOT EXISTS payments (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID NOT NULL UNIQUE,
    amount      DECIMAL(10, 2) NOT NULL,
    status      VARCHAR(50) NOT NULL, -- PROCESSING, SUCCEEDED, FAILED
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Transactional outbox
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

-- Consumer idempotency
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        UUID PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
