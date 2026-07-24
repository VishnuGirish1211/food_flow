-- Restaurant service schema for restaurant_db.
-- Applied automatically on service startup.

CREATE TABLE IF NOT EXISTS restaurants (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255) NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS menu_items (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    restaurant_id UUID NOT NULL REFERENCES restaurants(id) ON DELETE CASCADE,
    name          VARCHAR(255) NOT NULL,
    description   TEXT,
    price         DECIMAL(10, 2) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_menu_items_restaurant
    ON menu_items (restaurant_id);

CREATE TABLE IF NOT EXISTS menu_item_stock (
    item_id       UUID PRIMARY KEY REFERENCES menu_items(id) ON DELETE CASCADE,
    quantity      INTEGER NOT NULL CHECK (quantity >= 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotency key for stock reservations
CREATE TABLE IF NOT EXISTS stock_reservations (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID NOT NULL UNIQUE, -- Crucial for consumer idempotency
    items       JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
