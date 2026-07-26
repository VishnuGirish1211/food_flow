-- Add idempotency key to orders table to prevent duplicate POST requests
ALTER TABLE orders ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255) UNIQUE;
