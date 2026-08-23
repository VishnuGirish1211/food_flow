-- Persist the payment_simulate directive alongside the payment.
--
-- The directive arrives on the stock.reserved event but was previously only
-- read in-memory during consumption. If the service crashed between the two
-- payment transactions, reconciliation had no way to recover it and fell back
-- to PAYMENT_SIMULATE_DEFAULT, silently resolving a "failure" order to
-- SUCCEEDED. Storing it in Transaction 1 makes the outcome recoverable.
ALTER TABLE payments ADD COLUMN IF NOT EXISTS payment_simulate VARCHAR(20);
