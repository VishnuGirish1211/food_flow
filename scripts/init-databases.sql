-- This script runs once when the PostgreSQL container starts for the first time.
-- It creates the four databases required by the microservices.
-- Each service connects to its own database and never accesses another.

CREATE DATABASE auth_db;
CREATE DATABASE order_db;
CREATE DATABASE restaurant_db;
CREATE DATABASE payment_db;
