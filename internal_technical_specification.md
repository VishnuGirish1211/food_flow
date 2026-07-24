# Internal Technical Specification: Resto2 Repository

====================================================
SECTION 1
COMPLETE SYSTEM OVERVIEW
====================================================

The `resto2` repository implements an event-driven microservices architecture utilizing the Transactional Outbox pattern and Choreographed Sagas.

**Services & Databases:**
- **Auth Service (`auth-service:8001`)**: Connects to `postgres:5432` (database `auth_db`) and `redis:6379`.
- **Order Service (`order-service:8002`)**: Connects to `postgres:5432` (database `order_db`) and `kafka:9092`.
- **Restaurant Service (`restaurant-service:8003`)**: Connects to `postgres:5432` (database `restaurant_db`) and `kafka:9092`.
- **Payment Service (`payment-service:8004`)**: Connects to `postgres:5432` (database `payment_db`) and `kafka:9092`.

**Infrastructure:**
- **Nginx (`nginx:8085`)**: API Gateway proxying traffic to the Python services based on URI prefixes (`/auth`, `/orders`, `/restaurants`, `/payments`).
- **PostgreSQL (`postgres:5432`)**: Single PostgreSQL instance hosting 4 logically isolated databases.
- **Redis (`redis:6379`)**: Memory store exclusively used by the Auth Service for IP-based rate limiting.
- **Kafka (`kafka:9092`)**: Single-node Kraft cluster managing asynchronous inter-service event communication.
- **Prometheus**: NOT IMPLEMENTED IN THIS REPOSITORY. (The Python services expose a `/metrics` endpoint via the `prometheus_client` library, but the Prometheus server scraper is absent from `docker-compose.yml`).
- **Grafana**: NOT IMPLEMENTED IN THIS REPOSITORY.
- **Docker**: Containerizes all infrastructure and services via `docker-compose.yml`.

**Background Workers:**
- **Outbox Relays**: Order, Restaurant, and Payment services each run an internal `asyncio` loop polling their respective `outbox_events` tables and publishing to Kafka.
- **Kafka Consumers**: Order, Restaurant, and Payment services each run an internal `asyncio` loop consuming topics via `aiokafka`.

**Communication flow:**
- Client → Nginx (HTTP)
- Nginx → Services (HTTP)
- Services → PostgreSQL (asyncpg TCP) / Redis (redis.asyncio TCP)
- Services → Kafka (aiokafka TCP) → Services

====================================================
SECTION 2
FULL STARTUP SEQUENCE
====================================================

1. **Docker Daemon**: Interprets `docker-compose.yml`.
2. **Network Creation**: Creates default bridge network.
3. **Volume Mounts**: Provisions `postgres_data` volume and mounts `nginx/nginx.conf` and `scripts/init-databases.sql`.
4. **Environment Variables**: Injects container-specific environments (`DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `JWT_SECRET`, etc.).
5. **Infrastructure Containers Start**: `postgres`, `redis`, and `kafka` boot.
6. **PostgreSQL Initialization**: On first start, `postgres` executes `scripts/init-databases.sql`, creating `auth_db`, `order_db`, `restaurant_db`, `payment_db`.
7. **Healthchecks Loop**: Docker polls `pg_isready`, `redis-cli ping`, and `kafka-topics --list`.
8. **Microservice Containers Start**: Once healthchecks pass, `auth-service`, `order-service`, `restaurant-service`, `payment-service` boot.
9. **Container Entrypoints**: Executes `uvicorn main:app --host 0.0.0.0 --port 800X`.
10. **Python Interpreter**: Evaluates imports in `main.py` globally. Global singletons (like `structlog.get_logger()`) are instantiated.
11. **Event Loop**: Uvicorn starts the `asyncio` event loop.
12. **FastAPI Lifespan (Startup Phase)**:
    - **Connection Pools**: Calls `db.create_pool()` which opens TCP connections via `asyncpg.create_pool`. Registers JSONB codecs per connection.
    - **Migrations**: `_run_migrations()` reads `migrations/001_init.sql` and executes it.
    - **Kafka Producers**: Order/Restaurant/Payment call `shared.kafka.create_producer()`. `AIOKafkaProducer.start()` opens TCP connection to Kafka broker.
    - **Background Tasks**: Order/Restaurant/Payment call `asyncio.create_task(run_consumer(...))` and `asyncio.create_task(run_outbox_relay(...))`.
    - **Kafka Consumers**: Inside the consumer task, `shared.kafka.create_consumer()` opens TCP connection to Kafka broker.
13. **HTTP Server Bound**: Uvicorn begins accepting TCP connections on port 800X.
14. **Nginx Startup**: `nginx` container boots (delayed by `depends_on`), loads `nginx.conf`, and begins routing HTTP requests on port 8085.
15. System becomes idle waiting for HTTP traffic or Kafka events.

====================================================
SECTION 3
HTTP REQUEST LIFECYCLE
====================================================

Tracing `POST /orders` (Order Creation):

1. **TCP**: Client establishes TCP connection with `nginx:8085`.
2. **Nginx**: Matches `/orders/` path prefix. Injects `X-Real-IP` and proxies HTTP request to `order-service:8002/orders/`.
3. **FastAPI ASGI**: Uvicorn receives the request and passes it to the FastAPI ASGI app.
4. **Middleware (`shared/middleware.py`)**: `RequestIdMiddleware.dispatch()` executes.
   - Extracts `X-Request-ID` or generates a new `uuid4`. (Object created: String UUID).
   - Binds to `structlog` ContextVar.
   - Attaches to `request.state.request_id`.
   - Records `time.perf_counter()`.
   - `await call_next(request)` yields to router.
5. **Router (`services/order/routes.py`)**: FastAPI matches `POST /orders`.
6. **Dependencies**: Evaluates `Depends(get_current_user)`.
   - Extracts `Bearer <token>` from HTTP Header.
   - Decodes via `jwt.decode` (using PyJWT). (Object created: dict `{"user_id": ..., "email": ...}`).
7. **Pydantic**: Validates request body into `CreateOrderRequest` object.
8. **Business Logic**: Calculates `total_amount` by looping over items. Constructs `event_payload` dict. Calls `wrap_event("order.placed", ...)` which generates a new `event_id` UUID. (Object created: Event envelope dictionary).
9. **Database (`asyncpg`)**: 
   - `async with pool.acquire() as conn:`
   - `async with conn.transaction():` (BEGIN)
   - Executes `INSERT INTO orders` (status='PENDING').
   - Executes `INSERT INTO outbox_events` (topic='order.placed').
   - Exits block (COMMIT).
10. **Logger**: `logger.info("order_created", ...)` executes, writing structured JSON to stdout.
11. **Response Definition**: Returns a dictionary representing the accepted order.
12. **Middleware Continuation**: `RequestIdMiddleware` resumes. Calculates duration, sets `response.headers["X-Request-ID"]`, logs `request_completed`.
13. **TCP Response**: Uvicorn serializes response to JSON, Nginx proxies it back to the client.

====================================================
SECTION 4
EVERY DATABASE
====================================================

### 1. auth_db (Auth Service)
- **`users`**
  - **Columns**: `id` UUID PK, `email` VARCHAR(255) UNIQUE, `password_hash` VARCHAR(255), `created_at` TIMESTAMPTZ.
  - **Purpose**: Identity store.
  - **Lifecycle**: Inserted on `/register`. Updated: Never. Deleted: Never.
- **`refresh_tokens`**
  - **Columns**: `id` UUID PK, `user_id` UUID FK(users.id) CASCADE, `token_hash` VARCHAR(255), `expires_at` TIMESTAMPTZ, `created_at` TIMESTAMPTZ.
  - **Indexes**: `idx_refresh_tokens_user`, `idx_refresh_tokens_hash`.
  - **Purpose**: Persist hashed refresh tokens for session rotation.
  - **Lifecycle**: Inserted on `/login` and `/refresh`. Updated: Never. Deleted on `/refresh` (rotation or expiration).

### 2. order_db (Order Service)
- **`orders`**
  - **Columns**: `id` UUID PK, `user_id` UUID, `restaurant_id` UUID, `items` JSONB, `total_amount` DECIMAL(10, 2), `status` VARCHAR(50), `payment_simulate` VARCHAR(20), `created_at`, `updated_at`.
  - **Indexes**: `idx_orders_user`, `idx_orders_status`.
  - **Purpose**: System of record for orders.
  - **Lifecycle**: Inserted on `/orders` (PENDING). Updated by `consumers.py` to CONFIRMED, PAYMENT_FAILED, or STOCK_UNAVAILABLE. Deleted: Never.
- **`outbox_events`**
  - **Columns**: `id` UUID PK, `topic` VARCHAR(255), `key` VARCHAR(255), `payload` JSONB, `created_at`, `published_at` TIMESTAMPTZ (Nullable).
  - **Indexes**: `idx_outbox_unpublished` (partial index WHERE published_at IS NULL).
  - **Purpose**: Target for background Kafka relay.
  - **Lifecycle**: Inserted atomically with domain changes. Updated (`published_at` = NOW()) by outbox relay. Deleted: Never.
- **`processed_events`**
  - **Columns**: `event_id` UUID PK, `processed_at` TIMESTAMPTZ.
  - **Purpose**: Consumer exactly-once idempotency.
  - **Lifecycle**: Inserted inside consumer transaction. Updated: Never. Deleted: Never.

### 3. restaurant_db (Restaurant Service)
- **`restaurants`**
  - **Columns**: `id` UUID PK, `name` VARCHAR, `description` TEXT, `created_at`. (Inserted via `002_seed.sql`).
- **`menu_items`**
  - **Columns**: `id` UUID PK, `restaurant_id` FK(restaurants) CASCADE, `name`, `description`, `price` DECIMAL, `created_at`.
  - **Indexes**: `idx_menu_items_restaurant`.
- **`menu_item_stock`**
  - **Columns**: `item_id` PK FK(menu_items) CASCADE, `quantity` INTEGER (CHECK >= 0), `updated_at`.
  - **Purpose**: Tracks inventory.
  - **Lifecycle**: Updated heavily by consumers subtracting (reserve) or adding (release).
- **`stock_reservations`**
  - **Columns**: `id` UUID PK, `order_id` UUID UNIQUE, `items` JSONB, `created_at`.
  - **Purpose**: Compensation log for rolling back reservations.
  - **Lifecycle**: Inserted on `order.placed`. Deleted on `stock.release`.
- **`outbox_events`** & **`processed_events`**: Identical schema/purpose as order_db.

### 4. payment_db (Payment Service)
- **`payments`**
  - **Columns**: `id` UUID PK, `order_id` UUID UNIQUE, `amount` DECIMAL, `status` VARCHAR(50), `created_at`, `updated_at`.
  - **Purpose**: Financial transaction record.
  - **Lifecycle**: Inserted (PROCESSING) before gateway simulation. Updated (SUCCEEDED/FAILED) after.
- **`outbox_events`** & **`processed_events`**: Identical schema/purpose as order_db.

====================================================
SECTION 5
EVERY EVENT
====================================================

### Topic: `order.placed`
- **Producer**: Order Service (`routes.py` POST /orders)
- **Consumer**: Restaurant Service (`consumers.py`)
- **Schema/Payload**: JSON envelope containing dict `{"order_id", "user_id", "restaurant_id", "items", "total_amount", "payment_simulate"}`.
- **When produced**: HTTP request completes DB insertion.
- **Why produced**: Tell restaurant to reserve inventory.
- **Failure handling**: Rely on Outbox Relay for Producer crashes.
- **Duplicate handling**: Consumer `processed_events` idempotency check.

### Topic: `stock.reserved`
- **Producer**: Restaurant Service (`consumers.py`)
- **Consumer**: Payment Service (`consumers.py`)
- **Schema/Payload**: JSON envelope identical to `order.placed` payload.
- **When produced**: Inventory successfully deducted.
- **Why produced**: Tell payment service to charge the user.

### Topic: `stock.rejected`
- **Producer**: Restaurant Service (`consumers.py`)
- **Consumer**: Order Service (`consumers.py`)
- **Schema/Payload**: JSON envelope `{"order_id", "reason"}`.
- **When produced**: Inventory deduction fails (CHECK >= 0 constraint or qty logic).
- **Why produced**: Tell order service the saga failed due to stock.

### Topic: `payment.succeeded`
- **Producer**: Payment Service (`consumers.py`)
- **Consumer**: Order Service (`consumers.py`)
- **Schema/Payload**: JSON envelope `{"order_id"}`.
- **When produced**: Payment mock returns True.
- **Why produced**: Tell order service to confirm order.

### Topic: `payment.failed`
- **Producer**: Payment Service (`consumers.py`)
- **Consumer**: Order Service (`consumers.py`)
- **Schema/Payload**: JSON envelope `{"order_id"}`.
- **When produced**: Payment mock returns False.
- **Why produced**: Tell order service to fail order.

### Topic: `stock.release`
- **Producer**: Order Service (`consumers.py`)
- **Consumer**: Restaurant Service (`consumers.py`)
- **Schema/Payload**: JSON envelope `{"order_id"}`.
- **When produced**: When Order Service receives `payment.failed`.
- **Why produced**: Tell restaurant to rollback the inventory deduction.

====================================================
SECTION 6
EVERY SAGA
====================================================

**Saga: Order Creation**

**Branch 1: Success**
- Start: `order.placed`
- Restaurant deducts stock -> emits `stock.reserved`
- Payment simulates SUCCESS -> emits `payment.succeeded`
- Order transitions to `CONFIRMED`.

**Branch 2: Failure - Stock Unavailable**
- Start: `order.placed`
- Restaurant lacks stock -> emits `stock.rejected`
- Order transitions to `STOCK_UNAVAILABLE`. (Terminal).

**Branch 3: Failure - Payment Failed (Requires Compensation)**
- Start: `order.placed`
- Restaurant deducts stock -> emits `stock.reserved`
- Payment simulates FAILURE -> emits `payment.failed`
- Order transitions to `PAYMENT_FAILED`.
- **Compensation Trigger**: Order service emits `stock.release`.
- **Rollback**: Restaurant service consumes `stock.release`, adds stock back to `menu_item_stock`, deletes `stock_reservations` row.

**Timeout / Crash Recovery**: 
- Orchestration happens purely via Choreography. No timeout manager exists in this repo. If a service crashes, Kafka messages queue up. When the service reboots, it reads from its offset, resuming the saga automatically.

====================================================
SECTION 7
SERVICE SPECIFICATIONS
====================================================

### Order Service
- **Purpose**: Manage order state machine. Saga initiator.
- **Startup**: Connects DB, runs migrations, connects Kafka Producer/Consumer, starts Outbox background relay task, starts Consumer background task.
- **Background workers**: Outbox relay loop, Consumer loop.
- **API routes**: `POST /orders`, `GET /orders/{id}`.
- **Database usage**: `orders`, `outbox_events`, `processed_events`.
- **Transactions**: Route uses atomic insert. Consumer uses atomic update/insert + `FOR UPDATE` row lock on `orders` row.
- **Outbox relay**: Standard implementation polling `outbox_events`.
- **Shutdown**: Sets `shutdown_event`, `asyncio.gather` waits for tasks, stops producer/consumer, closes DB pool.

### Restaurant Service
- **Purpose**: Inventory and menu management. Saga participant.
- **Startup / Shutdown**: Identical pattern to Order Service.
- **Background workers**: Outbox relay, Consumer loop.
- **API routes**: `GET /restaurants`, `GET /restaurants/{id}/menu`.
- **Database usage**: `restaurants`, `menu_items`, `menu_item_stock`, `stock_reservations`.
- **Transactions**: Consumer uses `FOR UPDATE` on `menu_item_stock` rows.

### Payment Service
- **Purpose**: Financial settlement simulator. Saga participant.
- **Startup / Shutdown**: Identical pattern to Order Service.
- **Background workers**: Outbox relay, Consumer loop.
- **API routes**: `GET /payments/order/{id}`.
- **Database usage**: `payments`.
- **Transactions**: Two-transaction implementation. Tx1 records `PROCESSING` intent and idempotency. Tx2 records `SUCCEEDED`/`FAILED` and outbox event after the mock gateway call returns.

### Auth Service
- **Purpose**: User identity and edge rate limiting.
- **Startup / Shutdown**: Connects PostgreSQL and Redis. Closes on exit. (No Kafka).
- **Background workers**: None.
- **API routes**: `POST /register`, `POST /login`, `POST /refresh`.
- **Database usage**: `users`, `refresh_tokens`.
- **Transactions**: Route uses atomic insert/delete.
- **Redis usage**: Fails-open if unavailable. Used in `RateLimitMiddleware`.

### Inventory, Monitoring, etc.
- NOT IMPLEMENTED IN THIS REPOSITORY as dedicated services. Inventory is handled by Restaurant Service. Monitoring relies on the `/metrics` endpoint.

====================================================
SECTION 8
CONSUMER FLOWS
====================================================

Tracing `services/restaurant/consumers.py`:

1. **Receive**: `await consumer.getmany(timeout_ms=1000)` yields a batch of `msg`.
2. **Deserialize**: `msg.value` was already deserialized to JSON dict by `aiokafka`'s `value_deserializer=lambda v: json.loads(v.decode("utf-8"))`.
3. **Idempotency**: 
   - `async with conn.transaction():` (BEGIN)
   - `SELECT 1 FROM processed_events WHERE event_id = $1`
   - If true, return (exiting transaction block).
4. **Transaction (Business Logic)**:
   - Evaluates `if event_type == "order.placed":`
   - Calls `reserve_stock()`. Loops over items.
   - `SELECT quantity FROM menu_item_stock WHERE item_id = $1 FOR UPDATE`
   - `UPDATE menu_item_stock SET quantity = quantity - $1`
   - `INSERT INTO stock_reservations`
5. **Outbox**:
   - `INSERT INTO outbox_events (..., topic='stock.reserved', ...)`
   - `INSERT INTO processed_events`
6. **Commit**: Exits `async with conn.transaction():`, PostgreSQL implicitly issues COMMIT.
7. **Kafka ACK**:
   - Outermost loop finishes iterating over `msg` batch.
   - `await consumer.commit()` commits the offset to Kafka.
8. **Next service**: Outbox relay (running in parallel) polls DB and pushes to Kafka.

====================================================
SECTION 9
OUTBOX PATTERN
====================================================

Implementation found in `shared/outbox.py`.

- **Polling**: `while not shutdown_event.is_set():` infinite loop.
- **Transactions**: `async with conn.transaction():` is created *per event*.
- **FOR UPDATE SKIP LOCKED**: 
  - `SELECT id, topic, key, payload FROM outbox_events WHERE published_at IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED`.
  - This locks exactly one row. If another instance of the relay is running, it skips the locked row, allowing highly concurrent worker scaling without deadlocks or double-publishing.
- **Publishing**: `await producer.send_and_wait(...)`. Blocks until Kafka acknowledges.
- **Mark Processed**: `UPDATE outbox_events SET published_at = NOW() WHERE id = $1`.
- **Crash Recovery**: If the service crashes after `send_and_wait` but before the `UPDATE` commits, the transaction rolls back. The row's `published_at` remains NULL. Upon restart, it will be published again.
- **Duplicate Publication**: Guaranteed to happen in the crash recovery scenario. Downstream consumers mitigate this via `processed_events` checking.
- **Retries**: If `send_and_wait` raises an exception, the `try...except` catches it, the transaction rolls back, it sleeps for `poll_interval=1.0s`, and retries.
- **Ordering**: Enforced locally by `ORDER BY created_at`. However, concurrent relays using `SKIP LOCKED` will process events out-of-order. This project trades strict global ordering for high throughput concurrency.
- **Worker Lifecycle**: Task created in FastAPI `lifespan`. Shut down via `shutdown_event.set()`, followed by one final loop flush.

====================================================
SECTION 10
FAILURE ANALYSIS
====================================================

1. **Database crash**: Connection pool raises exception. Requests fail (HTTP 500). Outbox relay logs `outbox_relay_error` and retries infinitely. Consumer logs `consumer_error` and retries infinitely. Nothing is lost.
2. **Kafka crash**: Producer `send_and_wait` fails, rolling back DB outbox transaction. Events queue in `outbox_events` table (survives). Consumer cannot fetch (retries). Nothing is lost.
3. **Producer crash**: If Outbox relay crashes mid-publish, DB transaction rolls back. Event remains in `outbox_events`. Replayed on restart.
4. **Consumer crash after DB commit before offset commit**: Upon restart, consumer refetches message. Checks `processed_events`. Finds match. Skips business logic. Commits offset. No data lost.
5. **Consumer crash before DB commit**: PostgreSQL transaction rolls back. Message refetched on restart and processed as if new. No data lost.
6. **Service restart**: Caught by `shutdown_event`. Consumer finishes current batch, outbox executes final flush, pools close safely.
7. **Redis unavailable**: `auth/rate_limiter.py` catches connection error. Logs warning. `return await call_next(request)`. Fails-open. Rate limiting is lost; authentication services survive and continue functioning.
8. **Lost ACK**: (Kafka broker receives message but ACK is dropped). Outbox relay throws exception. DB rolls back. Relay retries and sends a duplicate. Downstream handles via idempotency.
9. **Transaction rollback**: If a consumer encounters a unique constraint violation or an explicit `raise`, the DB throws away changes.
10. **Power loss (SIGKILL)**: No graceful shutdown. Exact same recovery mechanics as #4 and #5. ACID compliance of PostgreSQL guarantees data safety.
11. **Deadlock during consumer transaction**: *CRITICAL IMPLEMENTATION FLAW IN THIS REPOSITORY.* If PostgreSQL aborts a transaction due to a deadlock, the code in `consumers.py` wraps `_handle_message` in a `try...except Exception`. The exception is logged, but the loop continues. At the end of the batch, `await consumer.commit()` is executed. **The message is permanently lost.**

====================================================
SECTION 11
CONCURRENCY
====================================================

- **Connection pools**: Managed by `asyncpg.create_pool(min_size=2, max_size=10)`. Restricts concurrent DB queries to 10 per service instance.
- **Locks (FOR UPDATE)**:
  - `services/restaurant/stock.py`: `SELECT quantity FROM menu_item_stock WHERE item_id = $1 FOR UPDATE`. Locks the specific row. If two concurrent orders request the same item, the second order blocks until the first order's transaction commits, ensuring exact inventory consistency without race conditions.
  - `services/order/state_machine.py`: `SELECT status FROM orders WHERE id = $1 FOR UPDATE`. Prevents concurrent saga messages (e.g. `payment.failed` and `stock.rejected` arriving simultaneously) from causing a race condition in order state.
- **Multiple Relays**: Safe due to `FOR UPDATE SKIP LOCKED`.
- **Multiple Consumers**: Safe due to Kafka Consumer Group partitions (only one consumer per partition) AND `processed_events` idempotency table preventing race conditions if Kafka delivers duplicates.

====================================================
SECTION 12
SEQUENCE DIAGRAMS
====================================================

**Workflow: Placing an Order (Happy Path)**

```text
Client          Gateway         Order DB        Kafka        Restaurant DB      Payment DB
  │               │               │               │               │               │
  ├─POST /orders─>│               │               │               │               │
  │               ├─Proxy /orders>│               │               │               │
  │               │               ├─BEGIN         │               │               │
  │               │               ├─INSERT Order  │               │               │
  │               │               ├─INSERT Outbox │               │               │
  │               │               ├─COMMIT        │               │               │
  │               │<──HTTP 202────┤               │               │               │
  │<──HTTP 202────┤               │               │               │               │
  │               │               │               │               │               │
  │               │               ├─Relay Selects>│               │               │
  │               │               │               ├─order.placed─>│               │
  │               │               │               │               ├─BEGIN         │
  │               │               │               │               ├─UPDATE Stock  │
  │               │               │               │               ├─INSERT Outbox │
  │               │               │               │               ├─COMMIT        │
  │               │               │               │               │               │
  │               │               │               │<─Relay Selects┤               │
  │               │               │               │               ├stock.reserved>│
  │               │               │               │               │               ├─BEGIN (Tx1)
  │               │               │               │               │               ├─INSERT Pymt
  │               │               │               │               │               ├─COMMIT (Tx1)
  │               │               │               │               │               ├─Gateway Sim
  │               │               │               │               │               ├─BEGIN (Tx2)
  │               │               │               │               │               ├─UPDATE Pymt
  │               │               │               │               │               ├─INSERT Outbox
  │               │               │               │               │               ├─COMMIT (Tx2)
  │               │               │               │               │               │
  │               │               │               │<───────Relay Selects──────────┤
  │               │               │               │               │               │
  │               │               │<pymt.succeeded┤               │               │
  │               │               ├─BEGIN         │               │               │
  │               │               ├─UPDATE Order  │               │               │
  │               │               ├─COMMIT        │               │               │
```

====================================================
SECTION 13
CALL TREES
====================================================

**Execution Tree: `POST /orders`**

```text
uvicorn.run()
 └── fastapi.FastAPI.__call__()
      └── RequestIdMiddleware.dispatch()
           ├── uuid.uuid4()
           ├── structlog.contextvars.bind_contextvars()
           ├── time.perf_counter()
           └── call_next()
                └── APIRouter.__call__()
                     ├── get_current_user() (Dependency)
                     │    ├── Header()
                     │    ├── authorization.split()
                     │    └── jwt.decode()
                     ├── CreateOrderRequest.validate() (Pydantic)
                     └── create_order()
                          ├── uuid.uuid4()
                          ├── sum(item.quantity * item.price)
                          ├── wrap_event()
                          │    ├── uuid.uuid4()
                          │    └── datetime.now()
                          ├── pool.acquire()
                          │    └── conn.transaction()
                          │         ├── conn.execute(INSERT orders)
                          │         └── conn.execute(INSERT outbox_events)
                          └── structlog.get_logger().info()
```

====================================================
SECTION 14
OBJECT LIFECYCLES
====================================================

- **HTTP Request (FastAPI/Starlette)**: Created by Uvicorn on TCP ingest. Modified by `RequestIdMiddleware` (`request.state`). Destroyed when route returns response.
- **Kafka Message (AIOKafka)**: Created by `consumer.getmany()`. Passed to `_handle_message`. Destroyed when loop iteration completes (GC'd).
- **Order (Database Row)**: Created in `POST /orders` as `PENDING`. Modified by Order Consumer (`transition_order` to `CONFIRMED` etc). Destroyed: Never.
- **Reservation (Database Row `stock_reservations`)**: Created by Restaurant Consumer (`reserve_stock`). Modified: Never. Destroyed by Restaurant Consumer (`release_stock`) during compensation.
- **Payment (Database Row)**: Created by Payment Consumer Tx1 (`PROCESSING`). Modified by Payment Consumer Tx2 (`SUCCEEDED`/`FAILED`). Destroyed: Never.
- **Outbox Event (Database Row)**: Created in business logic routes/consumers. Modified by Outbox Relay (`published_at` = NOW). Destroyed: Never.
- **Processed Event (Database Row)**: Created in consumer transactions. Modified: Never. Destroyed: Never.
- **Connection (asyncpg)**: Created by `db.create_pool()` at startup. Modified (state changes) when acquired by `pool.acquire()`. Destroyed at application shutdown.
- **Transaction (asyncpg)**: Created by `conn.transaction()`. Committed upon exiting the `async with` block. Destroyed immediately after.
- **Logger Context (structlog ContextVar)**: Created/Cleared at start of `RequestIdMiddleware`. Bound with `request_id`. Destroyed/overwritten on next request in the same thread/async context.

====================================================
SECTION 15
EVERY IMPLEMENTED SAFETY MECHANISM
====================================================

- **Idempotency**: Implemented via `processed_events` table. The `event_id` from the Kafka envelope is queried (`SELECT 1`) at the start of every consumer transaction. At the end of the transaction, the `event_id` is INSERTed. If a duplicate arrives, the initial SELECT finds it and safely ignores the message.
- **Transactions**: `asyncpg` context managers (`async with conn.transaction():`) wrap all database queries. If a Python exception is raised inside the block, PostgreSQL executes a `ROLLBACK`.
- **Retries**: Implemented inside `shared/outbox.py` via an infinite `while` loop that sleeps and retries on failure.
- **Connection pooling**: Implemented in `db.py` via `asyncpg.create_pool` with `min_size=2` and `max_size=10`.
- **Timeouts**: None implemented in business logic. (Consumer batch fetch has `timeout_ms=1000`).
- **Compensation**: Implemented natively in the Choreographed Saga. If `payment.failed` is received by the Order service, it publishes a `stock.release` event. The Restaurant service consumes this and restores the stock quantities using the `stock_reservations` data.
- **Unique constraints**: `payments(order_id)` and `stock_reservations(order_id)` enforce strict 1:1 business rules at the database level.
- **Locks**: Database-level pessimistic locking is implemented in `stock.py` via `SELECT ... FOR UPDATE`, strictly linearizing concurrent stock modifications.
- **Health checks**: `shared/health.py` executes a raw `SELECT 1` against the database pool to determine readiness for the Docker engine.
- **Metrics**: `shared/metrics.py` exposes Prometheus counters for `http_requests_total`, `kafka_messages_consumed_total`, and `outbox_events_published_total`.
- **Logging**: `structlog` binds a globally generated UUID to all log lines executed during an HTTP request cycle, emitting machine-readable JSON.

====================================================
SECTION 16
KNOWN LIMITATIONS
====================================================

1. **Permanent Message Loss on Consumer Exception**: In `consumers.py` across all services, the `try...except Exception` block inside the loop catches failures (like DB deadlocks or logic bugs) and logs them, but the outer loop continues and unconditionally executes `await consumer.commit()`. The failed message is acknowledged to Kafka and lost forever.
2. **Missing Outbox Cleanup**: The `outbox_events` table retains all events permanently. There is no background job implemented to prune `published_at IS NOT NULL` events, meaning the table will grow infinitely.
3. **Missing Idempotency Cleanup**: The `processed_events` table grows infinitely.
4. **No Saga Orchestration/Timeout**: If the Payment service never responds, the Order stays stuck in `PENDING` forever. There is no timeout or dead-letter queue mechanism implemented.
5. **Rate Limiting Fail-Open Risk**: The Redis rate limiter completely disables itself if Redis is down, meaning the service is vulnerable to brute force/DDoS during Redis outages.
6. **No Observability Tooling Provisioned**: While metrics are exposed, `Prometheus` and `Grafana` are completely missing from the `docker-compose.yml`.
7. **Single Point of Failure**: Kafka is deployed as a single node in `docker-compose.yml`.
8. **Inexact Outbox Ordering**: The outbox relay uses `SKIP LOCKED`. If two relay instances run, Event B could be published before Event A, breaking causal ordering guarantees.
9. **No Price Re-validation**: The Restaurant service's `reserve_stock` deducts inventory but does not cross-check the `price` in the payload against the actual price in `menu_items`, meaning users could inject fraudulent prices during `POST /orders`.
