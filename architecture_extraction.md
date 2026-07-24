# Kafka, Saga, and Outbox Architecture Extraction

========================================
PART 1 - PROJECT MAP
========================================

**Kafka & Event Configurations:**
- File: `docker-compose.yml`
  - Purpose: Provisions Kafka Kraft broker and sets replication factors.
  - Used by: Docker Daemon

**Shared Infrastructure:**
- File: `shared/events.py`
  - Purpose: Generates standardized event envelopes (`event_id`, `occurred_at`).
  - Imports: `uuid`, `datetime.timezone`
  - Used by: `services/order/routes.py`, `services/order/consumers.py`, `services/restaurant/consumers.py`, `services/payment/consumers.py`
- File: `shared/kafka.py`
  - Purpose: Instantiates `AIOKafkaProducer` and `AIOKafkaConsumer`.
  - Imports: `json`, `os`, `aiokafka.AIOKafkaConsumer`, `aiokafka.AIOKafkaProducer`
  - Used by: `main.py` and `consumers.py` in Order, Restaurant, and Payment services.
- File: `shared/outbox.py`
  - Purpose: Background worker that polls the database and publishes to Kafka.
  - Imports: `asyncio`, `structlog`, `aiokafka.AIOKafkaProducer`, `asyncpg.Pool`, `shared.metrics.outbox_events_published_total`
  - Used by: `main.py` in Order, Restaurant, and Payment services.

**Order Service:**
- File: `services/order/main.py`
  - Purpose: Lifespan management, starts Outbox worker and Consumer worker.
- File: `services/order/routes.py`
  - Purpose: Handles HTTP requests, opens DB transaction, inserts domain object and Outbox event.
- File: `services/order/consumers.py`
  - Purpose: Consumes events, manages idempotency, executes Saga compensations, publishes events.
- File: `services/order/migrations/001_init.sql`
  - Purpose: Defines `outbox_events` and `processed_events` tables.

**Restaurant Service:**
- File: `services/restaurant/main.py`
  - Purpose: Lifespan management.
- File: `services/restaurant/consumers.py`
  - Purpose: Consumes events, manages idempotency, executes reservations.
- File: `services/restaurant/stock.py`
  - Purpose: Executes `SELECT ... FOR UPDATE` DB locks to prevent concurrent reservation conflicts.

**Payment Service:**
- File: `services/payment/main.py`
  - Purpose: Lifespan management.
- File: `services/payment/consumers.py`
  - Purpose: Consumes events, executes the two-transaction payment process.

========================================
PART 2 - COMPLETE EVENT FLOW
========================================

**Flow: Order Created → Restaurant → Payment → Order Confirmation**

1. `HTTP Request` (POST /orders)
↓
2. `FastAPI Route` (`create_order` in `services/order/routes.py`)
↓
3. `Database Transaction` (`async with conn.transaction():`)
↓
4. `SQL Statements` (INSERT INTO orders, INSERT INTO outbox_events)
↓
5. `Commit`
↓
6. `Outbox Publisher` (`run_outbox_relay` loop in `shared/outbox.py`)
↓
7. `Kafka Producer` (`producer.send_and_wait` in `shared/outbox.py`)
↓
8. `Kafka Topic` (`order.placed`)
↓
9. `Kafka Broker`
↓
10. `Consumer` (`run_consumer` in `services/restaurant/consumers.py`)
↓
11. `Business Logic` (`reserve_stock` in `services/restaurant/stock.py`)
↓
12. `Database Transaction` (`async with conn.transaction():` in `_handle_message`)
↓
13. `SQL Statements` (SELECT FOR UPDATE on `menu_item_stock`, UPDATE `menu_item_stock`, INSERT `stock_reservations`, INSERT `outbox_events` (`stock.reserved`), INSERT `processed_events`)
↓
14. `Commit`
↓
15. `Outbox Publisher` (`shared/outbox.py` in Restaurant Service)
↓
16. `Kafka Producer` -> `Kafka Topic` (`stock.reserved`) -> `Kafka Broker`
↓
17. `Consumer` (`run_consumer` in `services/payment/consumers.py`)
↓
18. `Database Transaction 1` (INSERT `payments` (PROCESSING), INSERT `processed_events`) -> `Commit`
↓
19. `Business Logic` (`process_payment` in `gateway.py`)
↓
20. `Database Transaction 2` (UPDATE `payments` (SUCCEEDED), INSERT `outbox_events` (`payment.succeeded`)) -> `Commit`
↓
21. `Outbox Publisher` (`shared/outbox.py` in Payment Service)
↓
22. `Kafka Producer` -> `Kafka Topic` (`payment.succeeded`) -> `Kafka Broker`
↓
23. `Consumer` (`run_consumer` in `services/order/consumers.py`)
↓
24. `Database Transaction` (SELECT FOR UPDATE `orders`, UPDATE `orders` (CONFIRMED), INSERT `processed_events`)
↓
25. `Commit` (Flow finishes).

========================================
PART 3 - STARTUP FLOW
========================================

Starting from `docker compose up`:
1. **Container Startup:** `postgres`, `redis`, `kafka` containers start first.
2. **Database Init:** `postgres` executes `scripts/init-databases.sql`.
3. **Kafka Port:** `kafka` opens TCP port 9092.
4. **Service Containers:** `order-service`, `restaurant-service`, `payment-service` start.
5. **Python Process:** `uvicorn main:app` executes.
6. **First File Executed:** `main.py` in each service.
7. **Database Connections:** Inside the FastAPI `lifespan` context manager, `await db.create_pool()` executes, opening TCP connections to PostgreSQL and running migrations.
8. **Kafka Producer TCP:** Inside `lifespan`, `await create_producer()` executes, opening TCP connections to Kafka.
9. **Background Workers Start:** `asyncio.create_task(run_consumer(...))` and `asyncio.create_task(run_outbox_relay(...))` are called in `lifespan`.
10. **Kafka Consumer TCP:** Inside the `run_consumer` background task, `await create_consumer()` opens TCP connections to Kafka.
11. **HTTP Server:** Uvicorn finishes startup and listens on the exposed port.

========================================
PART 4 - PRODUCERS
========================================

**1. Topic:** `order.placed`
- Function: `create_order`
- File: `services/order/routes.py`
- Message schema: Envelope containing `order_id`, `user_id`, `restaurant_id`, `items`, `total_amount`, `payment_simulate`
- Key used: `order_id`
- Headers: None
- When it is called: On HTTP POST `/orders`
- Inside transaction: Yes
- How failures are handled: Transaction rolls back on DB failure. Outbox background relay handles Kafka failures.

**2. Topic:** `stock.release` (Compensation)
- Function: `_handle_message`
- File: `services/order/consumers.py`
- Message schema: Envelope containing `order_id`
- Key used: `order_id`
- Headers: None
- When it is called: On consumption of `payment.failed`
- Inside transaction: Yes
- How failures are handled: Outbox background relay handles Kafka failures.

**3. Topic:** `stock.reserved`
- Function: `_handle_message`
- File: `services/restaurant/consumers.py`
- Message schema: Envelope identical to `order.placed` payload
- Key used: `order_id`
- Headers: None
- When it is called: On consumption of `order.placed` and `reserve_stock` succeeds
- Inside transaction: Yes
- How failures are handled: Outbox background relay.

**4. Topic:** `stock.rejected`
- Function: `_handle_message`
- File: `services/restaurant/consumers.py`
- Message schema: Envelope containing `order_id`, `reason`
- Key used: `order_id`
- Headers: None
- When it is called: On consumption of `order.placed` and `reserve_stock` throws `InsufficientStockError`
- Inside transaction: Yes
- How failures are handled: Outbox background relay.

**5. Topic:** `payment.succeeded`
- Function: `_handle_message`
- File: `services/payment/consumers.py`
- Message schema: Envelope containing `order_id`
- Key used: `order_id`
- Headers: None
- When it is called: On consumption of `stock.reserved` and gateway returns True
- Inside transaction: Yes (Transaction 2)
- How failures are handled: Outbox background relay.

**6. Topic:** `payment.failed`
- Function: `_handle_message`
- File: `services/payment/consumers.py`
- Message schema: Envelope containing `order_id`
- Key used: `order_id`
- Headers: None
- When it is called: On consumption of `stock.reserved` and gateway returns False
- Inside transaction: Yes (Transaction 2)
- How failures are handled: Outbox background relay.

========================================
PART 5 - CONSUMERS
========================================

**1. Order Service Consumer**
- Topic: `payment.succeeded`, `payment.failed`, `stock.rejected`
- Consumer group: `order-service`
- Handler function: `_handle_message` in `services/order/consumers.py`
- Business action: Transitions order status via state machine.
- Database writes: `UPDATE orders`, `INSERT INTO processed_events`, `INSERT INTO outbox_events` (for `stock.release`).
- Events published afterwards: `stock.release` (only if `payment.failed`).

**2. Restaurant Service Consumer**
- Topic: `order.placed`, `stock.release`
- Consumer group: `restaurant-service`
- Handler function: `_handle_message` in `services/restaurant/consumers.py`
- Business action: Reserves stock (deducts menu quantities) or releases stock (adds back).
- Database writes: `UPDATE menu_item_stock`, `INSERT INTO stock_reservations` / `DELETE FROM stock_reservations`, `INSERT INTO processed_events`, `INSERT INTO outbox_events`.
- Events published afterwards: `stock.reserved`, `stock.rejected`.

**3. Payment Service Consumer**
- Topic: `stock.reserved`
- Consumer group: `payment-service`
- Handler function: `_handle_message` in `services/payment/consumers.py`
- Business action: Executes mock payment gateway.
- Database writes: Tx1: `INSERT INTO payments`, `INSERT INTO processed_events`. Tx2: `UPDATE payments`, `INSERT INTO outbox_events`.
- Events published afterwards: `payment.succeeded`, `payment.failed`.

========================================
PART 6 - TRANSACTIONAL OUTBOX
========================================

- **Outbox table schema:** `id` (UUID), `topic` (VARCHAR), `key` (VARCHAR), `payload` (JSONB), `created_at` (TIMESTAMPTZ), `published_at` (TIMESTAMPTZ).
- **Insert location:** Inside `async with conn.transaction():` blocks within HTTP routes and Kafka consumer handlers.
- **Publisher implementation:** `run_outbox_relay` in `shared/outbox.py`.
- **Polling mechanism:** `while not shutdown_event.is_set():` infinite loop leveraging `asyncio.wait_for`.
- **Batch size:** `LIMIT 1` via `SELECT ... FOR UPDATE SKIP LOCKED`.
- **Retry mechanism:** If `producer.send_and_wait` fails, the `async with conn.transaction()` rolls back. `published_at` is never updated. The `while True` loop breaks, logs the error, sleeps for `poll_interval`, and tries fetching the row again.
- **Delete/mark processed logic:** `UPDATE outbox_events SET published_at = NOW() WHERE id = $1`. Keeps events in DB.
- **Failure handling:** Wraps the entire polling function in a `try...except Exception` block, logging the exception and falling back to `asyncio.sleep(poll_interval)`.

========================================
PART 7 - DATABASE TRANSACTIONS
========================================

**1. Order Creation (`services/order/routes.py`)**
- BEGIN: `async with conn.transaction():`
- Queries:
  - INSERT `orders`
  - INSERT `outbox_events` (topic='order.placed')
- COMMIT
- Rollback conditions: Unhandled database exceptions.

**2. Order Consumer (`services/order/consumers.py`)**
- BEGIN: `async with conn.transaction():`
- Queries:
  - SELECT FROM `processed_events`
  - SELECT FOR UPDATE: `SELECT status FROM orders WHERE id = $1 FOR UPDATE` (inside `transition_order`)
  - UPDATE `orders`
  - INSERT `outbox_events` (topic='stock.release', only on payment failure)
  - INSERT `processed_events`
- COMMIT
- Rollback conditions: If event already processed, if state transition is invalid, or general DB error.

**3. Restaurant Consumer - Reservation (`services/restaurant/consumers.py`)**
- BEGIN: `async with conn.transaction():`
- Queries:
  - SELECT FROM `processed_events`
  - SELECT FOR UPDATE: `SELECT quantity FROM menu_item_stock WHERE item_id = $1 FOR UPDATE` (inside `reserve_stock`, runs in loop per item)
  - UPDATE `menu_item_stock` (in loop)
  - INSERT `stock_reservations`
  - INSERT `outbox_events` (`stock.reserved` or `stock.rejected`)
  - INSERT `processed_events`
- COMMIT
- Rollback conditions: DB Error. (Note: `InsufficientStockError` is caught *inside* the transaction block, leading to an alternate INSERT and COMMIT, not a rollback).

**4. Payment Consumer - Tx 1 (`services/payment/consumers.py`)**
- BEGIN: `async with conn.transaction():`
- Queries:
  - SELECT FROM `processed_events`
  - INSERT `payments` (status=PROCESSING)
  - INSERT `processed_events`
- COMMIT
- Rollback conditions: UniqueViolationError (duplicate payment).

**5. Payment Consumer - Tx 2 (`services/payment/consumers.py`)**
- BEGIN: `async with conn.transaction():`
- Queries:
  - UPDATE `payments`
  - INSERT `outbox_events`
- COMMIT
- Rollback conditions: General DB Error.

========================================
PART 8 - IDEMPOTENCY
========================================

- **Tables:** `processed_events` (`event_id` UUID PRIMARY KEY, `processed_at` TIMESTAMPTZ).
- **Unique constraints:** `payments.order_id` (Payment DB), `stock_reservations.order_id` (Restaurant DB).
- **Event IDs:** `wrap_event()` assigns a v4 UUID `event_id` to every event at creation.
- **Logic:** 
  ```sql
  SELECT 1 FROM processed_events WHERE event_id = $1
  ```
  If found, logs `duplicate_event_skipped` and returns early. The offset is still committed because the outer try-except loop completes successfully. At the end of the transaction, `INSERT INTO processed_events` stores the ID.
- **Retry behavior:** If the consumer crashes before DB commit, no ID is saved, offset is not committed. Message re-consumed. DB transaction guarantees atomicity. If it crashes after DB commit but before offset commit, consumer re-reads message, sees `event_id` in `processed_events`, skips logic, then commits offset.

========================================
PART 9 - KAFKA CONFIGURATION
========================================

- **Topics:** Automatically created.
- **Partitions:** Default (1).
- **Replication factor:** 1 (configured via `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`).
- **Consumer groups:** Explicitly defined (`order-service`, `restaurant-service`, `payment-service`).
- **Producer settings:** Serializes values using `json.dumps().encode("utf-8")`.
- **Acknowledgement mode:** Producer defaults (acks=1). Consumer is explicitly manual (`enable_auto_commit=False`).
- **Retries:** Producer defaults.
- **Batch settings:** Consumer fetches via `getmany(timeout_ms=1000)`.
- **Offset commit strategy:** Manual commit (`await consumer.commit()`) is invoked only after the entire batch returned by `getmany` has been iterated over.

========================================
PART 10 - FAILURE SCENARIOS
========================================

1. **Producer crashes before commit:** (Applies to outbox relay). Database transaction rolls back. `published_at` remains NULL. The message is completely safe in the DB and will be published on the next restart.
2. **Producer crashes after commit before publish:** The relay issues `producer.send_and_wait`. If the relay crashes immediately *after* this succeeds but *before* the DB UPDATE commits, the DB rolls back `published_at` to NULL. On restart, the relay will resend the exact same event. The duplicate is successfully absorbed by downstream consumers via the `processed_events` idempotency check.
3. **Kafka unavailable:** During relay: `send_and_wait` raises an Exception, DB rolls back, worker logs error and sleeps 1 second. During consume: `getmany()` fails, offset isn't committed, loop retries.
4. **Consumer crashes after DB commit before offset commit:** Upon restart, the consumer fetches the same message. It enters `_handle_message`, queries `SELECT 1 FROM processed_events`, finds the row, skips processing, returns successfully, and commits the offset.
5. **Consumer crashes before DB commit:** Database transaction rolls back. State remains identical to before the event arrived. Message is refetched and processed normally.
6. **Duplicate Kafka message arrives:** Processed exactly as Scenario 4. The idempotency `SELECT` catches it.
7. **Payment Service offline for 30 minutes:** Order stays in `PENDING` state. Restaurant successfully reserves stock. `stock.reserved` events queue up in Kafka. When Payment Service boots, it consumes the backlog, and the saga completes.
8. **Restaurant Service offline:** `order.placed` events queue in Kafka. Order stays `PENDING`.
9. **Deadlock during consumer transaction:** PostgreSQL aborts the transaction. `_handle_message` throws an exception. `run_consumer` catches it, logs `consumer_error`, but **crucially**, the code logic continues and executes `await consumer.commit()` if `result` was present. **Architectural flaw**: The offset is committed despite the error, causing the message to be permanently lost.
10. **Order Service crashes after payment success:** Same behavior as Scenario 4 & 5. If it crashes before DB commit, it rolls back and retries. If it crashes after DB commit, it idempotently ignores on retry and commits offset.

========================================
PART 11 - CALL GRAPH
========================================

```text
main.py
 ↓
lifespan()
 ├── db.create_pool()
 │    ├── _init_connection()
 │    └── _run_migrations()
 ├── create_producer()
 ├── asyncio.create_task(run_consumer())
 │    ├── create_consumer()
 │    └── while not shutdown_event.is_set():
 │         ├── consumer.getmany()
 │         ├── _handle_message()
 │         │    ├── SELECT processed_events
 │         │    ├── Business Logic (e.g. reserve_stock)
 │         │    └── INSERT processed_events
 │         └── consumer.commit()
 └── asyncio.create_task(run_outbox_relay())
      └── while not shutdown_event.is_set():
           └── _poll_and_publish()
                ├── SELECT ... FOR UPDATE SKIP LOCKED
                ├── producer.send_and_wait()
                └── UPDATE outbox_events
```

========================================
PART 12 - SOURCE CODE
========================================

**shared/events.py**
- `def wrap_event(event_type: str, payload: dict) -> dict:` (Line 19)

**shared/kafka.py**
- `async def create_producer() -> AIOKafkaProducer:` (Line 15)
- `async def create_consumer(topics: list[str], group_id: str) -> AIOKafkaConsumer:` (Line 29)

**shared/outbox.py**
- `async def run_outbox_relay(db_pool: Pool, producer: AIOKafkaProducer, shutdown_event: asyncio.Event, poll_interval: float = 1.0):` (Line 30)
- `async def _poll_and_publish(db_pool: Pool, producer: AIOKafkaProducer) -> int:` (Line 68)

**services/order/routes.py**
- `async def create_order(request: Request, body: CreateOrderRequest, user: dict = Depends(get_current_user), x_payment_simulate: Optional[str] = Header(None)):` (Line 54)

**services/order/consumers.py**
- `async def run_consumer(db_pool: Pool, shutdown_event: asyncio.Event):` (Line 29)
- `async def _handle_message(db_pool: Pool, envelope: dict):` (Line 67)

**services/order/state_machine.py**
- `async def transition_order(conn, order_id: str, new_status: str):` (Line 32)

**services/restaurant/consumers.py**
- `async def run_consumer(db_pool: Pool, shutdown_event: asyncio.Event):` (Line 23)
- `async def _handle_message(db_pool: Pool, envelope: dict):` (Line 55)

**services/restaurant/stock.py**
- `async def reserve_stock(conn, order_id: str, items: list[dict]):` (Line 14)
- `async def release_stock(conn, order_id: str):` (Line 57)

**services/payment/consumers.py**
- `async def run_consumer(db_pool: Pool, shutdown_event: asyncio.Event):` (Line 28)
- `async def _handle_message(db_pool: Pool, envelope: dict):` (Line 60)

**services/payment/gateway.py**
- `async def process_payment(amount: float, simulate_directive: str | None) -> bool:` (Line 13)
