# FoodFlow — Interview Question Bank

Companion to `INTERVIEW_SCRIPT.md`. That file is the **walkthrough you deliver**. This file is the **questions they fire back**.

## How to use this

- ⚠️ marks a **genuine weakness**. Do not defend these. Admitting them well scores higher than a bad defence — interviewers find them anyway, and the only thing you control is whether you found them first.
- Quoted blocks are spoken answers. Say them in your own words, don't recite.
- **Follow-ups** are where they'll go next. If you can't answer the follow-up, you don't own the answer yet.

## The system in one diagram

```
client → nginx → auth-service        (JWT issue/refresh, Redis rate limit)
                 order-service       POST /orders → 202 PENDING
                                      │
                                      ▼ order.placed
                 restaurant-service  reserve stock (SELECT ... FOR UPDATE)
                                      │
                          ┌───────────┴───────────┐
                          ▼ stock.reserved        ▼ stock.rejected
                 payment-service                  │
                  call gateway                    │
                          │                       │
              ┌───────────┴──────────┐            │
              ▼ payment.succeeded    ▼ payment.failed
                 order-service ◄─────────────────┘
                  CONFIRMED / PAYMENT_FAILED / STOCK_UNAVAILABLE
                                      │
                                      ▼ stock.release (compensation)
                 restaurant-service   put stock back
```

Every service: own database, outbox table, `processed_events` table, outbox poller (1s interval).

---

# 1. The first questions

These come immediately after your walkthrough. Rehearse these until automatic.

### Q: You return 202 — what does the user actually see?

> "The order is accepted, not confirmed. The client gets an order ID and status PENDING, then polls `GET /orders/{id}` until it reaches CONFIRMED, PAYMENT_FAILED, or STOCK_UNAVAILABLE. In production I'd push that over WebSocket or SSE instead of polling."

**Follow-ups:**
- *"What if the user closes the app mid-order?"* → The saga keeps running, it's not tied to the connection. They see the result next time they open it.
- *"Why 202 and not just wait and return 200?"* → The saga crosses three services and an external payment gateway. Holding the HTTP connection open ties up a worker and gives the user a spinner that can hang for however long the gateway takes. Accepting and resolving asynchronously means slow payments don't become slow API responses.
- *"How long until it resolves?"* → Roughly 2–3 seconds normally. Three hops, each with a 1-second outbox poll interval, so the polling dominates — the actual work is milliseconds.

### Q: What if you save the order but Kafka is down?

The single most-asked question about event-driven systems. **Know this cold.**

> "I never write to the database and Kafka separately — that's the dual-write problem, where one succeeds and the other doesn't and there's no way to recover. Instead the order row and the `order.placed` event go into the same database transaction. The event lands in an `outbox_events` table. A separate poller reads that table and publishes to Kafka. Either both the order and the event exist, or neither does."

**Follow-ups:**
- *"What if the poller publishes to Kafka, then crashes before marking the row as sent?"* → It publishes again next poll. That's at-least-once by design — the consumer's idempotency check drops the duplicate. You can't make this exactly-once, so you make it at-least-once and make consumers safe.
- *"Two pollers running, don't they double-publish?"* → `SELECT ... FOR UPDATE SKIP LOCKED`. Whoever grabs a row locks it, others skip past instead of blocking. Safe to run many.
- *"Why not just publish to Kafka first, then write the DB?"* → Then a crash after publishing means downstream services act on an order that doesn't exist in my database.
- *"Isn't polling wasteful?"* → Yes, it's a query per second per service even when idle. `LISTEN/NOTIFY` would let the writer wake the poller instantly. Debezium reading the write-ahead log removes polling entirely. Both are upgrades I'd make if the latency mattered.

### Q: What if the same message is delivered twice?

> "Kafka is at-least-once, so duplicates are expected, not exceptional. Every event carries a UUID `event_id` in the envelope. Each service has a `processed_events` table. The duplicate check and the business logic run in the same transaction — so either the work happened and the ID is recorded, or neither did. A repeat delivery hits the check and returns."

**Follow-ups:**
- *"Why does the check have to be in the same transaction?"* → If I recorded the ID separately and crashed in between, I'd either do the work twice or mark it done without doing it. One transaction removes the window.
- *"That table grows forever."* → ⚠️ Yes, no cleanup right now. See §10.
- *"What if two copies of the same message are processed at the exact same instant?"* → Both start their transaction, both see no row, both try to insert. The primary key on `event_id` means the second one fails with a unique violation and rolls back. The constraint is the real guard, the SELECT is just the fast path.

### Q: Why Kafka? Why not just have services call each other over HTTP?

> "With direct calls, order service is down whenever payment is down — failure spreads. With Kafka, if payment is down the events queue in the log and get processed when it comes back. The order takes longer, it doesn't fail. I also get replay: a consumer can re-read history, which you can't do with a fired-and-forgotten HTTP call."

**Follow-ups:**
- *"What do you lose by using Kafka?"* → Simplicity, and immediate feedback. Debugging is harder — a failure is somewhere in a chain of async steps instead of a stack trace. And I have to handle duplicates and ordering myself, which HTTP doesn't make me think about.
- *"Why Kafka over RabbitMQ / SQS?"* → Retention and replay. Kafka keeps messages after they're consumed, so I can reset a consumer group and reprocess. Ordering per key is also guaranteed, which matters because I need events for one order in order. For pure task queuing RabbitMQ would be simpler.

### Q: Why microservices at all? A monolith would be simpler.

**Do not invent a scaling story.** They will poke holes in it.

> "For this traffic, a monolith would genuinely be simpler and better — one database, one transaction, none of this machinery. I split it because the point was to build distributed failure handling, and you can't build that if one database is doing it for you. The real-world justification would be team ownership and independent deploys, but I won't pretend that's why I did it here."

**Follow-ups:**
- *"So when would microservices actually be right?"* → When teams are stepping on each other in one codebase, or when one component has genuinely different scaling or availability needs. Team boundaries usually before technical ones.

---

# 2. Order service

### Q: Who owns the order's state?

> "Order service, exclusively. It's the only thing that writes to the orders table. Transitions go through a state machine with an explicit whitelist — from PENDING you can go to CONFIRMED, PAYMENT_FAILED or STOCK_UNAVAILABLE. Terminal states allow nothing. Anything not in the table raises."

**Follow-ups:**
- *"Why bother with a state machine? Just update the column."* → Because messages arrive out of expected order and duplicated. Without the whitelist, a late `payment.succeeded` could flip an already-failed order to CONFIRMED. The whitelist makes invalid history impossible rather than unlikely.
- *"What locks the row?"* → `SELECT status ... FOR UPDATE` before checking, so two concurrent transitions on the same order serialize.

### Q: Why does `payment.failed` go to order service, which then emits `stock.release`? Why doesn't restaurant just consume `payment.failed` directly? ⭐

**This is the sharpest design question about your system.** Be honest — most of the obvious defences don't hold.

> "Honestly, restaurant *could* consume `payment.failed` directly and it would work today. Payment failure is terminal, `release_stock` already handles a missing reservation, and restaurant has its own idempotency table. So the usual arguments — retries changing the meaning, double-release, restaurant not knowing whether it reserved — don't actually apply to my code.
>
> The one reason that does hold is coupling. Stock won't only need releasing for payment failure — cancellations, fraud checks, saga timeouts all need the same rollback. If restaurant listens for each cause directly it ends up knowing about every service in the system. `stock.release` is one stable command that all causes funnel into, so restaurant doesn't know a payment service exists. For a three-step saga that's arguably more structure than I need, but it's the seam I'd have to add the first time a second cause appears."

**Follow-ups:**
- *"So it's over-engineered?"* → Agree lightly. "For the current scope, a bit. I'd rather have the boundary than retrofit it into a running saga."
- *"Isn't the extra hop slower?"* → About a second, because of the poll interval. Compensation isn't latency-sensitive — the customer already knows the payment failed.
- *"Doesn't that make order service a single point of failure?"* → It's a coordination point, not an availability one. It's a stateless consumer over a durable log. If it's down, events wait in Kafka; the saga is delayed, not lost.

### Q: So order service is the orchestrator?

**Trap.** If you say yes, they'll ask where the command messages are, and you have none.

> "Not really. Restaurant listens to `order.placed`, payment listens to `stock.reserved` — they react to each other directly, order service isn't in the middle. A true orchestrator would send explicit commands and everyone would reply only to it. What order service actually is, is the saga's state owner: it holds the truth about where each order stands and it's the only thing allowed to decide the saga is dead. So it's choreography with a central state owner, not orchestration."

**Follow-ups:**
- *"Which would you pick if you started over?"* → Same hybrid. Full orchestration adds a hop to every step for a linear flow that doesn't branch. Full choreography leaves nobody owning saga state.
- *"When does orchestration win?"* → When the flow branches on conditions, or when you need to see and drive saga state as a first-class thing. Once "who decides what happens next" gets complicated, you want it in one place.

### Q: You have an `Idempotency-Key` header on POST /orders. Why, when you already have event idempotency?

> "Different layer, different problem. Event idempotency stops the same *event* being processed twice inside the system. The header stops the same *user request* creating two orders — the client's request times out, they retry, and without the key that's two orders and two charges. Order service checks the key against existing orders and returns the original instead of creating a new one."

**Follow-ups:**
- *"What if two requests with the same key arrive at once?"* → ⚠️ The check is a SELECT before the insert, so there's a race window. A unique constraint on `idempotency_key` is what actually makes it safe — the second insert fails and you return the existing order. Worth confirming that constraint exists rather than relying on the read.

---

# 3. Restaurant service

### Q: Two people order the last item at the same time.

> "Stock reservation does `SELECT quantity ... FOR UPDATE`, which locks the row. The second transaction blocks until the first commits, then reads the updated quantity and gets rejected. No overselling."

**Follow-ups:**
- *"What if you'd used SELECT without FOR UPDATE?"* → Both read 1, both think there's stock, both deduct, quantity goes to -1. Classic lost update.
- *"Optimistic locking instead?"* → A version column and retry on conflict. Better when conflicts are rare. Here conflicts are common on popular items during a rush, so pessimistic locking avoids a retry storm.

### Q: Halfway through a 5-item order, item 4 is out of stock. What about items 1–3?

> "The reservation runs inside a savepoint — a nested transaction. If any item fails, the savepoint rolls back and the deductions for items 1–3 are undone, but the outer transaction stays alive so I can still record the rejection and publish `stock.rejected` in the same transaction."

**Follow-ups:**
- *"What happens if you catch the error inside the savepoint block instead of outside?"* → The context manager never sees the exception, nothing rolls back, and partial deductions commit. The `except` has to be outside the block. It's called out in a comment in the code because it's easy to get wrong.
- *"Could you avoid partial deduction entirely?"* → Check all items first, then deduct. But between check and deduct another transaction can take the stock, so you'd need locks on every row anyway. The savepoint is cleaner.

### Q: Why does the restaurant service subscribe to `stock.release` rather than knowing about payments?

See §2. One entry point for compensation, no knowledge of upstream services.

**Follow-ups:**
- *"What if a `stock.release` arrives for an order that was never reserved?"* → `release_stock` looks up the reservation, finds nothing, logs a warning and returns. Safe no-op.
- *"What if it arrives twice?"* → The `processed_events` check stops it. Without that you'd credit stock back twice and invent inventory.

---

# 4. Payment service

### Q: The payment gateway times out. Did the card get charged or not?

> "I can't know from the response, so I don't try. I write a payments row as PROCESSING and commit *before* calling the gateway. Then I call it, then a second transaction records the outcome. If I crash or hang in between, that row is stuck in PROCESSING — which is a durable record that says 'I started something and don't know how it ended.' A reconciliation worker runs every 60 seconds and resolves anything stuck there for more than 3 minutes."

**Follow-ups:**
- *"Why not one transaction around the whole thing?"* → You can't hold a database transaction open across an external HTTP call — it holds locks for the gateway's latency, and if the process dies the transaction rolls back as if the call never happened, even though the card was charged.
- *"What does reconciliation actually do?"* → ⚠️ Be honest: it reproduces the outcome from the persisted simulate directive rather than querying the real gateway. Against a real provider it would look the payment up by an idempotency key and take the authoritative answer. The pattern is right; the lookup is stubbed.
- *"Why does the crash case need reconciliation at all — won't Kafka redeliver?"* → **This is the subtle one, and it's your best "hardest part" story.** The `processed_events` insert is in transaction 1, which already committed. So on redelivery the idempotency check sees the ID and skips — no event is ever published, and the order sits PENDING forever. Idempotency and crash recovery fight each other here. Reconciliation exists to close exactly that hole.
- *"Why mark it processed so early then?"* → So a gateway crash doesn't cause a re-charge. I'd rather risk a stuck record I can reconcile than double-charge a customer.

### Q: How do you avoid charging someone twice?

> "Two layers. The `processed_events` check means the same event never triggers two charges. And there's a unique constraint on `order_id` in the payments table, so even a different event for the same order can't create a second payment row."

**Follow-ups:**
- *"What if the constraint fires?"* → It's caught and logged as `duplicate_payment_skipped`, and the handler returns. ⚠️ Worth noting: it returns *without publishing any event*, so if that ever fired for a legitimate order, the order would sit PENDING. It's a defensive branch that would need the saga timeout to be safe.

---

# 5. Auth service

### Q: How does order service know who the user is?

> "The client sends a JWT in the Authorization header. Order service verifies the signature locally with the shared secret — no call to auth service. So auth being down doesn't stop people placing orders with tokens they already hold."

**Follow-ups:**
- *"Why not validate centrally?"* → It puts auth on the critical path of every request — latency plus a hard dependency. Signature verification is microseconds and needs no network.
- *"What do you lose?"* → Instant revocation. A token stays valid until it expires, no matter what.

### Q: How do you log someone out?

> "Revoke the refresh token — it's stored hashed in the database, so deleting it means they can't mint a new access token. The existing access token stays valid until expiry. For instant revocation you'd need a blocklist in Redis checked on every request, which trades away the whole benefit of local validation."

**Follow-ups:**
- *"Why is the refresh token hashed with SHA-256 and the password with bcrypt?"* → Different threat models. Passwords are low-entropy and human-chosen, so they need a slow hash to resist brute force. A refresh token is a long random string — brute-forcing it isn't feasible, and it's checked on every refresh, so a slow hash would just be latency.

### Q: You're using HS256 with a shared secret. Problem? ⚠️

**Real weakness. Don't defend it.**

> "Yes. Every service needs the secret to verify, which means every service can also mint tokens — the auth service isn't actually privileged. And rotating the secret means coordinating all four at once. The right answer is RS256: auth holds the private key and signs, everyone else has the public key and can only verify. That's what I'd change before production."

**Follow-ups:**
- *"How would you rotate keys with RS256?"* → Publish a JWKS endpoint with a key ID in each token header. Add the new key, let services pick it up, start signing with it, retire the old one after the longest token lifetime.

### Q: Any rate limiting?

> "On auth endpoints — a sliding window counter in Redis keyed by IP. It's deliberately fail-open: if Redis is down, requests go through rather than locking everyone out of login."

**Follow-ups:**
- *"Fail-open on a security control is a choice. Defend it."* → It is, and it's arguable. I'd rather risk brute-force during a Redis outage than take authentication down entirely for everyone. That depends on the business — for a bank I'd fail closed. Either way it needs an alert on Redis being unreachable, so the degraded state is loud.
- *"Rate limiting by IP is weak."* → ⚠️ Agreed. Shared NATs get one bucket, and an attacker with many IPs bypasses it. Per-account limiting on login attempts would be the stronger control, ideally alongside IP limits.

---

# 6. Kafka and messaging

### Q: Two events for the same order arrive out of order. Problem?

> "The Kafka message key is the order ID, so all events for one order land on the same partition, and a partition is consumed in order by one consumer in the group. Events for a single order are always ordered. Different orders interleave, which doesn't matter."

**Follow-ups:**
- *"What if you hadn't keyed by order ID?"* → Round-robin across partitions, so `payment.succeeded` could be processed before `stock.reserved`. The state machine would reject the invalid transition — which is a safety net, but it'd be rejecting valid orders.
- *"Doesn't keying create hot partitions?"* → It can if one key is disproportionately busy. Order IDs are UUIDs so they spread evenly. Keying by restaurant ID would be a real risk.

### Q: How do you scale a consumer?

> "More instances in the same consumer group — Kafka assigns partitions across them. The ceiling is partition count: with 6 partitions, the 7th instance idles."

**Follow-ups:**
- *"So just add partitions?"* → You can add but never remove. And partition assignment is by hash of the key, so adding partitions changes where an order ID lands — events for an in-flight order could go to a different partition and be processed out of order. I'd add during a quiet window, or over-provision from the start since partitions are cheap.
- *"What happens during a rebalance?"* → Partitions get reassigned and processing pauses briefly. Uncommitted messages get reprocessed by the new owner — safe because of idempotency.

### Q: Do you version your events? ⚠️

> "No. The envelope has `event_id`, `event_type`, `occurred_at` and `payload` — no version field. It's one shared `wrap_event` function so adding one is a one-line change, but I haven't. The rule I follow informally is additive-only: add optional fields, never rename or remove. Anything bigger would need a `v2` topic running in parallel."

**Follow-ups:**
- *"What breaks if you rename a field today?"* → Consumers throw `KeyError` on every message with the new shape. And because of how my error handling works, those messages get dropped rather than retried — so a rename during a rolling deploy silently loses orders. That makes the missing DLQ worse than it looks.

---

# 7. Failure handling ⚠️ (your weakest area — know it best)

### Q: A message fails processing. What happens to it?

**The biggest gap in the system. Lead with it before they find it.**

> "It gets dropped. My consumers catch the exception, log it, and then commit the offset anyway — so Kafka never redelivers it. The intent was to avoid a poison message blocking the partition forever, but it's the wrong trade: I'm silently losing data to avoid a stall. Worth noting the docstring in my Kafka helper says consumers only commit after successful processing, which is what I meant to do and not what the loop does."

**Follow-ups:**

*"Why not just skip the commit and let it retry?"* — **Know this properly, it's the follow-up that separates people:**
> "That doesn't do what it sounds like. A consumer has two positions: an in-memory read position that moves forward on every fetch, and the committed offset which is only consulted on restart or rebalance. Skipping the commit doesn't rewind anything — the next poll still returns the *next* message. The failed one is skipped regardless; the only difference is that a future restart would replay it. To genuinely retry I'd have to `seek()` backwards."

*"Fine, so seek backwards."*
> "Two problems. It replays the whole rest of the batch — safe for me, because `processed_events` catches the re-delivery, which is a nice illustration that the pieces fit together. But if the message fails deterministically, like a malformed payload, it loops forever and the partition never advances. Every order behind it stops. That's the poison message trap."

*"So what's the actual fix?"*
> "Retry inside the handler, not at the offset level — the offset is per partition, the problem is per message. Two or three attempts with a short delay clears transient failures. Anything still failing is a real bug, so it goes to a dead letter topic and then it's safe to commit. The partition keeps moving and nothing is lost."

### Q: Give me a concrete example of a message failing.

Have specifics ready — vague answers die here.

> "Ordinary things, not exotic ones. A Postgres connection blip or a failover — the transaction dies mid-message. Connection pool exhaustion during a traffic spike, which is worst because it hits a burst of orders at peak. A deadlock, where Postgres kills one of two transactions on purpose — normal database behaviour whose expected response is 'retry', and I don't. Then permanent ones: a `KeyError` on a missing payload field, or an `InvalidTransitionError` from the state machine."

**Follow-up:** *"What's the worst case?"*
> "Order service drops a `payment.succeeded`. The card is charged, the stock is deducted, the order stays PENDING forever. The customer paid and got nothing, and the only trace is one log line. A two-second network blip is enough to cause it."

**Follow-up:** *"Why do those examples argue for your fix specifically?"*
> "They split cleanly. Connection blips, pool exhaustion and deadlocks would all succeed on a retry — I'm throwing away recoverable orders for nothing. Malformed payloads never succeed no matter how often I retry — those need a human, and I'm throwing away the evidence too. Retry-then-DLQ handles both groups correctly, which is why it's the right shape and not just 'add a queue'."

### Q: When would an order sit in PENDING for a long time?

> "Two kinds. Temporary — a service is down, a poller is stuck, consumers are lagging. Those catch up on their own; PENDING is just slow. And permanent — a dropped message. That never recovers, because Kafka won't redeliver and nothing scans for stranded orders."

**Follow-ups:**
- *"Anything else stuck alongside the order?"* → Yes, and this makes it worse: once stock is reserved, a stranded order holds inventory permanently. It's not just one unhappy customer, it's items nobody else can order, missing from the counts forever.
- *"What would you add?"* → A saga timeout — a job scanning for orders in PENDING past a threshold, failing them and emitting compensation. Same pattern as the payment reconciliation worker, one level up. ⚠️ It doesn't exist today; payment has reconciliation, the order saga has nothing.

### Q: A service is down for an hour. What breaks?

> "Nothing breaks, things pause. Events accumulate in Kafka. When it comes back it resumes from its last committed offset and drains the backlog. Orders in flight take an hour longer; none are lost."

**Follow-ups:**
- *"What if it's down longer than Kafka's retention?"* → Then messages are deleted and those orders are stranded. Retention has to exceed the worst realistic outage, and the saga timeout is the backstop that catches whatever falls through.
- *"Kafka itself goes down?"* → Writes still work. Orders commit to Postgres with events in the outbox; the poller just fails and retries. Once Kafka's back the backlog drains. The outbox is what makes a broker outage survivable.

---

# 8. Data and consistency

### Q: A database per service — really? ⚠️

**Gotcha. They may have already looked at your compose file.**

> "Four separate logical databases — auth_db, order_db, restaurant_db, payment_db — on one Postgres container. The architectural rule holds: no service can read another's tables, and nothing shares a connection string. But you're right that it's one failure domain and one resource pool. Splitting to separate instances is a config change rather than a code change, precisely because nothing crosses the boundary."

**Follow-ups:**
- *"How do you do a query that spans services — 'all orders with restaurant names'?"* → You can't join. Either the client calls both, or you denormalize what you need into the event payload, or you build a read model that consumes events from both. For a real app I'd denormalize the restaurant name into the order at creation time.
- *"What about reporting across all of it?"* → A separate analytics database fed by the same events, kept out of the transactional path.

### Q: Your system is eventually consistent. When does that hurt the business?

> "Between reserving stock and confirming payment, inventory is held for an order that isn't confirmed. If payment is slow, we're holding stock we may hand straight back. For a restaurant, a few seconds is fine. For concert tickets you'd want a short explicit hold with an expiry, because holding inventory is itself the scarce thing."

**Follow-ups:**
- *"Where would you accept it and where wouldn't you?"* → Fine for the order lifecycle — a couple of seconds to confirmation is invisible. Not fine inside a single stock decrement, which is why that's a locked transaction, not an eventually-consistent counter.

### Q: Why a saga instead of a distributed transaction / two-phase commit?

> "2PC needs every participant to hold locks until everyone agrees, plus a coordinator that can't fail. Across separate databases and an external payment gateway that isn't realistic — the gateway won't enlist in my transaction. A saga accepts that each step commits independently and gives each one an undo."

**Follow-ups:**
- *"What does a saga give up?"* → Isolation. There's a window where stock is deducted and payment hasn't happened, and anyone reading during that window sees a half-finished state. 2PC would hide it. Sagas expose it and expect the business to tolerate it.

---

# 9. Scaling

### Q: How would you scale this?

> "Each part scales independently, which is the payoff for splitting it. So the question is what hits its limit first, and they're all different."

**API services** — stateless, add instances behind nginx. Never the problem.
**Consumers** — more instances in the group, ceiling is partition count.
**Kafka** — more brokers.
**Postgres** — the hard one, the only stateful piece.

**Follow-up:** *"What breaks first?"*
> "Postgres, because everything else scales by adding machines and it doesn't. And within Postgres, stock reservation, because of row locking."

### Q: Why is stock reservation the bottleneck? ⭐

> "`SELECT ... FOR UPDATE` locks one row and holds it until commit, so every order for the *same item* queues behind it. Throughput for that item is one divided by lock hold time — not bound by CPU or connections. Lunch rush, everyone ordering the popular dish, and they all serialize on one row. Different items are fine, they're different rows."

**Follow-up:** *"How would you fix it?"* — three options, cheapest first:

1. **Hold the lock for less time.** Currently two round trips — `SELECT FOR UPDATE` then `UPDATE`. Collapse to one statement: `UPDATE ... SET quantity = quantity - $1 WHERE item_id = $2 AND quantity >= $1 RETURNING quantity`. No row returned means insufficient stock. Same safety, about half the lock time. **Start here.**
2. **Split the counter.** Ten rows of 10 instead of one row of 100; orders hash to a bucket, so ten proceed in parallel. Costs: availability checks sum N rows, and you can be told "no stock" while another bucket has some.
3. **Move it to Redis.** Atomic decrement, no lock. But stock now lives outside the transaction, so a crash between the Redis decrement and the Postgres write desyncs them. Only at genuinely high scale.

### Q: Anything else generating load you haven't mentioned? ⭐

Easy to miss because it's outside the saga.

> "The polling. Clients poll `GET /orders/{id}` until the order resolves. Ten thousand orders in flight at a one-second poll is ten thousand requests a second hitting Postgres to ask 'are we there yet', almost all returning the same answer as last time. That's more load than placing orders. Push over WebSocket or SSE and it's one message per status change. If I had to keep polling, cache the status in Redis so it never touches Postgres."

### Q: What about latency rather than throughput?

> "The outbox pollers dominate. Each sleeps a second between polls, and there are three hops before confirmation, so polling is most of the 2–3 seconds — the actual work is milliseconds. Shortening the interval means hammering the database with empty queries. `LISTEN/NOTIFY` is the better fix: the writer wakes the poller, polling becomes a fallback. Beyond that, Debezium off the write-ahead log."

### Q: How do you know when to scale?

> "Consumer lag first — how far behind the latest message each group is. Steady growth means consumers can't keep up, and it goes bad before users notice, so it's the one I'd alert on. Then p99 API latency, connection pool wait time, and how long orders sit in PENDING."

**Follow-up:** *"Multi-region?"* → Much harder — the saga assumes one Kafka cluster and one database. Either pin an order to a region and keep the saga local, or accept cross-region lag. I'd want a real business reason; it's a different architecture, not a bigger one.

---

# 10. Operations and deployment

### Q: These tables grow forever. Thought about it? ⚠️

> "No cleanup on `outbox_events` or `processed_events` — both grow indefinitely. Published outbox rows can be deleted after a day, they've done their job. `processed_events` is subtler because it's the duplicate guard, but Kafka retention is finite, so once a message can no longer be redelivered the row is dead weight. I'd keep it somewhat past the retention window and delete in batches. Left alone it's a slow leak that eventually hurts insert performance and bloats indexes."

**Follow-up:** *"Why not just delete outbox rows immediately after publishing?"* → Then a crash between publishing and deleting looks identical to never having published. Keeping them with a `published_at` timestamp means the state is always readable, and deletion is a separate lazy job.

### Q: How do you deploy without downtime?

> "API services sit behind nginx, so rolling restart — new instances up, old ones drained. Consumers are easier: stop one, Kafka rebalances its partitions to the others, start the new version. Idempotency means a message half-processed during a restart is safe to redo."

**Follow-ups:**
- *"What about a message published by a new version and consumed by an old one mid-rollout?"* → Why the additive-only rule matters — old consumers must ignore fields they don't know. ⚠️ Without event versioning I'm relying on discipline rather than anything enforced.

### Q: How do you run a migration safely?

> "Additive only while traffic is live. Add a nullable column, backfill, then start using it. Never rename or drop in the same deploy as the code change, because both versions run simultaneously during a rollout. Drops happen a release later once nothing references the column."

### Q: How would you know something is wrong in production?

> "Prometheus metrics on messages consumed, labelled by topic and success/failure, so an error spike on one topic is visible. Structured JSON logs carrying the order ID so I can trace one order across all four services."

**Follow-ups:**
- *"What's missing?"* → ⚠️ Alerts. I have metrics but nothing paging anyone. The three I'd add: consumer lag growth, outbox rows unpublished past a threshold, and orders stuck in PENDING. Those catch most silent failures.
- *"Distributed tracing?"* → Not implemented. The order ID acts as a poor man's correlation ID. Proper tracing would mean propagating a trace ID through the event envelope into OpenTelemetry.

### Q: Secrets? ⚠️

> "Environment variables in docker-compose, and the JWT secret has a dev default in code. Fine for local, not for production — that wants a real secret manager and no fallback default, so a missing secret fails loudly at startup instead of silently running with a known one."

---

# 11. Extending the system

### Q: Add delivery to the flow. What changes?

> "New service subscribing to `payment.succeeded`, publishing `delivery.assigned` or `delivery.failed`. Order service learns two new events and gains a state. If delivery fails it's the same compensation shape — release stock, trigger a refund. Nothing existing changes, which is the payoff for routing compensation through order service."

### Q: The customer cancels after payment succeeded.

> "That's a new command, not a saga failure — the saga already completed successfully. So it's an `order.cancel` request that checks whether cancellation is still allowed, then emits `stock.release` plus a refund event. It reuses the compensation events but it's a new flow, not a rollback."

The distinction between *compensating a failed saga* and *undoing a completed one* is a genuinely senior answer.

**Follow-up:** *"What if it's already being cooked?"* → That's why it's a policy decision, not an automatic rollback. The state machine decides which states allow cancellation; past a point you refund partially or not at all.

### Q: Multiple restaurants in one order?

> "The saga becomes a fan-out — reserve at N restaurants, and you need all N to succeed before charging. That means order service tracking partial completion, and compensation has to release only the ones that succeeded. That's the point where I'd switch to real orchestration, because 'are we there yet' becomes genuine state that someone has to own."

---

# 12. Project-level questions

### Q: What was the hardest part?

Pick something specific and technical.

> "The payment service's crash window. It needs two transactions — record intent, call the gateway, record outcome — because you can't hold a transaction open across an external call. But the idempotency marker is in the first transaction, so if you crash in between, Kafka redelivers, the idempotency check sees the ID and *skips* the message. No event is ever published and the order hangs forever. Idempotency and crash recovery actively fight each other there. That's why the reconciliation worker exists — it's the only way out."

### Q: What would you do differently?

Three specific things, no waffle.

> "Dead letter queues from day one — I under-thought consumer failure and it's the real hole. A saga timeout, so nothing can sit in PENDING forever. And RS256 instead of a shared JWT secret."

### Q: Is this production ready?

Don't oversell.

> "The architecture is — outbox, idempotency, compensation, the state machine are all real and tested. The operational side isn't: no DLQ, no saga timeout, no table cleanup, one Postgres instance, secrets in environment variables, metrics but no alerts. It's a correct skeleton that needs hardening, not a rewrite."

### Q: How did you test it?

> "Three levels. Unit tests for the state machine and stock logic, no infrastructure. Integration tests against the running API. And saga tests that drive a full order through real Kafka and Postgres and assert the final state — including failure paths, like forcing a payment failure and verifying the stock actually came back."

**Follow-up:** *"How do you test a crash?"* → The payment simulate directive lets me force specific outcomes deterministically. ⚠️ Real crash injection — killing a process mid-transaction — isn't automated; I verified those paths manually.

---

# 13. Your known gaps — memorize this list

If you can recite these unprompted, you control the conversation. An interviewer who finds a gap you already named reads it as self-awareness. One who finds a gap you defended reads it as a blind spot.

| # | Gap | The one-line answer |
|---|-----|---------------------|
| 1 | **No DLQ** — failed messages dropped, offset committed anyway | Retry 2–3 times in the handler, then park in a dead letter topic, then commit |
| 2 | **No saga timeout** — orders can sit PENDING forever | A scanner like the payment reconciliation worker, one level up: fail stale orders and emit compensation |
| 3 | **No table cleanup** — outbox and processed_events grow forever | Batch-delete published outbox rows after a day; keep processed_events just past Kafka retention |
| 4 | **Shared HS256 JWT secret** — every service can mint tokens | RS256, auth signs with a private key, everyone else verifies with the public one |
| 5 | **One Postgres instance** — four logical DBs, one failure domain | The boundary is enforced in code, so splitting is config not refactoring |
| 6 | **No event versioning** — envelope has no version field | Additive-only by discipline today; a version field is one line, a v2 topic for anything bigger |
| 7 | **Metrics but no alerts** | Consumer lag, unpublished outbox rows, stale PENDING orders |
| 8 | **Reconciliation doesn't query the real gateway** | Reproduces from a persisted directive; real version looks up by idempotency key |

**The framing that ties it together:** the *correctness* machinery is built and tested. The *operational* machinery — what happens when something fails in a way you didn't anticipate — is where the gaps are. That's a coherent story about where you spent your time, not a list of things you forgot.
