# FoodFlow — Interview Walkthrough Script

Everything below is grounded in the actual code. File:line references are for *your* recall — don't recite them, but knowing them means you can open the file when they ask.

---

## 1. THE 30-SECOND PITCH

> "It's the backend for a food ordering app. When you place an order, three different things have to happen — the restaurant has to set aside the food, we have to take your money, and then we confirm the order. Those three things are handled by three separate programs that don't talk to each other directly. They pass messages, like a relay race.
>
> The hard part isn't the ordering — it's what happens when something goes wrong halfway through. If we set aside the food but your card gets declined, the food has to go back on the shelf automatically. And if a server crashes right in the middle, the system has to be able to pick up where it left off without charging you twice or losing your order.
>
> So really, the project is about failure handling. The ordering part is the excuse to build the failure handling."

**Why this works:** it names the actual engineering problem in the first 20 seconds without using a single pattern name. You'll introduce "saga," "outbox," and "idempotency" later — each one *after* you've described the problem it solves.

---

## 2. THE WALKTHROUGH

Read each chunk out loud. Stop at the end of each one — that pause is where the interviewer jumps in.

---

### CHUNK 1 — The problem

**WHAT I SAY:**

> "The core problem is that placing an order isn't one action, it's three, and they live in three different services with three different databases. Reserve the stock, take the payment, confirm the order. In a normal single-database app you'd wrap all three in one transaction — if any step fails, everything rolls back and it's like nothing happened. I can't do that here, because there's no single database to roll back. Once the restaurant service has deducted its stock and committed, that's committed. The payment service can't undo it. So instead of preventing partial failure, the system has to detect it and actively undo it."

**LIKELY INTERRUPTION:** *"Why split it into separate services at all? Wouldn't one service with one database be much simpler?"*

**HOW I ANSWER:**

> "Honestly — yes, for the traffic this project actually handles, a monolith would be simpler and better. I split it because the point of the project was to learn distributed failure handling, and you can't learn that if there's one database doing it for you. The real-world justification would be team ownership and independent scaling — the payment team ships on its own schedule, and you can scale the order service for a lunch rush without scaling payments. But I wouldn't pretend that's why I did it here. I did it to build the hard parts."

*(This answer is worth memorizing. Interviewers respect "the honest reason is learning" far more than a made-up scaling story they can poke holes in.)*

---

### CHUNK 2 — The architecture

**WHAT I SAY:**

> "There are four services. Auth handles signup, login, and tokens. Restaurant owns the menus and the stock counts. Payment does the charging. Order owns the order itself and its status. Each one has its own Postgres database and physically cannot read another service's tables — no shared schema, no cross-service joins. In front of everything there's an nginx reverse proxy on port 8085 that routes by URL prefix: anything starting with `/auth` goes to the auth service, `/orders` to the order service, and so on.
>
> The services never call each other over HTTP. They communicate by publishing messages to Kafka, which is a message log — think of it as a shared append-only notebook. One service writes 'order was placed,' and any service that cares reads it and reacts."

**LIKELY INTERRUPTION:** *"Why messages instead of just having the order service call the payment service directly over HTTP?"*

**HOW I ANSWER:**

> "Two reasons. First, availability: if I call payment over HTTP and payment is down, my request fails and the customer sees an error. With messages, the order service writes the event and returns immediately — the message sits in Kafka until payment comes back up, and then it processes normally. The customer never knows payment was down. Second, coupling: with HTTP, the order service has to know payment exists, know its address, and handle its errors. With events, the order service just announces what happened. If we later add a loyalty-points service that also cares about placed orders, we add it without touching the order service at all."

**LIKELY FOLLOW-UP:** *"So what's the downside?"*

> "You lose the immediate answer. When someone places an order, I can't tell them 'confirmed' — I have to say 'received, we'll let you know.' That's why `POST /orders` returns 202 Accepted with status PENDING rather than 200 with a final answer. For food delivery that's fine; for something like a bank transfer where the user expects an immediate yes or no, it'd be the wrong choice."

---

### CHUNK 3 — Tracing one order: the write

**WHAT I SAY:**

> "Let me trace an actual order. The request hits `POST /orders` on the order service. First it validates the JWT — it pulls the token out of the Authorization header and verifies the signature locally, no network call to auth. Then it opens one database transaction and does exactly two inserts: the order row with status PENDING, and a second row in a table called `outbox_events` that contains the message it *wants* to send to Kafka. Both inserts commit together or neither does. Then it returns 202 immediately — it has not talked to Kafka at all yet."

**LIKELY INTERRUPTION:** *"Wait — why write the message to a database table instead of just publishing it to Kafka right there?"*

**HOW I ANSWER:** *(This is the outbox question. Lead with the failure, not the pattern name.)*

> "Because if I do both — commit to the database, then publish to Kafka — there's a gap between them where the process can die. If it dies in that gap, the order exists in my database with status PENDING, but no message was ever sent. Nobody reserves stock, nobody charges the card, and that order sits there forever. It's silently broken and nothing in the system will ever notice.
>
> I could flip the order and publish first, but then the opposite happens: the message goes out, stock gets reserved, the card gets charged, and then my database write fails — so I've charged someone for an order that doesn't exist.
>
> The fix is to never have two separate writes. The message goes into my own database, in the same transaction as the order. Now there's no gap — either both rows exist or neither does. A separate background loop reads that table and does the actual publishing. That's called the transactional outbox pattern."

**LIKELY FOLLOW-UP:** *"Where's that background loop?"*

> "It's in `shared/outbox.py`, running as an asyncio task inside the same process as the API. It polls the `outbox_events` table about once a second for rows where `published_at` is null, sends them to Kafka, then marks them published."

---

### CHUNK 4 — The relay, and why it's safe

**WHAT I SAY:**

> "The relay query is `SELECT ... FOR UPDATE SKIP LOCKED`. `FOR UPDATE` locks the row it grabs, so if I'm running two copies of the order service, they can't both grab the same event and publish it twice. `SKIP LOCKED` means the second one doesn't sit there waiting — it just skips that row and takes the next one, so both copies stay busy instead of queuing behind each other."

**LIKELY INTERRUPTION:** *"What if it sends to Kafka successfully but crashes before it marks the row published?"*

**HOW I ANSWER:**

> "Then it republishes the same event on the next poll — the row still looks unpublished. So that message gets delivered twice. I decided that's the right way for it to fail: a duplicate message is recoverable, a lost message isn't. The cost is that every consumer downstream has to be able to handle receiving the same message twice without doing the work twice — which I'll get to, because that's a whole thing on its own. There's actually a test for this exact scenario, `tests/saga/test_outbox_crash_recovery.py`, which inserts a duplicate unpublished row and asserts stock only gets deducted once."

---

### CHUNK 5 — Restaurant service: reserving stock

**WHAT I SAY:**

> "The restaurant service is subscribed to the `order.placed` topic. It picks up the message and, inside one transaction, does `SELECT quantity ... FOR UPDATE` on each item — that locks the stock row so no other order can touch it at the same time. If there's enough, it deducts the quantity, writes a reservation record so it remembers what to give back later, and writes a `stock.reserved` event to its own outbox. If there isn't enough, it writes `stock.rejected` instead."

**LIKELY INTERRUPTION:** *"What stops two simultaneous orders from both buying the last burger?"*

**HOW I ANSWER:**

> "The `FOR UPDATE` lock. The first transaction to reach that row locks it and holds the lock until it commits. The second one blocks — it physically waits at that line — and when it finally reads, it sees the already-decremented number and correctly rejects. Without `FOR UPDATE` both would read '1 available' at the same time, both would deduct, and I'd oversell.
>
> There's a test for this: `tests/saga/test_concurrency_stock_lock.py` sets stock to exactly 1, fires five concurrent orders, and asserts exactly one is confirmed, four are rejected, and the final stock is exactly zero."

**LIKELY FOLLOW-UP:** *"Could that lock deadlock?"*

> "Yes, and it's a real gap in my code. I loop over items in whatever order the client sent them. So order A locks the burger then reaches for the fries, while order B locks the fries then reaches for the burger — both wait forever and Postgres kills one of them. The fix is one line: sort the items by ID before locking, so every transaction always grabs locks in the same order and that cycle can't form. I know about it and I haven't fixed it."

---

### CHUNK 6 — Payment: the part I'm actually proud of

**WHAT I SAY:**

> "Payment listens for `stock.reserved`. This is the one place where the code deliberately does something that looks wrong. Instead of one transaction, it uses two, with the external gateway call in between. First transaction: write a payment row with status PROCESSING, and immediately record that I've handled this message — before I've actually charged anyone. Then call the gateway. Then a second transaction: update the row to SUCCEEDED or FAILED and write the outcome event."

**LIKELY INTERRUPTION:** *"Why mark it processed before you've actually done the work? That seems backwards."*

**HOW I ANSWER:** *(This is your best answer in the whole interview. Slow down here.)*

> "Because of what happens if the process dies during the gateway call. At that moment I genuinely do not know whether the customer was charged — the request left my server, and I never got an answer.
>
> If I hadn't marked it processed, Kafka would redeliver that message when I restart, and I'd call the gateway again. If the first call actually went through, I've now charged the customer twice.
>
> By marking it processed first, redelivery gets skipped, so I can never double-charge. The cost is the opposite failure: the payment is stuck at PROCESSING forever and the order never resolves.
>
> So I picked which failure I wanted. Double-charging a customer is a support ticket, a refund, and a very angry person. A stuck payment is invisible to the customer and I can fix it with a background job. So I chose 'might not charge' over 'might double-charge' — and then I wrote the background job that cleans up the stuck ones."

**LIKELY FOLLOW-UP:** *"What's the background job?"*

> "`services/payment/reconciliation.py`. Every 60 seconds it looks for payments still at PROCESSING that haven't been touched in three minutes, resolves them, and publishes the outcome. It re-checks the status with `FOR UPDATE` inside the transaction before writing, so if something else already resolved it in the meantime, it backs off instead of double-writing."

**LIKELY FOLLOW-UP:** *"How does it know whether the payment should have succeeded or failed?"*

> "It reads the `payment_simulate` directive that Transaction 1 persisted on the payment row. My gateway is a pure function of that directive, so re-deriving the outcome gives the same answer the original call would have — that's what makes recovery deterministic rather than a guess.
>
> This was actually a bug I found and fixed. The directive arrives on the event but originally was only held in memory, so reconciliation passed `None` and fell back to `PAYMENT_SIMULATE_DEFAULT`, which is `success`. That meant a crash mid-gateway would silently resolve a deliberately-failing order to SUCCEEDED — the recovery path inverted the business outcome. The fix was a column and one line in each of the two places. There's a regression test for it in `tests/unit/test_reconciliation.py`.
>
> I'd flag what this does *not* solve: re-deriving only works because the gateway is fake and deterministic. A real gateway's outcome isn't derivable — only it knows whether the money moved — so the real fix is the idempotency-key lookup I describe under A2, which also distinguishes 'still pending' and 'never received' from a settled result."

---

### CHUNK 7 — Closing the loop, and the state machine

**WHAT I SAY:**

> "The order service listens for `payment.succeeded`, `payment.failed`, and `stock.rejected`. On success it moves the order to CONFIRMED and we're done. All the status transitions go through one function in `state_machine.py` that has an explicit table of what's allowed — from PENDING you can go to CONFIRMED, PAYMENT_FAILED, or STOCK_UNAVAILABLE, and the three end states can't go anywhere. Anything else raises."

**LIKELY INTERRUPTION:** *"Why bother with a state machine? It's four states."*

**HOW I ANSWER:**

> "Because in an event-driven system messages arrive out of order and late. Without that table, a delayed `payment.succeeded` could arrive after an order was already cancelled and flip a cancelled order to confirmed — and I'd have no idea it happened. The table makes that structurally impossible rather than depending on me remembering to check. It also does `SELECT ... FOR UPDATE` on the order row first, so two events for the same order can't interleave and both read the old status."

---

### CHUNK 8 — The failure path

**WHAT I SAY:**

> "If payment fails, the order service does two things in one transaction: sets the order to PAYMENT_FAILED, and writes a `stock.release` event to its outbox. The restaurant service picks that up, looks up the reservation record it saved earlier, adds the quantities back, and deletes the reservation. That's the undo."

**LIKELY INTERRUPTION:** *"So this is a saga — what happens if the compensation itself fails?"*

**HOW I ANSWER:**

> "Right now, not enough. The release event goes through the same outbox, so it won't get lost — it'll be retried until it publishes. But if the restaurant service throws while *processing* it, my consumer logs the error and moves on, and that stock is never returned. There's no dead-letter queue and no alerting on it. In production that needs a dead-letter topic and a metric that pages someone when compensation fails, because unreleased stock is invisible revenue loss — the food is there, the system just thinks it isn't."

**On the word "saga" — define it the first time you say it:**

> "A saga just means: instead of one big transaction that can roll back, you have a chain of small local transactions, and each one has a matching undo step. If step three fails, you run the undo for steps two and one. You never truly roll back — you move forward into a corrected state."

---

### CHUNK 9 — Idempotency

**WHAT I SAY:**

> "Everything I've described depends on one thing: any service can receive the same message twice and must not do the work twice. Kafka guarantees at-least-once delivery, my outbox can republish after a crash, and consumers can reprocess after a restart. So every consumer, in the same transaction as its business logic, inserts the message's ID into a `processed_events` table. If the ID is already there, it skips. Because that check and the actual work are in one transaction, there's no window where it does the work but forgets it did."

**LIKELY INTERRUPTION:** *"Doesn't Kafka have exactly-once semantics?"*

**HOW I ANSWER:**

> "It has exactly-once *within Kafka* — reading from a topic and writing to another topic transactionally. But my side effect is a Postgres write in a different database, which Kafka's transaction can't cover. The moment your effect leaves Kafka, you're back to at-least-once delivery, and the only real answer is making the consumer idempotent. That's what `processed_events` is."

**LIKELY FOLLOW-UP:** *"There's also an Idempotency-Key header on the order endpoint — what's that for?"*

> "Different layer, same idea. That one protects against the *client* retrying — user double-clicks, or the network drops the response and the browser retries. Same key means you get the existing order back instead of a second one. And I'll be upfront: that implementation has a bug in it, which I'll cover when we get to what's broken."

---

### CHUNK 10 — What's known-broken

**WHAT I SAY:**

> "I should walk you through what I know is wrong, because there's a real list. The biggest one is that the order service trusts the client for prices. The request body contains the price per item and the server just multiplies and sums it — it never checks against the restaurant's actual menu. So you can order a burger for one cent. That's a straightforward exploit and it's documented in the README as a known limitation. The fix is that the order service should only accept item IDs and quantities, and look up the real prices itself.
>
> Beyond that there are a few I found reading back through the code that aren't documented, and a couple of them are worse than the pricing one because they're silent.
>
> Two of them I've already fixed, and they're the ones I'd point at first if you want to know how I read my own code — both were in failure paths, which is where I wasn't looking. Group C in section 4 has both."

**LIKELY INTERRUPTION:** *"Okay, tell me the worst one."*

**HOW I ANSWER:**

> "Partial stock deduction — and it's fixed now, but it's worth walking through because of *why* it was there. In `services/restaurant/consumers.py`, if you ordered two items and the second was out of stock, the first item's stock had already been deducted before we hit the failure. I was catching that exception *inside* the transaction block, and catching a Python exception doesn't roll back Postgres — so the deduction committed along with the rejection event. Since the reservation record is only written after the whole loop succeeds, the compensation path had nothing to look up either. Every rejected multi-item order silently destroyed inventory.
>
> The fix is a savepoint around the reservation and moving the `except` out one level, so the deductions roll back while the outer transaction survives to record the rejection. What I'd actually stress is that my unit tests couldn't have caught it — they mock the connection, and a mock has no transaction semantics. So the regression test is a saga test against real Postgres, and I confirmed it by reverting the fix and watching the stock drop from 100 to 98. C2 in section 4 has the full version."

---

### CHUNK 11 — Production changes

**WHAT I SAY:**

> "If this were going to production, the order I'd fix things in is: first correctness — server-side pricing, the partial-deduction bug, and the consumer offset bug, because those three are actively wrong. Then reliability — a dead-letter queue so failed messages go somewhere instead of vanishing, and configuring the Kafka producer for `acks=all` so a broker failure can't lose a message I've already told the database was sent. Then operations — the health checks only test the database, so a service reports healthy while Kafka is down and the entire order flow is frozen. And I'd pull the consumers and outbox relays out of the API process into separate deployments, because right now scaling to handle more HTTP traffic also multiplies my Kafka consumers, which isn't what I want."

**LIKELY INTERRUPTION:** *"You mentioned running the consumer inside the API process — what actually breaks if you run three replicas?"*

**HOW I ANSWER:**

> "The outbox relay is fine — `SKIP LOCKED` handles multiple relays cleanly, that's exactly what it's for. The consumers are also fine correctness-wise because they're all in the same Kafka consumer group, so Kafka splits the partitions between them. The real problem is that I can't tune them independently. If I need ten API replicas for traffic, I get ten consumers whether I want them or not, and if my topics only have one partition, nine of them sit idle holding database connections for nothing. It works, it's just wasteful and it couples two things that should scale on different signals."

---

## 3. IF THEY ASK "WHY DID YOU DO X"

**Why Kafka instead of REST calls between services?**
> If payment is down and I call it over HTTP, the customer's order fails. With Kafka the message waits in the log until payment recovers, and the customer never notices. The tradeoff is I can't give an immediate answer, which is why the endpoint returns 202 instead of 200.

**Why the outbox table instead of publishing directly?**
> Because "commit to Postgres" and "publish to Kafka" are two separate writes with a gap between them, and a crash in that gap either loses the message or charges someone for an order that doesn't exist. Putting the message in my own database in the same transaction removes the gap entirely.

**Why `SKIP LOCKED` in the relay?**
> `FOR UPDATE` alone stops two relays from publishing the same event, but the second one would sit and wait for the first to finish. `SKIP LOCKED` tells it to move on to the next row instead, so both stay productive.

**Why `processed_events` in the same transaction as the work?**
> If I marked it processed in a separate transaction, a crash between the work and the marking means the message gets redelivered and the work runs twice. Same transaction means both happen or neither does.

**Why does payment mark the message processed *before* charging?**
> Because a crash during the gateway call leaves me not knowing whether the charge went through. Retrying risks double-charging. Not retrying risks not charging. I chose not-charging, because a stuck payment is fixable by a background job and a double charge is a refund and an angry customer.

**Why a reconciliation worker instead of just retrying?**
> Retrying is what causes the double-charge. Reconciliation looks at payments still stuck at PROCESSING after three minutes and resolves them once, with a `FOR UPDATE` re-check so two workers can't both resolve the same one.

**Why a state machine for four states?**
> Because events arrive late and out of order. Without an explicit allowed-transitions table, a delayed success event could resurrect a cancelled order and nothing would flag it.

**Why separate databases per service?**
> Because if they shared one, someone would eventually write a join across service boundaries and now the two services can never be deployed or changed independently — you'd have microservices with all the coupling of a monolith and none of the convenience.

**Why does rate limiting fail open when Redis is down?**
> Rate limiting protects the service; it isn't the service. If Redis dies and I fail closed, a cache outage takes down login for everyone. Failing open means a window with no rate limiting, which is the smaller harm.

**Why SHA-256 for refresh tokens but bcrypt for passwords?**
> Bcrypt is deliberately slow to make guessing passwords expensive — passwords are low-entropy and humans reuse them. A refresh token is 64 random bytes from `secrets.token_urlsafe`; nobody's brute-forcing that, so paying bcrypt's cost on every token lookup buys nothing.

**Why 202 instead of 200 on order creation?**
> Because 200 means "done" and it isn't done — no stock is reserved and no money has moved. 202 means "I've accepted this and I'm working on it," which is literally true, and it's why the response includes a status the client can poll.

---

## 4. BUGS AND BAD DESIGN — THE HONEST TABLE

### Group A — Intentional, documented tradeoffs (own these confidently)

| # | Issue | Why it's bad | Fix / alternative | Type |
|---|---|---|---|---|
| A1 | **Client dictates prices.** `services/order/routes.py:99` sums `item.quantity * item.price` straight from the request body. | Anyone can order anything for a penny. It's a complete authorization bypass on money. | INTENTIONAL |
| A2 | **Reconciliation retries the gateway with no idempotency key.** `reconciliation.py:45` | The whole point of the two-transaction design was to never double-charge, and the recovery path reintroduces exactly that risk. | INTENTIONAL |
| A3 | **Outbox holds a DB transaction open across the Kafka network call.** `shared/outbox.py:74-96` | A slow Kafka pins a Postgres connection. Under load the relay can exhaust the pool and take down the API sharing it. | INTENTIONAL |
| A4 | **No deterministic lock ordering in stock reservation.** `stock.py:23` | Two concurrent multi-item orders with overlapping items in different orders deadlock. | INTENTIONAL |
| A5 | **No distributed tracing.** | Request IDs exist for HTTP but don't propagate through Kafka events, so a single order's journey can't be followed across services. | INTENTIONAL |

**Full spoken answers:**

**A1 — Client-supplied pricing:**
> "It's documented but it's still the most serious flaw in the system — you can buy a burger for a penny. The fix is that the order service should never accept a price at all. The request body should only carry item IDs and quantities. Then the order service needs the real prices, and there are two ways to get them. The synchronous version: call the restaurant service, get the current menu, compute the total server-side, reject if any item doesn't exist. That's simple but it puts a network hop on the critical path and couples order to restaurant's availability. The version I'd actually build: don't price it in the order service at all. Let the order start as PENDING with no authoritative total, and let the restaurant service — which already owns the menu and is already consuming `order.placed` — compute the real total when it reserves stock, and put that number in the `stock.reserved` event. Payment charges *that* number, not the client's. The client's price becomes a display hint I can compare against and reject on mismatch, which also gives you a clean 'the price changed while you were checking out' flow. The general principle is that whoever owns the data owns the calculation."

**A2 — Gateway idempotency in reconciliation:**
> "The two-transaction design exists specifically to never double-charge, and then the recovery path blindly re-calls the gateway, which can double-charge. That's an inconsistency in my own reasoning and I'd call it out before an interviewer does. The fix is that every gateway call — the original one and the reconciliation one — passes the same idempotency key, and the payment row's UUID is the natural choice since it's already unique per order. Real gateways like Stripe take exactly this header and return the original charge instead of creating a second one. So reconciliation calling again becomes a safe read: either the charge already happened and I get the original result back, or it didn't and this call performs it. Either way, one charge. The other half is that reconciliation currently *assumes* an outcome rather than asking — it just calls `process_payment` again. What it should do is query the gateway for the charge's status by that key first, and only attempt a new charge if none exists."

**A3 — Outbox holds a transaction across the network:**
> "The relay opens a transaction, does `send_and_wait` to Kafka inside it, then commits. That means a Postgres connection is held for the entire Kafka round trip. If Kafka gets slow, connections pile up, and because the relay shares a pool with the HTTP API, a Kafka slowdown becomes an API outage. That's a bad failure coupling. There's also a smaller version of the same problem: it fetches `LIMIT 1` and loops, so a hundred pending events means a hundred separate transactions and a hundred round trips. The fix is to restructure into three phases — one short transaction that claims a batch of rows with `FOR UPDATE SKIP LOCKED` and marks them claimed, then release the connection, then publish the batch to Kafka with no transaction held, then a second short transaction to mark them published. Batch the sends instead of one at a time. Duplicates are still possible if it dies between publishing and marking, but that's already the case and consumers already handle it."

**A4 — Deadlock risk:**
> "`reserve_stock` loops over items in whatever order the client sent, and locks each with `FOR UPDATE`. Order A locks burger then wants fries; order B locks fries then wants burger. Classic deadlock — Postgres detects it and kills one transaction with a serialization error. And because of the offset-commit bug I'll get to, that killed message doesn't get retried, it just gets dropped. The fix is genuinely one line: sort the items by item ID before the loop. If every transaction always acquires locks in the same global order, a circular wait is impossible. It's the standard fix and I should have written it that way from the start. The stronger version, since this is a single `UPDATE ... WHERE quantity >= $1` away, is to skip the explicit `SELECT FOR UPDATE` entirely and let the update's own row lock plus a `RETURNING` clause tell me whether it succeeded — fewer round trips and a shorter lock window."

**A5 — No tracing:**
> "There's a request ID middleware that generates an ID per HTTP request and binds it to the structured logs, and nginx passes one through. But it stops at the HTTP boundary — it never gets into the Kafka event envelope, so once an order becomes an event I can't correlate anything. If an order gets stuck I'm grepping four services by order ID and reconstructing the timeline by hand. The cheap fix, which I'd do first, is to add `correlation_id` to the event envelope in `shared/events.py` and have every consumer bind it to its logging context — that alone makes the logs joinable. The proper fix is OpenTelemetry with context propagated in Kafka message headers, which gets you an actual waterfall view across the async hops."

---

### Group B — Straight mistakes (own briefly, then talk fix)

| # | Issue | Why it's bad | Type |
|---|---|---|---|
| B2 | **Consumer commits Kafka offsets even when processing failed.** All three consumers. | Any message that throws is marked consumed and never retried. Silent data loss. | MISTAKE |
| B3 | **Rate limiter buckets by the proxy's IP.** `auth/rate_limiter.py:34` | Every user on the planet shares one 30-req/min bucket. | MISTAKE |
| B4 | **Idempotency-Key is globally unique but looked up per-user.** `002_idempotency_key.sql` vs `routes.py:78` | Two users choosing the same key → unhandled 500. | MISTAKE |
| B5 | **Dedupe keyed on event ID, not business outcome.** `state_machine.py:18` | Reconciliation's replacement event isn't deduped and throws on a correct order. | MISTAKE |
| B6 | **Health check never checks Kafka.** `shared/health.py` | Service reports healthy while the entire saga is frozen. | MISTAKE |
| B7 | **Kafka producer uses default acks.** `shared/kafka.py:21` | README claims guaranteed delivery; config doesn't provide it. | MISTAKE |
| B8 | **Two Prometheus metrics declared, never incremented.** `shared/metrics.py:13,19` | The HTTP metrics the README advertises are permanently zero. | MISTAKE |
| B9 | **SQL built with an f-string.** `reconciliation.py:32` | Parameterization violation, even though the input is internal today. | MISTAKE |
| B10 | **Migrations re-run on every boot with no version ledger.** `services/*/db.py:41` | Works only because every statement is `IF NOT EXISTS`. First real migration breaks it. | MISTAKE |
| B11 | **Auth register is check-then-insert.** `auth/routes.py:45-51` | Concurrent signups with the same email → 500 instead of 409. | MISTAKE |
| B12 | **`processed_events` grows forever; JWT secret is shared symmetric; no password policy.** | Unbounded table, any service can mint tokens, `"a"` is a valid password. | MISTAKE |
| B13 | **Unit tests use hand-rolled mock connections.** | They can't observe transaction semantics — which is exactly why C2 survived as long as it did. | MISTAKE |

**Full spoken answers:**

**B2 — Offsets committed on failure:**
> "This one is a mistake and I'm not going to dress it up — there's no tradeoff story here, I just wrote it wrong. In all three consumers, the loop processes each message inside a try/except that logs the error and continues. Then, *after* the loop, outside the try/except, there's `if result: await consumer.commit()`. So the offset commit happens whether processing succeeded or not. Any message that threw an exception is now marked as consumed, and Kafka will never redeliver it. It's gone.
>
> It's worse than it sounds because of what it interacts with. A deadlock from A4 gets dropped. A transient database blip gets dropped. Any bug I fix tomorrow still loses every message that hit it today. And the comments give it away twice — the line directly above the commit says 'Commit offsets for all successfully processed messages,' and the docstring in `shared/kafka.py` says I use manual commit 'so consumers only commit after successful processing.' Both describe the correct behavior; neither is what the code does. There's also no dead-letter queue anywhere in the project, so a failed message has nowhere to go even if I noticed.
>
> The fix has two parts. First, the commit has to be conditional on success — I track the last successfully processed offset per partition and commit only up to that point, so anything after a failure gets redelivered. Second, retrying forever isn't right either, because a genuinely malformed message would block that partition permanently — that's a poison message. So: retry a bounded number of times, and if it still fails, publish it to a dead-letter topic with the error and the original payload, *then* commit past it. That way the partition keeps moving, nothing is silently lost, and there's a queue I can alert on and replay from. I'd also add an alert on the existing `kafka_messages_consumed_total{status="error"}` counter, which today increments into the void."

**B3 — Rate limiter sees the wrong IP:**
> "The rate limiter builds its Redis key from `request.client.host`. In production every request arrives through nginx, so `request.client.host` is always the nginx container's IP — the same value for every user in the world. That means the 30-requests-per-minute limit is a global limit across all users, not per-user. Two people logging in at once eat into each other's budget, and thirty logins a minute across the entire platform starts returning 429 to everyone. It also means the thing it's supposed to prevent — one attacker brute-forcing logins — instead just denies service to everybody else.
>
> The irony is nginx is already sending the right value: the config sets `X-Real-IP` on every proxied request. I just never read it. The fix is to read `X-Real-IP`, or the first entry in `X-Forwarded-For`, and fall back to `request.client.host`. The important caveat is that a client can forge those headers, so you only trust them when the request actually came from your proxy — in FastAPI you'd configure `ProxyHeadersMiddleware` with the trusted proxy IPs rather than parsing headers by hand. Separately, I'd rate-limit login attempts by email as well as by IP, because IP-based limiting alone doesn't stop a distributed attempt against one account."

**B4 — Idempotency key scoping:**
> "The migration declares `idempotency_key VARCHAR(255) UNIQUE`, which makes the key unique across the entire orders table. But the lookup in the route is `WHERE idempotency_key = $1 AND user_id = $2`. Those two disagree. If user A uses the key `abc` and user B also uses `abc` — which is entirely plausible since clients pick these — user B's lookup finds nothing because the user_id doesn't match, so the code proceeds to insert, and the insert hits the global unique constraint. That's an unhandled `UniqueViolationError`, which surfaces as a 500. So a completely valid request from user B fails with a server error because of something user A did.
>
> The fix is to make the constraint match the semantics: drop the column-level `UNIQUE` and add a composite `UNIQUE (user_id, idempotency_key)`. Idempotency keys are only meaningful within one user's scope, so that's what the database should enforce. Second, even with the right constraint, the check-then-insert is a race — two concurrent retries of the same request can both pass the SELECT before either inserts. So the insert needs to handle the violation rather than just avoid it: catch the unique violation, re-read the existing order, and return that. Then the constraint is the actual source of truth and the SELECT is just a fast path. I'd also store the request body's hash alongside the key, so if someone reuses a key with a different payload I can return a 422 instead of silently handing back an unrelated order."

**B5 — Dedupe on event ID rather than outcome:**
> "Idempotency is keyed on `event_id`, which is generated fresh by `wrap_event` for each event. That's correct for redelivery of the *same* message. It doesn't cover semantically duplicate messages with different IDs — and I have a path that produces exactly those. If a payment gets stuck and the reconciliation worker resolves it, the worker publishes a brand new `payment.succeeded` with a new event ID. If the original one had also eventually made it through, the order service now sees two logically identical events that don't dedupe. The second one calls `transition_order` on an order that's already CONFIRMED, the allowed-transitions table has no entry for CONFIRMED, so it raises `InvalidTransitionError`. The final state is correct, but it logs an exception and burns an error counter on a healthy order — so my error metrics have false positives, which is arguably worse than a quiet bug because it trains you to ignore alerts.
>
> Two fixes, and they're complementary. The narrow one is to make the state machine treat a transition into the state you're already in as a no-op rather than an error — CONFIRMED to CONFIRMED is not a violation, it's a duplicate. The real one is to dedupe on business identity instead of message identity: a unique constraint on `(order_id, event_type)` in `processed_events`, so 'this order's payment succeeded' can only be recorded once regardless of how many messages carry it. And the reconciliation worker itself should carry the original event's ID forward instead of minting a new one, so the two are recognizably the same fact."

**B6 — Health check is incomplete:**
> "`create_health_router` only runs `SELECT 1` against Postgres. But order, restaurant, and payment all depend on Kafka just as hard — if Kafka is down, the API happily accepts orders, writes them to the outbox, and nothing ever processes them. Every health check stays green while the entire product is broken. In Kubernetes that's actively harmful: the readiness probe passes, traffic keeps routing to a service that can't do its job, and there's no signal anywhere.
>
> The fix is for services that have a Kafka client to include its state in the check — the producer and consumer both expose enough to tell whether they're connected to a broker, and I'd surface that as a separate key in the response so you can see which dependency is the problem. The nuance is that readiness and liveness want different answers: liveness should only fail if the process itself is wedged, because restarting won't fix a Kafka outage and you don't want a crash-loop. Readiness should fail so traffic stops. So I'd split into `/health/live` — process is up — and `/health/ready` — dependencies are reachable. I'd also add outbox lag, the count of rows with `published_at IS NULL` older than some threshold, since that's the number that actually tells you the pipeline has stalled."

**B7 — Producer durability settings:**
> "The producer in `shared/kafka.py` sets a serializer and nothing else, so it takes aiokafka's defaults. The default is `acks=1`, meaning the leader broker acknowledges the write before it's replicated. If that leader dies before replication, the message is gone — but my relay already got its acknowledgment and marked the row published, so my database says 'sent' and the event does not exist. That's silent message loss, and the README describes this system as providing guaranteed at-least-once delivery, which with these settings isn't quite true. It doesn't bite in this project because the compose file runs a single broker with replication factor one, so there's nothing to fail over to anyway — but that's an accident of the dev setup, not a design.
>
> The fix is `acks="all"` so every in-sync replica confirms before the send returns, `enable_idempotence=True` so a producer-side retry can't write the same message twice, and explicit retry and timeout settings rather than defaults. On the broker side, topics need `min.insync.replicas=2` with replication factor 3, otherwise `acks=all` on a single-replica topic means the same thing as `acks=1`. It's a config change, not a code change, which is exactly the kind of thing that's easy to leave as a default and then be wrong about in a README."

**B8 — Dead metrics:**
> "`shared/metrics.py` declares `http_requests_total` and `http_request_duration_seconds`, and nothing in the codebase ever increments them. They're exported on `/metrics` as permanently zero. Meanwhile the request middleware already computes the exact duration I'd want and just writes it to a log line. So I have the data and I throw it away. The README advertises Prometheus metrics, which is technically true — two Kafka and outbox counters do work — but the HTTP ones are decoration. There's also no Prometheus server in the compose file, so nothing is scraping any of it.
>
> The fix is small: increment both in `RequestIdMiddleware`, which already sits in the right place and already has the method, path, status, and elapsed time. The one thing to be careful about is the label cardinality — labelling by raw path means every order UUID becomes its own metric series and Prometheus falls over. You want the route *template*, `/orders/{order_id}`, which Starlette exposes on the matched route. Then add a Prometheus service to compose so it's actually collected, and honestly the dashboard I'd care about most isn't HTTP latency at all — it's consumer lag and outbox backlog, because those are what tell you the saga is stuck."

**B9 — f-string in SQL:**
> "`reconcile_stuck_payments` builds its query with an f-string to interpolate the interval: `INTERVAL '{stuck_duration}'`. Today that value is a default argument I control, so it isn't exploitable. But it's a parameterized-query violation sitting in the codebase, and the only thing standing between it and an injection is that nobody has yet wired a config value or an admin endpoint into that parameter. That's exactly how these become real. Everywhere else in the project uses `$1` placeholders properly, so this is an inconsistency, not a pattern.
>
> The fix is to pass it as a parameter — `updated_at < NOW() - $1::interval` and hand asyncpg the value, or take an integer of minutes and use `make_interval`. Either way the value never gets concatenated into SQL text. I'd also grep for the same shape elsewhere; the test helper `poll_for_status` interpolates table and column names the same way, which is fine for a test fixture but worth knowing about so nobody copies the pattern into service code."

**B10 — Migrations:**
> "Each service's `create_pool` globs its migrations directory and executes every `.sql` file on every single startup. There's no tracking table, so migration 001 runs again every time the container boots. It works today purely because every statement is `CREATE TABLE IF NOT EXISTS` or `ADD COLUMN IF NOT EXISTS`. The moment I write a migration that isn't idempotent — an `INSERT`, a data backfill, a `DROP COLUMN` — restarting the service either errors or corrupts data. It's also racy: with multiple replicas, several instances run DDL against the same database simultaneously.
>
> The fix is a real migration tool. Alembic is the standard choice for Python and Postgres — it keeps a `alembic_version` table, applies only what hasn't run, and gives you downgrade paths. If I wanted to keep the lightweight approach, the minimum viable version is a `schema_migrations` table recording each applied filename, wrapped in a transaction with an advisory lock so only one instance applies at a time. Beyond that, running migrations from inside the app process at startup is itself the wrong shape — it should be an init container or a deploy step, so a schema change is a deliberate action rather than a side effect of a pod restarting."

**B11 — Register race:**
> "Registration does a `SELECT` to check whether the email exists, and inserts if it doesn't. Between those two statements another request can insert the same email. The second insert then violates the unique constraint on `users.email`, which nothing catches, so the user gets a 500 instead of the 409 the code was clearly trying to produce. It's a narrow window and unlikely in practice, but it's the same check-then-act shape as the idempotency bug, which suggests it's a habit rather than a one-off.
>
> The fix is to let the database be the authority instead of pre-checking: `INSERT ... ON CONFLICT (email) DO NOTHING RETURNING id`, and if nothing comes back, return 409. One round trip, no window, and the constraint does the work it already exists to do. The general rule I'd take from both of these is that if a database constraint enforces something, the application shouldn't try to enforce it separately — it should handle the constraint violation."

**B12 — The smaller ones, together:**
> "Three things I'd group. First, `processed_events` never gets pruned. Every message every service ever handles leaves a row forever. It's the right design for correctness but it needs a retention policy — the entries only need to outlive Kafka's retention window, since a message can't be redelivered after it's aged out of the log. So a daily job deleting rows older than the retention period, or monthly partitions you can drop wholesale.
>
> Second, JWTs use HS256 with the same secret shared to auth and order via environment variables. HS256 is symmetric, so the key that verifies is the key that signs — any service holding it can mint valid tokens for any user, and the blast radius of leaking it from any one service is total. The fix is RS256: auth holds the private key and signs, everyone else gets the public key and can only verify. Then compromising the order service doesn't let you forge logins. The secret also has a hardcoded fallback default in the source, which should just fail to start if unset rather than silently running with a known key.
>
> Third, the register model is `password: str` with no constraints, so a single character is a valid password. That's a `Field(min_length=12)` on the Pydantic model, plus ideally a check against a breached-password list. Small fix, and the kind of thing that's embarrassing to have missed rather than hard."

**B13 — The tests:**
> "This is the one I'd volunteer without being asked, because it explains the others. The unit tests for stock, reconciliation, and the state machine all define their own `MockConnection` class with `fetchval` and `execute` methods that append to lists. That means they test my Python control flow and nothing else — a mock connection has no transaction semantics, so a test can pass while the real code commits a partial write. That's precisely how C2 survived: `test_insufficient_stock` passes a single item, asserts the exception is raised, and by construction cannot observe that a multi-item order would have committed a partial deduction. Its regression test had to be a saga test against real Postgres, which is the pattern I'd apply to the rest of them.
>
> The saga tests are the good ones — they use testcontainers to spin up real Postgres and real Kafka, run the actual consumers, and assert on real database state. Those caught real behavior. So the fix is to move this class of logic down into that layer: a test that orders two items where the second is short, and asserts stock for the first is unchanged. That test fails against the current code, which is the point. More broadly I'd stop writing mock-connection tests for anything that touches a transaction — if the correctness depends on database behavior, the test needs a database. Mocks are fine for the gateway simulator, which is pure logic."

---

### Group C — Found and fixed (lead with this one)

| # | Issue | Why it was bad | Type |
|---|---|---|---|
| C1 | **Reconciliation lost the `payment_simulate` directive.** `reconciliation.py` passed `None` to the gateway. | The fallback to `PAYMENT_SIMULATE_DEFAULT` ("success") meant any payment stranded by a crash resolved to SUCCEEDED — including orders explicitly meant to fail. The recovery path silently inverted the business outcome. | FIXED |
| C2 | **Partial stock deduction on multi-item rejection.** `restaurant/consumers.py` caught `InsufficientStockError` inside the transaction. | Every rejected multi-item order permanently destroyed inventory, silently, with no reservation record for compensation to release. | FIXED |

**C1 — The directive the recovery path threw away:**
> "This is the one I found by reading my own failure path instead of my happy path, and it's the bug I'd most want to be asked about.
>
> The `payment_simulate` directive rides in on the `stock.reserved` event and decides whether the charge succeeds. The consumer read it into a local variable, used it, and dropped it — it was never persisted. So when reconciliation later picked up a payment stranded at PROCESSING, it had no directive to work with. The code passed `None`, which falls back to `PAYMENT_SIMULATE_DEFAULT`, which is `success`. An order that was explicitly supposed to fail would be recovered as SUCCEEDED, an event would be published saying so, and the rest of the saga would confirm an order that should have been rejected. There's even a comment in the old code admitting it: *'Since we lost the simulate directive in the DB, we pass None to use the default.'* I'd written down that I was losing information and moved on.
>
> What makes it a good bug is that nothing catches it. The happy path is fine. The failure path is fine — `test_saga_payment_failed_compensation` passes, because the *consumer* still has the directive in memory. It only goes wrong in the seam between them: crash mid-gateway, and recovery quietly produces the opposite answer.
>
> The fix was small — a migration adding `payment_simulate` to `payments`, write it in Transaction 1, read it back in reconciliation — plus a regression test that asserts a stuck payment carrying `failure` resolves to FAILED and publishes `payment.failed`.
>
> The deeper lesson is that Transaction 1's job isn't just recording intent for idempotency. It's the last moment before the un-rollbackable side effect, so it has to persist *everything recovery will need*. I was thinking of it as a lock, not as a checkpoint."

**Where C1 sits relative to A2:** they're the same seam from two directions. C1 was recovery producing the *wrong* answer; A2 is recovery being *unsafe to ask* in the first place. Fixing C1 makes recovery correct in this simulation; A2's idempotency-key lookup is what makes it correct against a real gateway.

**C2 — Partial stock deduction:**
> "This was the worst bug in the codebase, and it's a genuine mistake — no tradeoff story.
>
> In `reserve_stock` I loop over items, and for each one I check stock under `FOR UPDATE` and then deduct it. If item three is short, I raise `InsufficientStockError`. But I was catching that exception *inside* the `async with conn.transaction()` block. Catching a Python exception doesn't roll back a Postgres transaction — the transaction only rolls back if the exception escapes the context manager. So items one and two were already decremented, I swallowed the error, wrote a `stock.rejected` event in that same transaction, and committed all of it together. The customer got correctly rejected, and two items of inventory evaporated. Worse, I only write the `stock_reservations` row *after* the whole loop succeeds, so on the reject path there was no reservation record at all — `release_stock` looks the order up by ID, finds nothing, logs a warning and returns. And `stock.release` is only ever published on `payment.failed`, so on a stock rejection compensation isn't even attempted. Four separate reasons that inventory was never coming back.
>
> The fix is smaller than the explanation. I wrap the reservation in a savepoint — in asyncpg a nested `conn.transaction()` *is* a savepoint. The error escapes the inner block, the savepoint rolls back the deductions, and the outer transaction is still alive to write the rejection event and commit. One added line, and the `except` moved out one level. The critical detail is that the `except` has to sit *outside* the nested block: catching it inside is the original bug, just relocated. There's a comment in the code saying exactly that, because it's the kind of thing someone helpfully 'tidies' back into place.
>
> What I'd still do on top: validate all items before mutating any of them — two passes, one to check everything under lock, one to deduct. The savepoint *repairs* a partial write; two-pass never creates one. And sort the items by ID before locking, which fixes the deadlock in A4 at the same time, since it's the same loop.
>
> The part I'd actually want to talk about is why it survived. This is a transaction-semantics bug, and my unit tests mock the connection with a dict that has no transactions — they could not have caught it in principle. The regression test for it is a saga test against real Postgres: order two items where the second is short, assert the first item's stock is unchanged. I verified it the honest way, by reverting the fix and watching stock go from 100 to 98. That's B13 in practice, not in theory."

---

## 5. QUESTIONS I CAN'T DODGE

**1. "What happens if the same order.placed event is delivered twice?"**
> Each consumer inserts the event ID into `processed_events` in the same transaction as the business logic, so the second delivery sees the row and skips. There's a test that publishes the same event ID twice and asserts stock only moves once. What it does *not* cover is two different events that mean the same thing — my reconciliation worker generates a fresh event ID for a replacement `payment.succeeded`, and that isn't deduped. I'd fix that by keying on `(order_id, event_type)` rather than on the message ID.

**2. "A consumer throws an exception on a message. Walk me through what happens."**
> It logs the error, increments an error counter, and then — this is a bug — commits the offset anyway, because the commit sits outside the try/except. The message is marked consumed and never comes back. That's silent data loss and there's no dead-letter queue for it to land in. The fix is to commit only up to the last successful offset so the failure gets redelivered, with a bounded retry count and a DLQ topic for poison messages so one bad message can't block a partition forever.

**3. "How do you know you're not overselling stock?"**
> `SELECT ... FOR UPDATE` on each stock row inside the transaction, so a concurrent order blocks until the first commits and then reads the updated number. There's a test that sets stock to 1, fires five concurrent orders, and asserts exactly one confirms and final stock is zero. One caveat I'd raise myself: there's still no deterministic lock ordering, so two multi-item orders with overlapping items can deadlock. The related bug — a rejected multi-item order committing partial deductions — is fixed; the reservation now runs in a savepoint, and there's a saga test asserting the first item's stock is untouched when a later one is short.

**4. "What if the payment service crashes mid-charge?"**
> The message was marked processed before the gateway call, deliberately, so it won't be redelivered and can't double-charge. The payment sits at PROCESSING. The reconciliation worker sweeps every 60 seconds for payments stuck longer than three minutes and resolves them, re-checking status under `FOR UPDATE` so two workers can't both resolve it. The honest gap is that reconciliation calls the gateway again with no idempotency key, which reintroduces the double-charge risk the design existed to prevent — the fix is passing the payment ID as an idempotency key on every gateway call, and querying the charge's status before attempting a new one.

**5. "Your README says the outbox guarantees delivery. Does it?"**
> Not fully, and that's a claim I'd soften. The outbox guarantees the *intent* is durable — the event can't be lost between my database commit and the send attempt, because it's in the same transaction. What it doesn't guarantee is that Kafka keeps it, because the producer runs with default `acks=1`, so a leader failure after acknowledgment loses a message my database has already marked published. It doesn't manifest here because there's one broker and nothing to fail over to. To actually earn that sentence I need `acks=all`, an idempotent producer, and `min.insync.replicas=2` on multi-replica topics.

**6. "Can I place an order for a burger for one cent?"**
> Yes. The order service takes the price from the request body and never checks it against the menu. It's documented as a known limitation but it's still a complete authorization bypass on money and I'd rank it as the most serious flaw. The fix is that the client sends only item IDs and quantities, and the restaurant service — which owns the menu and already consumes `order.placed` — computes the authoritative total and puts it in `stock.reserved`, which is what payment charges against.

**7. "Why is the outbox relay running inside your API process?"**
> Convenience, and I'd change it. Correctness-wise it's fine — `SKIP LOCKED` means multiple relays coexist safely and the consumers share a consumer group so Kafka partitions the work. The problem is coupling: scaling the API for HTTP traffic multiplies my consumers whether I want that or not, and if a topic has one partition the extra consumers idle while holding database connections. They should be separate deployments scaled on different signals. There's also a shared-pool risk — the relay holds a connection during the Kafka round trip, so slow Kafka can starve the API of connections.

**8. "You have Prometheus metrics — what would you actually alert on?"**
> Right now, honestly, less than the README implies: the two HTTP metrics are declared and never incremented, so they're permanently zero, and there's no Prometheus server in the compose file to scrape anything. What I'd actually alert on, once wired up: outbox backlog — rows with `published_at IS NULL` older than a minute, which is the single best signal that the pipeline has stalled; consumer lag per topic; the error-status consumer counter, which today increments into the void; and payments sitting at PROCESSING beyond the reconciliation window, since that means the recovery path itself is failing.

**9. "Your health checks pass. Does that mean the system works?"**
> No, and that's a real weakness. The health check only runs `SELECT 1` against Postgres. If Kafka is down, every service reports healthy while the entire order flow is frozen — orders get accepted and nothing ever processes them. In Kubernetes that's worse than useless because readiness stays green and traffic keeps flowing to a service that can't do its job. I'd split liveness from readiness, include broker connectivity in readiness, and add outbox lag as a check, since it catches stalls that a connectivity ping wouldn't.

**10. "How much of this did you write yourself?"**
> I used AI assistance heavily for the implementation. What I own is the architecture and the failure analysis — the two-transaction payment split and why it fails toward not-charging, the outbox and what specifically breaks without it, the reconciliation worker. What that process cost me is visible in the bugs: the partial-stock-deduction bug and the offset-commit bug are both cases where the code reads correctly and behaves wrong, and my tests couldn't catch them because they mock the database connection. I found those by reading the code line by line afterward, which is the part I'd do earlier next time.

*(Answer 10 only if asked directly. Don't volunteer it. But have it ready and delivered calmly — the specificity of the bugs you found is the proof that you understand the code, and it lands far better than a denial.)*

---

## 6. WORDS I CAN'T SAY WITHOUT EXPLAINING

Say the plain sentence the **first** time the word comes out of your mouth. After that you can use the term freely.

| Term | Say this the first time |
|---|---|
| **Saga** | "A chain of small transactions where each step has an undo step — if step three fails you run the undos for two and one, because there's no single transaction to roll back." |
| **Transactional outbox** | "Instead of writing to my database and then publishing to Kafka as two separate steps, I write the message into my own database in the same transaction as the data — so there's no gap where a crash loses it." |
| **Idempotent / idempotency** | "Doing the same thing twice has the same effect as doing it once — so if a message gets delivered twice, the second one changes nothing." |
| **Compensating transaction** | "The undo step. You can't roll back a committed transaction in another service, so you run a second transaction that reverses it." |
| **Eventual consistency** | "For a short window different services disagree — the order says pending while the stock is already reserved — and they catch up within a second or two." |
| **At-least-once delivery** | "The message is guaranteed to arrive, but it might arrive more than once, so the receiver has to cope with duplicates." |
| **Exactly-once processing** | "Messages may arrive twice, but the *effect* only happens once — I get there by recording every message ID I've handled." |
| **Choreography (vs orchestration)** | "There's no central coordinator telling services what to do. Each one reacts to events on its own — like dancers who know the routine, versus a conductor calling the moves." |
| **Dead-letter queue** | "A separate topic where messages go when they've failed too many times, so they're parked somewhere you can inspect and replay instead of blocking everything behind them." |
| **`SELECT ... FOR UPDATE`** | "It locks that row until my transaction finishes, so nobody else can read-and-modify it at the same time and overwrite my change." |
| **`SKIP LOCKED`** | "If a row is already locked by someone else, don't wait for it — skip it and take the next one." |
| **Consumer group** | "A set of workers sharing a topic, where Kafka makes sure each message goes to exactly one of them, so adding workers splits the load instead of duplicating it." |
| **Offset** | "A bookmark. It's how far a consumer has read in the log — committing the offset says 'I'm done up to here, don't send me these again.'" |
| **Two-phase commit (2PC)** | "The alternative to sagas — a coordinator makes every service promise to commit, then tells them all to go. It gives you real atomicity but everyone holds locks while waiting, and if the coordinator dies mid-vote everyone is stuck." |
| **Bounded context** | "A hard line around one service's data. The restaurant service owns stock and nobody else can read that table — they have to ask." |
| **Fail open** | "When the protective thing breaks, let traffic through rather than blocking it — a Redis outage disabling rate limiting rather than disabling login." |
| **Reconciliation** | "A background sweep that finds things stuck halfway and finishes them — because in a distributed system some percentage of operations will always end up in limbo." |

---

## LAST THING BEFORE YOU WALK IN

Three sentences to keep in your pocket, in priority order:

1. **"I know about that one."** — For the documented limitations. Say it flatly, then give the fix. Confidence comes from having the fix ready, not from the bug not existing.
2. **"That's a real bug, and here's what's wrong with it."** — For B2, and for the Group C pair (C1, C2) where you can say you found it, fixed it, and wrote the test that proves it. Do not reach for a justification. Naming your own bug precisely, and immediately describing the fix, is a stronger signal than a clean codebase would be.
3. **"I don't know, but here's how I'd find out."** — For anything genuinely outside what you built. Never guess at a mechanism. Say what you'd measure or read to answer it.

The thing being tested isn't whether the code is perfect. It's whether you can be handed a system, reason about how it fails, and be honest about what you don't know. You now have all three.
