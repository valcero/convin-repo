# Webhook Ingestion Service: 

## What Was Broken 

I found four really tricky bugs hiding in the service. Here's what was going wrong:

1. **The Server Was Ghosting Active Jobs (Graceful Shutdown):** 
   Whenever we shut down the server, it just killed everything instantly! If a recording was still processing in the background, it just died. 
   * **Fix:** I added a `sync.WaitGroup` to the `Service` struct. Now, before the app exits in `main.go`, it actually waits politely for all the background goroutines to finish their work.

2. **The "Silent Black Hole" (Context Cancellation):** 
   I noticed that the background recording tasks were just silently failing. It turns out, we were passing the HTTP request context directly into the background goroutine. So, the second the HTTP handler returned a `202 Accepted` to the client, the context was cancelled, which instantly aborted our background database queries. Plus, we were ignoring the errors! 
   * **Fix:** I used the new `context.WithoutCancel(ctx)` so the background task gets its own independent lifecycle, and I added proper `s.log.Error` logging so we aren't flying blind anymore.

3. **Race Condition in Stats (Data Drifting):** 
   Under heavy load, our in-memory cache totals were drifting way off. I wrote a massive concurrent test with 100 goroutines and watched it fail spectacularly. The issue was a classic Go data race: we were reading and writing to a shared map and updating struct fields concurrently without locking them. (Thankfully, our database `ON CONFLICT` updates were perfectly safe!).
   * **Fix:** I added a simple `sync.RWMutex` to the cache `Record` method.

4. **The Clone Invasion (Lack of Idempotency):** 
   The `EventExists` check was using a Read-Modify-Write pattern. If two identical webhooks hit the service at the exact same millisecond, they both checked the DB, both saw the event didn't exist, and both inserted a duplicate! 
   * **Fix:** I implemented a two-part deduplication strategy (details below!).

## Why I Chose Redis `SETNX` + Postgres Unique Constraint

To fix the idempotency bug, I decided to go with a "belt and suspenders" approach to make it bulletproof:

1. **Redis `SETNX` (The Fast Path):** I added a Redis distributed lock right at the start of the `Ingest` function. By calling `SETNX` with a 24-hour TTL using the `event_id`, I can instantly reject duplicate payloads in memory. This saves our database from having to do unnecessary work.
2. **Postgres `UNIQUE` Constraint (The Ultimate Backup):** What if Redis goes down, gets flushed, or restarts? To be absolutely certain we never get duplicate data, I wrote a migration (`002_add_unique_event_id.sql`) to add a `UNIQUE` constraint to the `event_id` column in our Postgres `events` table. Even if the Redis lock fails, the database will catch the duplicate and throw an error. 

## Scaling to 10,000 Webhooks/Second 🌐

Right now, the service is working great, but if we get hit with 10k requests a second, our database connection pool is going to be completely overwhelmed. To handle that kind of massive scale, here is my proposed architecture change:

* **Decouple Ingestion from Processing:** Instead of launching inline goroutines, the HTTP handler should just instantly dump the payload onto a message broker like Kafka or RabbitMQ and return a `202 Accepted`. We can then spin up dedicated consumer worker services to pull off the queue at their own pace.
* **Batch Database Writes:** Instead of inserting rows one by one, our new consumer workers should gather events in memory and do bulk inserts (e.g., writing 1,000 records at once every second). This will reduce database contention drastically.
* **Distributed Rate Limiting:** Since we already have Redis set up, we should use it to build a sliding-window rate limiter on the ingest API. This way, if a single client goes rogue, we can throttle them before they take down the whole service.

I made sure to add tests for everything before writing the code, and the test suite is currently 100% green.
