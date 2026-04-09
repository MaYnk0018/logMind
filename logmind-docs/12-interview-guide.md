# 12 — Interview Guide: All 7 Phases

> 25 questions interviewers WILL ask. Each answer is 2–3 sentences using precise technical terms.
> Memorise the bolded lines — they're the ones that land.

---

## Phase 1 — Monolith & Database

**Q: Why partition the logs table by month?**

`ALTER TABLE logs DROP PARTITION p202501` is an O(1) metadata operation — it drops the
file pointer, not the rows. A `DELETE WHERE timestamp < '2025-02-01'` on an unpartitioned
100M-row table would take hours. **The partition key `YEAR * 100 + MONTH` maps each month
to a unique integer range that MySQL can skip entirely during time-range queries.**

---

**Q: Why the composite index (service_id, timestamp)?**

The most common query is "all logs for service X in time range Y". The composite index
lets MySQL perform one B-tree range scan using both predicates. **Without it, MySQL would
full-scan the table or use two separate indexes and merge them — both worse than one.**

---

**Q: Why Z-score instead of a fixed error threshold?**

A fixed threshold (e.g. 50 errors/min) fails because services have different baselines.
**Z-score is relative to each service's own rolling mean — `payment-service` spiking
from 50 to 60 errors/min has Z ≈ 0.5 (fine), while `auth-service` spiking from 0 to 5
errors/min has Z >> 2.5 (critical).** A fixed threshold cannot distinguish these.

---

**Q: Why require 20 data points before scoring (cold start guard)?**

Without the minimum, the first bucket with 1 error against a mean of 0 gives Z = infinity
(division by near-zero stddev). **20 buckets = 20 minutes of baseline — enough for the
mean and stddev to stabilize.** Before that, the Z-score is mathematically meaningless.

---

**Q: Why `@Transactional(readOnly=true)` on query methods?**

`readOnly=true` tells Hibernate to skip dirty checking — no need to snapshot entity state
for comparison on flush. It also signals the connection pool that read replicas can be
used if configured. **It is a free optimization with no downside for read-only operations.**

---

**Q: Why use `VARCHAR(36)` for UUIDs instead of `BINARY(16)`?**

`VARCHAR(36)` is human-readable in logs, queries, and debugging. The 20-byte storage
overhead per row is acceptable at Phase 1 scale. **In Phase 7 hardening this can be
migrated to `BINARY(16)` for a significant storage and index size improvement.**

---

## Phase 2 — Kafka

**Q: Why Kafka over RabbitMQ or a database queue?**

**Kafka retains messages for replay — if the storage consumer is down for 2 hours,
messages queue in Kafka and are processed when the consumer recovers.** RabbitMQ deletes
messages after acknowledgement — those 2 hours of logs are lost. Database queues
(polling a table) are slower and add write load to the main DB.

---

**Q: Why manual offset commit instead of auto-commit?**

With auto-commit, the offset advances before processing. If the DB write fails after
the commit, the message is permanently lost. **Manual commit (our choice) means the offset
only advances after a successful `logRepository.saveAll()` — at-least-once delivery.**
Duplicates are handled by `INSERT IGNORE` with the `log_id` idempotency key.

---

**Q: What is the Phase 2 change surface? What stays the same?**

**One method changes**: `LogIngestionService.ingestBatch()` replaces `logRepository.saveAll()`
with `kafkaTemplate.send()`. One new file: `RawLogConsumer`. Zero changes to: controllers,
DTOs, entities, repositories, `ServiceRegistry`, `WindowState`, `SlidingWindowAnalyzer`,
`GlobalExceptionHandler`, or any tests.

---

**Q: Why does `LogBatcher` have two flush triggers (size AND timer)?**

Size-only: at low traffic, 10 logs could sit in the buffer indefinitely waiting for the
100-log threshold. Timer-only: at high traffic, 10,000 logs could accumulate before the
500ms fires. **Both triggers together guarantee at most 100 logs OR 500ms delay —
bounded in both directions regardless of traffic volume.**

---

**Q: Why key Kafka messages by `serviceName`?**

Kafka's hash partitioner assigns `hash(key) % partitions` — consistent per key.
**All logs from `payment-service` always land on the same partition, giving ordering
guarantees within a service.** The anomaly detector relies on ordered delivery to maintain
correct window state per service.

---

## Phase 3 — Anomaly Detection Microservice

**Q: Why can't you scale the anomaly detection service beyond the partition count?**

`WindowState` is in-memory per service. If two instances both consume logs for
`payment-service`, each has half the data — Z-scores are computed on 50% of observations.
**Kafka partition assignment guarantees each partition is owned by exactly one consumer
instance in a group, so all `payment-service` logs go to one instance with the full
`WindowState`.**

---

**Q: Why population stddev instead of sample stddev?**

We have the full population of observations in the window — all 60 buckets.
Population stddev uses the N denominator; sample stddev uses N-1. **N-1 overestimates
variance when the entire population is observed, which is our case.** Sample stddev is
for when you're drawing a sample from a larger unknown population.

---

## Phase 4 — AI Layer

**Q: Why structure the prompt statistics first, logs second, JSON schema last?**

Language models weight earlier tokens more heavily in their context window.
**Quantified statistics (Z-score, baseline, severity) first give the model a numerical
frame before reading raw text. The JSON schema instruction last is closest to where
generation begins — the model is most likely to follow instructions it just read.**

---

**Q: Why store `raw_prompt` and `raw_response` in the database?**

**For debugging and re-analysis.** If a hypothesis is rated thumbs-down, you can query
the exact prompt sent to the model and the exact response received. This also enables
prompt A/B testing — run the same anomaly through two prompt versions and compare
feedback scores to determine which version is more accurate.

---

**Q: Why fingerprint-based RAG instead of real vector embeddings?**

MySQL lacks native vector search. The fingerprint approach — normalizing error message
patterns, severity, time-of-day, day-of-week — is explainable, fast for O(hundreds) of
past incidents, and demonstrates the RAG concept clearly. **In production I'd replace
`IncidentMemory` with a pgvector or Pinecone implementation behind the same interface
— the `AnomalyConsumer` wouldn't change.**

---

**Q: What is the eval loop and why do companies care about it?**

The `incident_feedback` table stores `rating = +1 or -1` per hypothesis. A daily query
computes `AVG(CASE WHEN rating=1 THEN 1 ELSE 0 END)` as accuracy over 30 days.
**AI companies care because it proves you think about measuring AI quality, not just
deploying it. A declining accuracy trend is a signal to retune the prompt or switch
the model — without the loop, you'd never know.**

---

**Q: Why cap output tokens at 800?**

A 2–4 sentence hypothesis plus three JSON arrays (affected components, suggested actions,
similar incident IDs) comfortably fits in 800 tokens. **Uncapped output risks runaway
generation — Claude occasionally produces very long responses when token budget is
unlimited. The cap makes cost predictable and response time consistent.**

---

## Phase 5 — Alerting

**Q: How do you prevent alert storm from a single spike?**

Two dedup layers: `AnomalyConsumer.isDuplicate()` skips the Claude call if the same
`serviceId + severity` was seen within 5 minutes. `AlertEvaluator.isRecentlyFired()`
checks `alert_events` for the same `ruleId + serviceId` within 5 minutes.
**One spike produces at most one Claude call and one notification per rule per 5 minutes.**

---

**Q: Why write the alert event with `delivered=false` BEFORE dispatching?**

If the service crashes between a successful dispatch and the `delivered=true` update,
the event is recorded but with `delivered=false`. **An admin can query
`SELECT * FROM alert_events WHERE delivered=false` to find undelivered alerts and retry
manually.** Without this pattern, the crash would be completely silent.

---

## Phase 7 — Production Hardening

**Q: How does the circuit breaker protect against Claude API failures?**

Resilience4j tracks the last 10 calls. If ≥50% fail, the circuit opens — all subsequent
calls immediately return the fallback (queue anomaly for retry) without hitting the API.
**After 30 seconds in open state, the circuit allows 3 test calls. If ≥2 succeed, it
closes. This prevents hammering a slow API, exhausting timeouts, and wasting money.**

---

**Q: How do you handle Kafka duplicate messages?**

A `UNIQUE INDEX on log_id` UUID in the `logs` table. The `log_id` is generated in the
ingestion service before publishing to Kafka — same message on replay carries the same
`log_id`. **`INSERT IGNORE` silently discards the duplicate without error.
The idempotency key is created at the source, before the Kafka boundary.**

---

**Q: Why Prometheus on every service instead of just the gateway?**

The gateway only sees aggregate traffic. **Service-level metrics reveal the real story:
`kafka_consumer_lag` tells you if storage is falling behind; `claude_api_tokens_used_total`
tracks cost; `dlq_messages_total` is the canary — rising dead letters mean silent
failures in the pipeline that the gateway knows nothing about.**

---

**Q: What is `spring.jpa.hibernate.ddl-auto: validate` and why always use it?**

`validate` checks that entity mappings match the existing DB schema on startup and fails
fast if they don't. It never modifies the schema. **`create-drop` would drop and recreate
all tables on every startup — destroying production data. Flyway is the only thing
that should modify the schema, and it does so with versioned, audited migration files.**

---

## General System Design

**Q: If traffic spikes 10x, what breaks first and how do you fix it?**

The ingestion endpoint is the front door. With Kafka, the ingestion service itself can
scale horizontally — it just publishes to Kafka and returns 202. The storage consumer
batches writes so DB load scales with batch size, not request count. **The anomaly
detector is the bottleneck — it can only scale to the partition count (6). Solution:
increase topic partitions and add consumer instances proportionally.**
