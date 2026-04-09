# 07 — Phase 7: Hardening & Production Readiness

> **Goal**: Every service is production-grade. Circuit breakers, rate limiting, DLQ, metrics, graceful shutdown, idempotency.
>
> **Affects**: all existing services — no new services.
>
> **Estimated time**: 16–20 hours

---

## 1. Circuit Breakers — Resilience4j

Add to every service that calls an external resource:
- `ClaudeClient` → `claudeApi` circuit
- `WebhookDispatcher` → `webhookDispatch` circuit
- `EmailDispatcher` → `emailDispatch` circuit
- `SlackDispatcher` → `slackDispatch` circuit

### `pom.xml`
```xml
<dependency>
  <groupId>io.github.resilience4j</groupId>
  <artifactId>resilience4j-spring-boot3</artifactId>
</dependency>
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-aop</artifactId>
</dependency>
```

### `application.yml`
```yaml
resilience4j:
  circuitbreaker:
    instances:
      claudeApi:
        slidingWindowType: COUNT_BASED
        slidingWindowSize: 10
        failureRateThreshold: 50           # open at 50% failures
        waitDurationInOpenState: 30s
        permittedNumberOfCallsInHalfOpenState: 3
        registerHealthIndicator: true      # /actuator/health shows state
      webhookDispatch:
        slidingWindowSize: 5
        failureRateThreshold: 60
        waitDurationInOpenState: 60s
        permittedNumberOfCallsInHalfOpenState: 2
```

### Usage
```java
@CircuitBreaker(name = "claudeApi", fallbackMethod = "fallback")
public ClaudeResponse analyze(String prompt) { ... }

public ClaudeResponse fallback(String prompt, Exception ex) {
    log.warn("Circuit open: {}", ex.getMessage());
    return ClaudeResponse.circuitOpen();
}
```

**Circuit state machine**:
```
CLOSED ──(50% failures in 10 calls)──► OPEN
  ▲                                       │
  │                         (wait 30s)    │
  │                                       ▼
(2/3 test calls succeed) ─── HALF-OPEN ◄─┘
```

---

## 2. Rate Limiting — Bucket4j

Per-service token bucket on ingestion endpoint. 100 requests / 10 seconds per service.

### `pom.xml`
```xml
<dependency>
  <groupId>com.github.vladimir-bukhtoyarov</groupId>
  <artifactId>bucket4j-core</artifactId>
  <version>8.7.0</version>
</dependency>
```

### `RateLimitFilter.java` — `@Component`
```java
@Slf4j
@Component
@RequiredArgsConstructor
public class RateLimitFilter implements Filter {

    private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    @Value("${logmind.rate-limit.max-per-service:100}")
    private int maxPerService;

    @Value("${logmind.rate-limit.window-seconds:10}")
    private int windowSeconds;

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpReq = (HttpServletRequest) req;
        HttpServletResponse httpRes = (HttpServletResponse) res;

        // Extract service name from request body or header
        String serviceName = httpReq.getHeader("X-Service-Name");
        if (serviceName == null) serviceName = "unknown";

        Bucket bucket = buckets.computeIfAbsent(serviceName, this::createBucket);

        if (bucket.tryConsume(1)) {
            chain.doFilter(req, res);
        } else {
            log.warn("Rate limit exceeded for service={}", serviceName);
            httpRes.setStatus(429);
            httpRes.setHeader("Retry-After", String.valueOf(windowSeconds));
            httpRes.getWriter().write("""
                {"type":"https://logmind.dev/errors/rate-limited",
                 "title":"Too Many Requests",
                 "status":429,
                 "detail":"Rate limit exceeded. Retry after %d seconds."}
                """.formatted(windowSeconds));
        }
    }

    private Bucket createBucket(String serviceName) {
        Bandwidth limit = Bandwidth.classic(maxPerService,
                Refill.greedy(maxPerService, Duration.ofSeconds(windowSeconds)));
        return Bucket.builder().addLimit(limit).build();
    }
}
```

---

## 3. Dead-Letter Queue Handling

Messages that fail after all retries → `raw-logs-dlq` (or per-topic DLQ).

### `DlqConsumer.java`
```java
@Slf4j
@Component
@RequiredArgsConstructor
public class DlqConsumer {

    private final MeterRegistry meterRegistry;

    @KafkaListener(topics = "raw-logs-dlq", groupId = "dlq-monitor-group")
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        log.error("DLQ message received: topic=raw-logs service={} offset={}",
                record.key(), record.offset());

        meterRegistry.counter("dlq_messages_total",
                "topic", "raw-logs",
                "service", record.key())
                .increment();

        // Optional: INSERT into dead_letter_logs for manual review
        // Optional: send admin alert (Slack/email)

        ack.acknowledge();  // always ack DLQ — just monitor
    }
}
```

---

## 4. Prometheus Metrics

Add to all services:

### `pom.xml`
```xml
<dependency>
  <groupId>io.micrometer</groupId>
  <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

### `application.yml`
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, info, metrics, prometheus
  metrics:
    export:
      prometheus:
        enabled: true
```

### Custom metrics — add throughout the codebase

```java
// In LogIngestionService:
meterRegistry.counter("log_ingest_total", "service_name", serviceName).increment(entities.size());

// In SlidingWindowAnalyzer:
meterRegistry.counter("anomaly_detected_total",
        "service_id", serviceId,
        "severity",   classify(stats.zScore()).name()).increment();

// In ClaudeClient:
meterRegistry.counter("claude_api_calls_total", "status", "success").increment();
meterRegistry.counter("claude_api_tokens_used_total").increment(inputTokens + outputTokens);

// In DlqConsumer:
meterRegistry.counter("dlq_messages_total", "topic", "raw-logs").increment();

// In NotificationDispatcher:
meterRegistry.counter("alert_dispatched_total",
        "channel",   rule.getNotificationChannel().name(),
        "delivered", String.valueOf(delivered)).increment();
```

### Complete metrics table

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `http_server_requests_seconds` | Timer | uri, method, status | Built in by Actuator |
| `log_ingest_total` | Counter | service_name | Logs ingested per source |
| `anomaly_detected_total` | Counter | service_id, severity | Anomalies per service |
| `claude_api_calls_total` | Counter | status | Claude API call success/failure |
| `claude_api_tokens_used_total` | Counter | — | Total tokens (cost tracking) |
| `dlq_messages_total` | Counter | topic, service | Dead-letter queue volume |
| `alert_dispatched_total` | Counter | channel, delivered | Alert delivery by channel |
| `kafka_consumer_lag` | Gauge | topic, partition | Built in by Spring Kafka |
| `resilience4j_circuitbreaker_state` | Gauge | name | Circuit breaker state |

---

## 5. Graceful Shutdown

```yaml
# application.yml — all services
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
server:
  shutdown: graceful
```

**What happens on SIGTERM**:
1. Spring stops accepting new HTTP requests
2. In-flight requests are allowed to complete (up to 30s)
3. Kafka consumers: pause consumption, finish current message, commit offsets
4. `@PreDestroy` methods run — flush LogBatcher if non-empty
5. DB connections returned to pool, pool shuts down
6. JVM exits

### `LogBatcher.java` — add `@PreDestroy`
```java
@PreDestroy
public void shutdown() {
    log.info("Shutdown: flushing {} remaining logs", buffer.size());
    if (!buffer.isEmpty()) {
        flush();
    }
}
```

---

## 6. Idempotency — Handle Kafka Duplicate Messages

At-least-once delivery means duplicates are possible on consumer restart.

### Flyway V2 migration
```sql
-- V2__add_log_idempotency.sql
ALTER TABLE logs ADD COLUMN log_id VARCHAR(36);
CREATE UNIQUE INDEX uk_logs_log_id ON logs(log_id);
```

### `LogBatcher.java` — use INSERT IGNORE
```java
// In the flush() method, replace saveAll() with a native INSERT IGNORE:
@Query(value = """
    INSERT IGNORE INTO logs (log_id, service_id, level, message, metadata, timestamp)
    VALUES (:logId, :serviceId, :level, :message, :metadata, :timestamp)
    """, nativeQuery = true)
void insertIgnore(@Param("logId") String logId, ...);
```

Or use `ON DUPLICATE KEY UPDATE created_at = created_at` as a no-op update.

**How it works**:
```
First delivery:   logId="abc-123" → INSERT succeeds ✓
Duplicate:        logId="abc-123" → INSERT IGNORE silently skips ✓
Net result:       exactly one row in the DB ✓
```

---

## 7. Architecture Decision Record (ARCHITECTURE.md)

Create `ARCHITECTURE.md` at the root of the project. Answer these questions:

```markdown
# Architecture Decision Records

## ADR-001: MySQL over PostgreSQL
**Decision**: MySQL 8 with RANGE partitioning
**Rationale**: RANGE partition by month enables O(1) pruning via DROP PARTITION.
The time-series query pattern (service_id + timestamp range) is well-served
by the composite index. JSON column for metadata avoids schema churn.
**Trade-off**: No native vector search (pgvector) → fingerprint-based RAG instead.

## ADR-002: Z-score anomaly detection (no ML library)
**Decision**: Pure statistical sliding window in Java
**Rationale**: No training data needed. Adapts to each service's own baseline.
Explainable — interviewers can understand and audit the math.
**Trade-off**: Cannot detect gradual drift (only spikes). Cannot correlate
anomalies across services. Add DBSCAN clustering in a future phase if needed.

## ADR-003: Fingerprint RAG over vector embeddings
**Decision**: Weighted fingerprint similarity in application code
**Rationale**: MySQL lacks native vector search. Fingerprints are explainable
and fast for O(hundreds) of past incidents.
**Trade-off**: Lower semantic accuracy than real embeddings.
Migration path: replace IncidentMemory with pgvector/Pinecone implementation
behind the same interface.

## ADR-004: At-least-once Kafka delivery
**Decision**: Manual offset commit after successful DB write
**Rationale**: No data loss on consumer crash. Duplicates handled by
INSERT IGNORE with log_id UUID idempotency key.
**Trade-off**: Duplicate log records possible (silent via INSERT IGNORE).

## ADR-005: Monolith first (Phase 1)
**Decision**: Start with single Spring Boot process, extract to microservices in phases
**Rationale**: Deliver end-to-end value immediately. Easier to debug.
Phase 2 extraction requires changing one method — demonstrates clean architecture.
**Trade-off**: Phase 1 has no backpressure — DB is on the critical path.
Accepted: Phase 1 is dev/demo use, not production traffic.
```
