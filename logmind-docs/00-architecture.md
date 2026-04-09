# 00 — Architecture, HLD & System Design

> This document covers the **full system** before any code is written.
> Read this first. Every phase decision flows from these foundations.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Technology Choices & Rationale](#3-technology-choices--rationale)
4. [Communication Patterns](#4-communication-patterns)
5. [Data Flow — End to End](#5-data-flow--end-to-end)
6. [Failure Modes & Resilience](#6-failure-modes--resilience)
7. [Database Schema Design](#7-database-schema-design)
8. [Kafka Topic Design](#8-kafka-topic-design)
9. [Infrastructure (Docker Compose)](#9-infrastructure-docker-compose)
10. [Concurrency Design](#10-concurrency-design)
11. [Scalability Design](#11-scalability-design)

---

## 1. System Overview

LogMind ingests log events from any number of application services, persists them in a
time-series-optimised MySQL database, runs a statistical anomaly detection algorithm to
identify error spikes, feeds detected anomalies to Claude for AI root cause analysis, and
dispatches configurable alerts via webhook, email, or Slack.

### What makes this production-grade

| Concern | Solution |
|---------|---------|
| High-throughput ingest | Kafka decouples HTTP handler from DB write. Batch insert 100 logs or 500ms. |
| Time-series data | MySQL RANGE partition by month. O(1) data pruning. Composite index on (service_id, timestamp). |
| Anomaly detection | Per-service sliding window Z-score. No ML library — pure statistics. Bounded memory O(windowSize/bucketSize). |
| AI analysis | Claude API with token budgeting, RAG from past incidents, structured JSON output, eval loop. |
| Resilience | Circuit breaker on Claude API. Retry with backoff on notifications. Dead-letter queue for failed Kafka messages. |
| Observability | Prometheus metrics on every service. Spring Actuator health + info. |

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     CLIENT SERVICES                             │
│         (your app, microservices, scripts sending logs)         │
└──────────────────────────┬──────────────────────────────────────┘
                           │  HTTP POST /api/ingest
                           ▼
┌──────────────────────────────────────────┐
│  Ingestion Service                :3001  │
│  • Validate & normalize log entry        │
│  • Resolve service name → UUID (cached)  │
│  • Publish to Kafka raw-logs topic       │
│  • Rate limit: 100 req/s per service     │
└──────────────────┬───────────────────────┘
                   │  Kafka: raw-logs  (6 partitions, key=serviceName)
                   ▼
┌──────────────────────────────────────────┐
│  Storage Service                  :3002  │
│  • Consume raw-logs                      │
│  • Buffer in LogBatcher (100 or 500ms)   │
│  • Bulk INSERT into MySQL logs table     │
│  • Publish to stored-logs topic          │
└──────────────────┬───────────────────────┘
                   │  Kafka: stored-logs  (6 partitions, key=serviceId)
                   ▼
┌──────────────────────────────────────────┐
│  Anomaly Detection Service        :3003  │
│  • Consume stored-logs                   │
│  • Maintain WindowState per service      │
│  • Compute Z-score on every log          │
│  • Publish AnomalyMessage if Z > 2.5     │
└──────────────────┬───────────────────────┘
                   │  Kafka: anomalies  (3 partitions, key=serviceId)
                   ▼
┌──────────────────────────────────────────┐
│  AI Analysis Service              :3004  │
│  • Consume anomalies                     │
│  • RAG: fingerprint + past incidents     │
│  • Build prompt with token budget        │
│  • Call Claude API (circuit breaker)     │
│  • Parse structured JSON response        │
│  • Store incident + embedding            │
│  • Expose feedback endpoint (eval loop)  │
└──────────────────┬───────────────────────┘
                   │  Kafka: ai-results  (3 partitions, key=anomalyId)
                   ▼
┌──────────────────────────────────────────┐
│  Alert Service                    :3005  │
│  • Consume ai-results                    │
│  • Evaluate against alert_rules          │
│  • Dedup (5-min window per rule)         │
│  • Dispatch: Webhook / Email / Slack     │
│  • Persist alert_events with delivery    │
└──────────────────────────────────────────┘

┌──────────────────────────────────────────┐
│  API Gateway                      :3000  │
│  • Spring Cloud Gateway                  │
│  • Route /api/* to internal services     │
│  • SSE: /api/stream/logs                 │
│  • CORS for React frontend               │
└──────────────────┬───────────────────────┘
                   │  HTTP
                   ▼
┌──────────────────────────────────────────┐
│  React Frontend                   :5173  │
│  • Vite + React + Recharts + Tailwind    │
│  • Live log stream (SSE EventSource)     │
│  • Anomaly list + Incident explorer      │
│  • AI feedback buttons (eval loop UI)    │
│  • Alert rule CRUD                       │
└──────────────────────────────────────────┘

Infrastructure: MySQL 8 + Kafka + Zookeeper (Docker Compose)
```

---

## 3. Technology Choices & Rationale

### Spring Boot 3.2 / Java 21
Jakarta EE namespace (not javax). Virtual thread support via `@EnableVirtualThreads` if
needed for blocking I/O. Spring auto-configuration reduces boilerplate. Entire team
knows Spring — no ramp-up cost.

### Apache Kafka
- **Durability**: messages survive consumer restarts. Critical for log data.
- **Consumer groups**: multiple independent processors on the same topic without coordination.
- **Partition key**: keying by `serviceName` guarantees all logs from one service land on
  one partition — ordering guarantee per service.
- **Retention**: configurable per topic. raw-logs: 24h. ai-results: 7 days (replay).

**Why not RabbitMQ?** RabbitMQ deletes messages after acknowledgement. Kafka retains them
for replay. If the anomaly detection service is down for 2 hours, Kafka queues the messages.
RabbitMQ loses them.

### MySQL 8 with RANGE Partitioning
```sql
PARTITION BY RANGE (YEAR(timestamp) * 100 + MONTH(timestamp))
```
- `ALTER TABLE logs DROP PARTITION p202501` — O(1) metadata operation, drops the file pointer.
- `DELETE FROM logs WHERE timestamp < '2025-02-01'` — O(N), reads and deletes every row.
- At 1M logs/day, 6 months = 180M rows. DROP PARTITION: milliseconds. DELETE: hours.

**Composite index** `(service_id, timestamp)` perfectly matches the query pattern:
*"all logs for service X in time range Y"* — single B-tree scan.

### Resilience4j
Standard Spring Boot circuit breaker library. `@CircuitBreaker` annotation is zero-boilerplate.
Configured in `application.yml`. Exposes health indicators to Actuator automatically.

### React + Vite + Recharts
- **Vite**: HMR in <100ms vs Create React App's ~3s.
- **Recharts**: declarative React API, no imperative D3 manipulation.
- **Tailwind**: utility-first, no stylesheet naming conflicts.

---

## 4. Communication Patterns

| Pattern | Used for | Why |
|---------|---------|-----|
| **Synchronous HTTP** | Client→Ingestion, Frontend→Gateway, Gateway→Services | Immediate response needed |
| **Async Kafka** | Ingestion→Storage→Anomaly→AI→Alert | Processing pipeline. Decouple, absorb spikes. |
| **SSE** | Gateway→React for live log stream | One-directional push. HTTP/1.1 compatible. Auto-reconnect. |
| **In-memory** | WindowState, ServiceRegistry cache | Zero network latency for hot paths |

---

## 5. Data Flow — End to End

### Single log ingest (all phases running)

```
1.  Client: POST /api/logs {"service":"payment","level":"ERROR","message":"timeout"}
2.  Ingestion: @Valid → validate fields
3.  Ingestion: ServiceRegistry.resolveId("payment") → UUID (from cache or DB)
4.  Ingestion: Build RawLogMessage{logId=UUID, service, level, message, ...}
5.  Ingestion: KafkaTemplate.send("raw-logs", "payment", message)
6.  Ingestion: Return 202 Accepted immediately — HTTP handler done in ~2ms
7.  Storage Consumer: receives message from raw-logs partition for "payment"
8.  LogBatcher: buffer.add(message). If size>=100 OR 500ms timer fires: flush()
9.  LogBatcher: logRepository.bulkInsert(batch) → INSERT INTO logs VALUES (...)
10. LogBatcher: for each log: KafkaTemplate.send("stored-logs", serviceId, storedMsg)
11. Anomaly Consumer: receives stored-logs message
12. SlidingWindowAnalyzer.analyze(serviceId, ERROR, timestampMs, logEntry)
13. WindowState.addLog() → increment bucket, evict stale buckets
14. WindowState.getStats() → compute mean, stddev, Z-score
15. If Z > 2.5: AnomalyRepository.save() + AnomalyPublisher.send("anomalies")
16. AI Consumer: receives anomaly message
17. Dedup check: same service+severity within 5 min? Skip. Else:
18. FingerprintBuilder.build(anomaly) → normalized error patterns
19. IncidentMemory.getSimilar(fingerprint) → top 3 past incidents from DB
20. PromptBuilder.build(anomaly, similarIncidents) → full prompt string
21. Token budget check: truncate if >3500 tokens
22. ClaudeClient.analyze(prompt) → POST to api.anthropic.com/v1/messages
23. ResponseParser.parse(rawText) → ParsedIncident{hypothesis, confidence, ...}
24. IncidentRepository.save() + EmbeddingRepository.save()
25. AiResultPublisher.send("ai-results")
26. Alert Consumer: receives ai-results message
27. AlertEvaluator.evaluate(anomaly) → check alert_rules, dedup check
28. For each matching rule: NotificationDispatcher.dispatch()
29. Write alert_event{delivered=false}, dispatch, set delivered=true
30. React Frontend: SSE stream shows log in real time (step 10 also fans to SSE)
31. React Anomaly List: polls /api/anomalies every 10s, shows new anomaly
32. React Incident Card: loads /api/incidents/by-anomaly/{id}, shows hypothesis
```

---

## 6. Failure Modes & Resilience

| Failure | What happens | Recovery |
|---------|-------------|---------|
| **Kafka consumer crash** | Consumer group rebalances. Another instance picks up uncommitted offsets. | Automatic. At-least-once delivery because offset only commits after DB write. |
| **MySQL down during storage** | Storage consumer fails to insert. Does NOT commit offset. | MySQL recovers → consumer resumes from last committed offset. No data loss. |
| **Claude API timeout/error** | Circuit breaker tracks failure rate. Opens after 50% failures in 10-call window. | Fallback: anomaly queued in-memory. Circuit tries 3 test calls after 30s. |
| **Notification dispatch failure** | Written to alert_events with delivered=false. Retry 3x with backoff. | Undelivered alerts visible in dashboard. Manual retry possible. |
| **Ingestion spike** | Kafka absorbs. Storage consumer batches at its own pace. Rate limiter caps single-client abuse. | Backpressure handled. No DB overload. |
| **Consumer restart (window state lost)** | WindowState rebuilt from scratch. Cold start re-triggers. | Phase 7: persist WindowState snapshots before shutdown. |

---

## 7. Database Schema Design

All tables in the `logmind` database. Managed by Flyway. JPA in `validate` mode — never
`create` or `create-drop`.

### 7.1 `services`

```sql
CREATE TABLE services (
  id          VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  name        VARCHAR(100) NOT NULL,
  description TEXT,
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_services_name (name)
) ENGINE=InnoDB;
```

**Design rationale**: service names are stored once. Every log row stores the 36-char UUID
instead of repeating the full service name string. The UNIQUE constraint makes registration
idempotent — two concurrent calls registering "payment-service" produce one row.

### 7.2 `logs` — partitioned time-series

```sql
CREATE TABLE logs (
  id         BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
  service_id VARCHAR(36)      NOT NULL,
  level      ENUM('DEBUG','INFO','WARN','ERROR','FATAL') NOT NULL,
  message    TEXT             NOT NULL,
  metadata   JSON,
  timestamp  DATETIME(3)      NOT NULL,
  log_id     VARCHAR(36),                          -- idempotency key (Phase 7)
  PRIMARY KEY (id, timestamp),                     -- composite PK required by partitioning
  UNIQUE KEY uk_logs_log_id (log_id),              -- dedup on replay
  INDEX idx_logs_service_time (service_id, timestamp),
  INDEX idx_logs_level_time   (level, timestamp)
) ENGINE=InnoDB
PARTITION BY RANGE (YEAR(timestamp) * 100 + MONTH(timestamp)) (
  PARTITION p202501 VALUES LESS THAN (202502),
  PARTITION p202502 VALUES LESS THAN (202503),
  -- ... monthly partitions ...
  PARTITION p202512 VALUES LESS THAN (202601),
  PARTITION p_future VALUES LESS THAN MAXVALUE
);
```

**Partition key formula**: `YEAR(timestamp) * 100 + MONTH(timestamp)`
- January 2025 → `202501` → `p202501` (VALUES LESS THAN 202502)
- December 2025 → `202512` → `p202512`

**To prune 6 months of old data**:
```sql
ALTER TABLE logs DROP PARTITION p202501;  -- O(1), milliseconds
-- vs:
DELETE FROM logs WHERE timestamp < '2025-02-01';  -- O(N), hours at scale
```

**Why composite PK (id, timestamp)?** MySQL requires the partition key column to be part of
every unique index. Since `timestamp` drives the partition, it must be in the PK.
JPA only sees `id` as `@Id` — the composite PK is enforced by Flyway DDL only.

### 7.3 `anomalies`

```sql
CREATE TABLE anomalies (
  id              VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  service_id      VARCHAR(36)  NOT NULL,
  detected_at     DATETIME(3)  NOT NULL,
  window_start    DATETIME(3)  NOT NULL,
  window_end      DATETIME(3)  NOT NULL,
  error_count     INT UNSIGNED NOT NULL,
  baseline_mean   FLOAT        NOT NULL,
  baseline_stddev FLOAT        NOT NULL,
  z_score         FLOAT        NOT NULL,
  severity        ENUM('LOW','MEDIUM','HIGH','CRITICAL') NOT NULL,
  log_sample      JSON         NOT NULL,   -- last 20 error logs for AI prompt
  status          ENUM('OPEN','ANALYZING','RESOLVED','FALSE_POSITIVE') NOT NULL DEFAULT 'OPEN',
  created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  INDEX idx_anomalies_service  (service_id, detected_at),
  INDEX idx_anomalies_severity (severity, detected_at),
  CONSTRAINT fk_anomalies_service FOREIGN KEY (service_id) REFERENCES services(id)
) ENGINE=InnoDB;
```

### 7.4 `incidents` — AI analysis output

```sql
CREATE TABLE incidents (
  id                   VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  anomaly_id           VARCHAR(36)  NOT NULL,
  hypothesis           TEXT         NOT NULL,
  confidence           FLOAT        NOT NULL,
  affected_components  JSON         NOT NULL,
  suggested_actions    JSON         NOT NULL,
  similar_incident_ids JSON,
  raw_prompt           TEXT         NOT NULL,   -- full prompt for debugging
  raw_response         TEXT         NOT NULL,   -- full response for debugging
  tokens_used          INT UNSIGNED NOT NULL,
  model_version        VARCHAR(50)  NOT NULL,
  created_at           DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_incidents_anomaly (anomaly_id),  -- one incident per anomaly
  CONSTRAINT fk_incidents_anomaly FOREIGN KEY (anomaly_id) REFERENCES anomalies(id)
) ENGINE=InnoDB;
```

### 7.5 `incident_feedback` — eval loop

```sql
CREATE TABLE incident_feedback (
  id          VARCHAR(36) NOT NULL DEFAULT (UUID()),
  incident_id VARCHAR(36) NOT NULL,
  user_id     VARCHAR(36),
  rating      TINYINT     NOT NULL,
  notes       TEXT,
  created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  CONSTRAINT chk_rating CHECK (rating IN (-1, 1)),  -- +1 = helpful, -1 = not helpful
  CONSTRAINT fk_feedback_incident FOREIGN KEY (incident_id) REFERENCES incidents(id)
) ENGINE=InnoDB;
```

**Eval loop query** — daily accuracy trend:
```sql
SELECT
  DATE(created_at)  AS day,
  AVG(CASE WHEN rating = 1 THEN 1 ELSE 0 END) AS accuracy,
  COUNT(*)          AS total_ratings
FROM incident_feedback
GROUP BY DATE(created_at)
ORDER BY day DESC;
```

### 7.6 `incident_embeddings` — RAG fingerprints

```sql
CREATE TABLE incident_embeddings (
  id          VARCHAR(36)  NOT NULL DEFAULT (UUID()),
  incident_id VARCHAR(36)  NOT NULL,
  fingerprint JSON         NOT NULL,
  summary     VARCHAR(500) NOT NULL,
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_embeddings_incident (incident_id),
  CONSTRAINT fk_embeddings_incident FOREIGN KEY (incident_id) REFERENCES incidents(id)
) ENGINE=InnoDB;
```

### 7.7 `alert_rules`

```sql
CREATE TABLE alert_rules (
  id                   VARCHAR(36) NOT NULL DEFAULT (UUID()),
  service_id           VARCHAR(36),                          -- NULL = all services
  log_level            ENUM('DEBUG','INFO','WARN','ERROR','FATAL'),  -- NULL = any level
  threshold_count      INT UNSIGNED NOT NULL,
  threshold_window_s   INT UNSIGNED NOT NULL,
  severity             ENUM('LOW','MEDIUM','HIGH','CRITICAL') NOT NULL,
  notification_channel ENUM('EMAIL','WEBHOOK','SLACK') NOT NULL,
  notification_target  TEXT NOT NULL,
  is_active            BOOLEAN NOT NULL DEFAULT TRUE,
  created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  CONSTRAINT fk_rules_service FOREIGN KEY (service_id) REFERENCES services(id)
) ENGINE=InnoDB;
```

### 7.8 `alert_events` — audit log

```sql
CREATE TABLE alert_events (
  id          VARCHAR(36) NOT NULL DEFAULT (UUID()),
  rule_id     VARCHAR(36) NOT NULL,
  anomaly_id  VARCHAR(36),
  fired_at    DATETIME(3) NOT NULL,
  payload     JSON        NOT NULL,
  delivered   BOOLEAN     NOT NULL DEFAULT FALSE,   -- false until confirmed dispatch
  PRIMARY KEY (id),
  CONSTRAINT fk_events_rule    FOREIGN KEY (rule_id)    REFERENCES alert_rules(id),
  CONSTRAINT fk_events_anomaly FOREIGN KEY (anomaly_id) REFERENCES anomalies(id)
) ENGINE=InnoDB;
```

---

## 8. Kafka Topic Design

| Topic | Partitions | Key | Retention | Consumer groups |
|-------|-----------|-----|-----------|----------------|
| `raw-logs` | 6 | `serviceName` | 24h | `storage-group` |
| `stored-logs` | 6 | `serviceId` (UUID) | 1h | `anomaly-group` |
| `anomalies` | 3 | `serviceId` | 48h | `ai-group` |
| `ai-results` | 3 | `anomalyId` | 7 days | `alert-group`, `stream-group` |
| `raw-logs-dlq` | 1 | `serviceName` | 7 days | `dlq-monitor-group` |

### Why 6 partitions for raw-logs?
6 partitions = up to 6 parallel storage consumer instances. Each partition is owned by
exactly one consumer in the group. Scaling horizontally = add consumer instances up to
partition count.

### Why key by serviceName for raw-logs?
All logs from `payment-service` land on the same partition → ordering guarantee within
a service. The anomaly detector relies on logs arriving in order to maintain correct
window state.

### Why manual offset commit everywhere?
```
auto-commit: logs processed BEFORE offset advanced
  → if processing fails after auto-commit → message lost forever ❌

manual-commit: offset advanced ONLY after successful DB write
  → if processing fails → consumer re-reads from last committed offset ✓
```

---

## 9. Infrastructure (Docker Compose)

```yaml
version: "3.9"
services:

  mysql:
    image: mysql:8.0
    container_name: logmind-mysql
    environment:
      MYSQL_ROOT_PASSWORD: root_secret
      MYSQL_DATABASE: logmind
      MYSQL_USER: logmind
      MYSQL_PASSWORD: logmind_secret
    ports: ["3306:3306"]
    volumes: [mysql_data:/var/lib/mysql]
    command: >
      --innodb-buffer-pool-size=256M
      --max-connections=100
      --character-set-server=utf8mb4
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-ulogmind", "-plogmind_secret"]
      interval: 10s
      retries: 5

  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_HEAP_OPTS: "-Xmx128m -Xms128m"
    ports: ["2181:2181"]

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on: [zookeeper]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_HEAP_OPTS: "-Xmx256m -Xms256m"
    ports: ["9092:9092"]

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on: [kafka]
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    ports: ["8080:8080"]

volumes:
  mysql_data:
```

**Memory budget for 8GB RAM**:

| Process | Heap / Memory |
|---------|-------------|
| MySQL | 256 MB (innodb-buffer-pool) |
| Zookeeper | 128 MB |
| Kafka | 256 MB |
| Each Spring Boot service | 256–512 MB (`-Xmx512m`) |
| IDE (IntelliJ) | 2–3 GB |
| **Total** | ~4 GB, leaving comfortable headroom |

---

## 10. Concurrency Design

| Component | Shared state | Thread-safety mechanism |
|-----------|-------------|------------------------|
| `ServiceRegistry` cache | `ConcurrentHashMap<String, String>` (name→UUID) | `computeIfAbsent` — atomic get-or-create. No explicit lock. |
| `SlidingWindowAnalyzer` windows | `ConcurrentHashMap<String, WindowState>` | `computeIfAbsent` for map. Individual `WindowState` not synchronized in Phase 1 (single-threaded per service via Kafka partition assignment). |
| `LogBatcher` buffer | `List<RawLogMessage>` | `synchronized` on `addAll()` and `_flush()`. Both the Kafka consumer thread and `@Scheduled` timer thread can trigger flush. |
| `SseLogStreamHandler` clients | `CopyOnWriteArrayList<SseEmitter>` | CopyOnWriteArrayList is thread-safe for reads. Concurrent SSE clients do not block each other. |
| `AlertEvaluator` dedup cache | `ConcurrentHashMap<String, Instant>` | `putIfAbsent` for atomic dedup check. |

---

## 11. Scalability Design

| Bottleneck | Solution |
|-----------|---------|
| Ingestion throughput | Kafka absorbs spikes. Multiple ingestion service instances behind a load balancer. |
| Storage throughput | Scale storage consumers up to partition count (6). LogBatcher batch insert amortises DB overhead. |
| Anomaly detection | One consumer per partition (max 6 instances). WindowState consistent per service via partition ownership. |
| AI analysis | Scale ai-analysis-service horizontally — each anomaly is independent. Circuit breaker prevents Claude API overload. |
| Alert dispatch | Scale alert-service horizontally. Alert dedup prevents storm. |
| DB read queries | `@Transactional(readOnly=true)` signals read-replica eligibility. Add MySQL read replica in production. |
| Log table growth | Monthly partition drop is O(1). Add new partitions via V2 Flyway migration. |
