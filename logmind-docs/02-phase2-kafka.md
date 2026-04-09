# 02 — Phase 2: Kafka Async Pipeline

> **Goal**: HTTP handler returns 202 in ~2ms. Storage and anomaly detection happen asynchronously.
>
> **Change surface**: One method in `LogIngestionService`. One new file `RawLogConsumer`. Zero other changes.
>
> **Estimated time**: 12–16 hours

---

## Table of Contents

1. [Why Kafka](#1-why-kafka)
2. [New dependencies & config](#2-new-dependencies--config)
3. [New files — complete LLD](#3-new-files--complete-lld)
4. [Modified files](#4-modified-files)
5. [Offset commit strategy deep dive](#5-offset-commit-strategy-deep-dive)
6. [Dead-letter queue](#6-dead-letter-queue)
7. [Testing Phase 2](#7-testing-phase-2)

---

## 1. Why Kafka

### Problem with Phase 1

```
POST /api/logs
   → validate (1ms)
   → resolve service (cache hit: 0ms, miss: 5ms)
   → saveAll to MySQL (10-50ms depending on load)
   → runAnomalyCheck (1-3ms)
   → return 202
Total: 12-59ms per request
```

At 1000 req/s this means the ingestion service must sustain 1000 MySQL writes/sec.
Under load the DB becomes the bottleneck and p99 latency spikes.

### Solution with Kafka

```
POST /api/logs
   → validate (1ms)
   → resolve service (cache: 0ms)
   → kafkaTemplate.send() (2ms round-trip to broker)
   → return 202
Total: 3ms always — regardless of DB load

[separate consumer]
   → receive from Kafka
   → buffer 100 logs or 500ms
   → bulk INSERT (one DB round-trip for 100 logs vs 100 round-trips)
   → runAnomalyCheck
```

The HTTP handler is now O(1) with respect to DB load.

---

## 2. New dependencies & config

### `pom.xml`

```xml
<dependency>
  <groupId>org.springframework.kafka</groupId>
  <artifactId>spring-kafka</artifactId>
  <!-- version managed by spring-boot-starter-parent -->
</dependency>
```

### `application.yml` additions

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS:localhost:9092}

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all                       # wait for all in-sync replicas before ack
      retries: 3
      properties:
        enable.idempotence: true      # exactly-once writes to Kafka

    consumer:
      group-id: storage-group
      auto-offset-reset: earliest     # on new group: start from beginning
      enable-auto-commit: false       # CRITICAL: manual commit only
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.logmind.dto

  # Listener factory config
  listener:
    ack-mode: MANUAL_IMMEDIATE        # ack() commits offset immediately when called
    type: batch                       # process messages in batches
    concurrency: 1                    # one thread per consumer instance
```

### `docker-compose.yml` — uncomment the Kafka block

```yaml
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
```

---

## 3. New files — complete LLD

### 3.1 `KafkaTopicConfig.java` — `@Configuration`

```java
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic rawLogsTopic() {
        return TopicBuilder.name("raw-logs")
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic storedLogsTopic() {
        return TopicBuilder.name("stored-logs")
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic anomaliesTopic() {
        return TopicBuilder.name("anomalies")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic aiResultsTopic() {
        return TopicBuilder.name("ai-results")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic rawLogsDlqTopic() {
        return TopicBuilder.name("raw-logs-dlq")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
```

Spring Kafka auto-creates topics on startup if they don't exist.
`replicas(1)` for single-broker dev. Change to `replicas(3)` in production.

---

### 3.2 `RawLogMessage.java` — Kafka message DTO

```java
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class RawLogMessage {
    private String logId;             // UUID — idempotency key for Phase 7
    private String service;           // human-readable name
    private String serviceId;         // UUID — resolved by ingestion service
    private LogLevel level;
    private String message;
    private Map<String, Object> metadata;
    private LocalDateTime timestamp;
    private LocalDateTime ingestedAt;
}
```

**Why separate from `IngestRequest`?** The Kafka message includes fields added by the
ingestion service (`logId`, `serviceId`, `ingestedAt`) that the caller does not supply.
Keeping them separate avoids leaking internal fields to the HTTP API.

---

### 3.3 `LogPublisher.java` — `@Service`

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class LogPublisher {

    private final KafkaTemplate<String, RawLogMessage> kafkaTemplate;

    public void publish(IngestRequest request, String serviceId) {
        RawLogMessage msg = RawLogMessage.builder()
                .logId(UUID.randomUUID().toString())
                .service(request.getService())
                .serviceId(serviceId)
                .level(request.getLevel())
                .message(request.getMessage())
                .metadata(request.getMetadata())
                .timestamp(request.getTimestamp() != null
                        ? request.getTimestamp()
                        : LocalDateTime.now())
                .ingestedAt(LocalDateTime.now())
                .build();

        kafkaTemplate.send("raw-logs", request.getService(), msg)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish log for service={}: {}",
                                request.getService(), ex.getMessage());
                        publishToDlq(msg);
                    }
                });
    }

    public void publishBatch(List<IngestRequest> requests, String serviceId) {
        requests.forEach(req -> publish(req, serviceId));
    }

    private void publishToDlq(RawLogMessage msg) {
        kafkaTemplate.send("raw-logs-dlq", msg.getService(), msg)
                .whenComplete((result, ex) -> {
                    if (ex != null) log.error("DLQ publish also failed: {}", ex.getMessage());
                });
    }
}
```

**Key=serviceName**: All logs from `payment-service` land on the same partition.
Kafka's hash partitioner assigns `hash(key) % numPartitions` — consistent per key.

---

### 3.4 `RawLogConsumer.java` — `@Component`

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class RawLogConsumer {

    private final LogBatcher logBatcher;

    @KafkaListener(
            topics = "raw-logs",
            groupId = "storage-group",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consumeBatch(
            List<ConsumerRecord<String, RawLogMessage>> records,
            Acknowledgment ack) {

        try {
            logBatcher.addAll(records.stream()
                    .map(ConsumerRecord::value)
                    .toList());

            ack.acknowledge();   // commit offsets AFTER successful batcher.addAll()

        } catch (Exception e) {
            log.error("Batch processing failed — NOT acknowledging. Will retry. error={}",
                    e.getMessage());
            // Do NOT call ack.acknowledge()
            // Kafka will re-deliver this batch on next poll
        }
    }
}
```

**Why batch mode?** `eachBatch` lets us hand the entire poll result to `LogBatcher.addAll()`
in one call. The batcher decides when to flush to DB. We acknowledge only after `addAll()`
succeeds — if it throws, the offset is not committed and Kafka re-delivers.

---

### 3.5 `LogBatcher.java` — `@Component`

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class LogBatcher {

    @Value("${logmind.batcher.max-size:100}")
    private int maxSize;

    @Value("${logmind.batcher.max-wait-ms:500}")
    private long maxWaitMs;

    private final LogRepository logRepository;
    private final StoredLogPublisher storedLogPublisher;
    private final ServiceRegistry serviceRegistry;

    private final List<RawLogMessage> buffer = new ArrayList<>();

    public synchronized void addAll(List<RawLogMessage> messages) {
        buffer.addAll(messages);
        if (buffer.size() >= maxSize) {
            flush();
        }
    }

    @Scheduled(fixedDelay = 500)   // timer trigger — every 500ms
    public synchronized void scheduledFlush() {
        if (!buffer.isEmpty()) {
            flush();
        }
    }

    private void flush() {
        if (buffer.isEmpty()) return;

        List<RawLogMessage> batch = new ArrayList<>(buffer);
        buffer.clear();

        try {
            List<LogEntity> entities = batch.stream()
                    .map(this::toEntity)
                    .toList();

            logRepository.saveAll(entities);
            log.debug("Flushed {} logs to DB", entities.size());

            // Publish each saved log to stored-logs for anomaly detection
            entities.forEach(storedLogPublisher::publish);

        } catch (Exception e) {
            log.error("Flush failed, re-queuing {} messages: {}", batch.size(), e.getMessage());
            // Put messages back at the front of the buffer for retry
            buffer.addAll(0, batch);
        }
    }

    private LogEntity toEntity(RawLogMessage msg) {
        return LogEntity.of(
                msg.getServiceId(),
                msg.getLevel(),
                msg.getMessage(),
                msg.getMetadata(),
                msg.getTimestamp()
        );
    }
}
```

**Dual-trigger design**:

```
Trigger 1 — size:  addAll() checks buffer.size() >= 100 → flush immediately
Trigger 2 — timer: @Scheduled fires every 500ms → flush if non-empty

Why both?
  Size only: at low traffic, logs sit in buffer for up to infinity waiting for 100
  Timer only: at high traffic, buffer can hold 10,000 logs before 500ms timer fires
  Both:       at most 100 logs OR 500ms delay — bounded in both directions
```

**Why `synchronized`?** Two threads can call `flush()`:
1. The Kafka consumer thread via `addAll()`
2. The Spring scheduler thread via `scheduledFlush()`

Without `synchronized`, both could drain the buffer concurrently — double processing.
`synchronized` on `addAll` and `scheduledFlush` ensures only one thread flushes at a time.

---

### 3.6 `StoredLogPublisher.java` — `@Service`

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class StoredLogPublisher {

    private final KafkaTemplate<String, StoredLogMessage> kafkaTemplate;

    public void publish(LogEntity entity) {
        StoredLogMessage msg = StoredLogMessage.builder()
                .logDbId(entity.getId())
                .serviceId(entity.getServiceId())
                .level(entity.getLevel())
                .message(entity.getMessage())
                .timestamp(entity.getTimestamp())
                .build();

        kafkaTemplate.send("stored-logs", entity.getServiceId(), msg);
    }
}
```

Key=`serviceId` (UUID). All logs from one service → one partition → consistent
`WindowState` in the anomaly consumer.

---

### 3.7 `StoredLogMessage.java` — Kafka DTO

```java
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class StoredLogMessage {
    private Long logDbId;          // DB auto-increment ID
    private String serviceId;      // UUID
    private String serviceName;    // for anomaly consumer display
    private LogLevel level;
    private String message;
    private LocalDateTime timestamp;
}
```

---

## 4. Modified files

### `LogIngestionService.java` — the one-line change

```java
@Transactional
public IngestResponse ingestBatch(List<IngestRequest> requests) {
    // ... validation ...

    String serviceName = requests.get(0).getService();
    String serviceId   = serviceRegistry.resolveId(serviceName);

    // ─── PHASE 1 (remove this block) ─────────────────────────────
    // List<LogEntity> entities = requests.stream().map(...).toList();
    // logRepository.saveAll(entities);
    // entities.forEach(log -> runAnomalyCheck(log, serviceId, serviceName));
    // ─────────────────────────────────────────────────────────────

    // ─── PHASE 2 (add this line) ──────────────────────────────────
    logPublisher.publishBatch(requests, serviceId);
    // ─────────────────────────────────────────────────────────────

    return IngestResponse.of(requests.size(), serviceId);
}
```

`@Transactional` can be removed from this method in Phase 2 — there is no DB write here.
Keep it for now to avoid surprises.

---

## 5. Offset commit strategy deep dive

```
Timeline: message arrives at Kafka, consumer processes it

AUTO-COMMIT (enable-auto-commit=true):
  t=0ms   Poll: receive message
  t=5ms   Auto-commit: offset advanced to message+1
  t=6ms   DB write starts
  t=CRASH DB write fails, service crashes
  t=?     Service restarts
  Result: offset already advanced → Kafka will NOT re-deliver → message LOST ❌

MANUAL-COMMIT (enable-auto-commit=false):
  t=0ms   Poll: receive message
  t=1ms   DB write starts
  t=10ms  DB write succeeds
  t=11ms  ack.acknowledge() → offset advanced to message+1
  t=CRASH between t=1 and t=11ms:
  t=?     Service restarts, polls again from last committed offset
  Result: message re-delivered → at-least-once ✓ (handle duplicate with log_id dedup)
```

**Delivery semantics comparison**:

| Semantic | How | Implication |
|----------|-----|-------------|
| **At-most-once** | Auto-commit before processing | Possible data loss on crash. Unacceptable for logs. |
| **At-least-once** | Manual commit after success (our choice) | Possible duplicates on crash. Handle with `INSERT IGNORE`. |
| **Exactly-once** | Kafka transactions + idempotent producer | Highest complexity. Add in Phase 7 if required. |

---

## 6. Dead-letter queue

Messages that cannot be processed after all retries go to `raw-logs-dlq`.

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class DlqConsumer {

    private final MeterRegistry meterRegistry;

    @KafkaListener(topics = "raw-logs-dlq", groupId = "dlq-monitor-group")
    public void consume(ConsumerRecord<String, RawLogMessage> record, Acknowledgment ack) {
        log.error("DLQ message: service={} logId={} topic=raw-logs",
                record.key(), record.value().getLogId());

        // Increment Prometheus counter
        meterRegistry.counter("dlq_messages_total",
                "topic", "raw-logs",
                "service", record.key()).increment();

        ack.acknowledge();  // always ack DLQ — just log and monitor
    }
}
```

**When does a message go to DLQ?**
- `LogPublisher.publish()` fails on `whenComplete` callback after 3 Kafka retries
- Any uncaught exception in `RawLogConsumer` after configured retry attempts

---

## 7. Testing Phase 2

### Integration test — verify Kafka round-trip

```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"raw-logs", "stored-logs"})
@ActiveProfiles("test")
class KafkaPipelineTest {

    @Autowired
    private LogIngestionService ingestionService;

    @Autowired
    private KafkaTemplate<String, RawLogMessage> kafkaTemplate;

    @Test
    @DisplayName("Ingested log reaches stored-logs topic within 2 seconds")
    void logFlowsFromIngestToStoredLogs() throws InterruptedException {
        IngestRequest request = new IngestRequest();
        request.setService("test-service");
        request.setLevel(LogLevel.ERROR);
        request.setMessage("Test message");

        ingestionService.ingest(request);

        // Consumer latch — wait for stored-logs message
        CountDownLatch latch = new CountDownLatch(1);
        // Configure test consumer listening on stored-logs...
        assertTrue(latch.await(2, TimeUnit.SECONDS),
                "Log should reach stored-logs within 2 seconds");
    }
}
```

**`@EmbeddedKafka`**: Starts an in-memory Kafka broker for tests. No Docker needed.
