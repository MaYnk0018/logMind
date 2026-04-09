# 03 — Phase 3: Anomaly Detection Microservice

> **Goal**: Extract `SlidingWindowAnalyzer` into its own Spring Boot process.
>
> **New service**: `anomaly-detection-service` on port `3003`.
>
> **Critical constraint**: Window state is in-memory. Each consumer instance must own its
> partitions exclusively. **Do not scale beyond partition count (6).**
>
> **Estimated time**: 16–20 hours

---

## Table of Contents

1. [Why extract anomaly detection](#1-why-extract-anomaly-detection)
2. [Partition ownership — the critical design constraint](#2-partition-ownership--the-critical-design-constraint)
3. [New service structure](#3-new-service-structure)
4. [New files — complete LLD](#4-new-files--complete-lld)
5. [Edge cases handled](#5-edge-cases-handled)

---

## 1. Why extract anomaly detection

In Phase 1/2, anomaly detection runs inside the storage consumer — after DB write, before
publishing to `stored-logs`. This couples two concerns with different resource profiles:

| Concern | Resource need |
|---------|--------------|
| Storage | I/O bound — waiting on MySQL |
| Anomaly detection | CPU bound — Z-score math on every log |

Separate services = separate scaling. Storage can have more instances (more DB connections).
Anomaly detection can have more CPU. Failures in anomaly detection don't block storage.

---

## 2. Partition ownership — the critical design constraint

```
WRONG — DO NOT DO THIS:

  stored-logs topic (6 partitions):
  partition-0 (payment-service logs) → Instance A  ← WindowState for payment-service
  partition-1 (order-service logs)   → Instance A
  partition-2 (payment-service logs) → Instance B  ← ALSO WindowState for payment-service ❌

  Both A and B have partial payment-service windows.
  A sees half the errors. B sees the other half.
  Z-score computed on 50% of data → completely wrong anomaly detection.

CORRECT:

  stored-logs is keyed by serviceId.
  hash("payment-service-uuid") % 6 = 2  → always partition-2
  All payment-service logs → partition-2 → only Instance A owns partition-2
  Instance A has complete WindowState for payment-service. ✓

Rule: Never scale anomaly-detection-service beyond the partition count (6).
```

---

## 3. New service structure

```
anomaly-detection-service/
├── AnomalyDetectionApplication.java     ← port 3003
├── consumer/
│   └── StoredLogConsumer.java           ← @KafkaListener on stored-logs
├── analyzer/
│   ├── SlidingWindowAnalyzer.java       ← IDENTICAL to Phase 1 — zero changes
│   ├── WindowState.java                 ← IDENTICAL to Phase 1 — zero changes
│   └── SeverityClassifier.java          ← extracted from SlidingWindowAnalyzer
├── publisher/
│   └── AnomalyPublisher.java            ← publishes to anomalies topic
├── repository/
│   └── AnomalyRepository.java           ← IDENTICAL to Phase 1
└── dto/
    └── StoredLogMessage.java            ← deserializes from stored-logs
```

---

## 4. New files — complete LLD

### 4.1 `AnomalyDetectionApplication.java`

```java
@SpringBootApplication
@EnableScheduling
public class AnomalyDetectionApplication {
    public static void main(String[] args) {
        SpringApplication.run(AnomalyDetectionApplication.class, args);
    }
}
```

Port 3003 in `application.yml`. `@EnableScheduling` kept for optional window snapshot
timer (Phase 7).

---

### 4.2 `StoredLogConsumer.java` — `@Component`

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class StoredLogConsumer {

    private final SlidingWindowAnalyzer analyzer;
    private final AnomalyRepository anomalyRepository;
    private final AnomalyPublisher anomalyPublisher;

    @KafkaListener(
            topics = "stored-logs",
            groupId = "anomaly-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, StoredLogMessage> record,
            Acknowledgment ack) {

        StoredLogMessage msg = record.value();

        try {
            long tsMs = msg.getTimestamp()
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();

            Map<String, Object> logEntry = Map.of(
                    "timestamp", msg.getTimestamp().toString(),
                    "level",     msg.getLevel().name(),
                    "message",   msg.getMessage()
            );

            analyzer.analyze(msg.getServiceId(), msg.getLevel(), tsMs, logEntry)
                    .ifPresent(anomaly -> {
                        AnomalyEntity saved = anomalyRepository.save(toEntity(anomaly));
                        anomalyPublisher.publish(saved);
                        log.warn("Anomaly: service={} severity={} z={}",
                                msg.getServiceId(), anomaly.severity(),
                                String.format("%.2f", anomaly.stats().zScore()));
                    });

            ack.acknowledge();

        } catch (Exception e) {
            log.error("Failed to process stored-log: service={} error={}",
                    msg.getServiceId(), e.getMessage());
            // Don't ack — re-deliver
        }
    }

    @KafkaListener(topicPartitions = {})
    public void onPartitionAssigned(
            @Header(KafkaHeaders.ASSIGNED_PARTITIONS) List<TopicPartition> partitions) {
        log.info("Assigned partitions: {}", partitions);
        // Phase 7: load WindowState snapshots from DB for assigned partitions
    }

    @KafkaListener(topicPartitions = {})
    public void onPartitionRevoked(
            @Header(KafkaHeaders.REVOKED_PARTITIONS) List<TopicPartition> partitions) {
        log.info("Revoked partitions: {}", partitions);
        // Phase 7: persist WindowState snapshots for revoked partitions
    }

    private AnomalyEntity toEntity(SlidingWindowAnalyzer.DetectedAnomaly anomaly) {
        WindowState.Stats s = anomaly.stats();
        AnomalyEntity e = new AnomalyEntity();
        e.setServiceId(anomaly.serviceId());
        e.setDetectedAt(LocalDateTime.now());
        e.setWindowStart(toLocalDateTime(s.windowStartMs()));
        e.setWindowEnd(toLocalDateTime(s.windowEndMs()));
        e.setErrorCount(s.currentCount());
        e.setBaselineMean(s.mean());
        e.setBaselineStddev(s.stddev());
        e.setZScore(s.zScore());
        e.setSeverity(anomaly.severity());
        e.setLogSample(anomaly.logSample());
        return e;
    }

    private LocalDateTime toLocalDateTime(long epochMs) {
        return LocalDateTime.ofEpochSecond(epochMs / 1000, 0, ZoneOffset.UTC);
    }
}
```

**Why `eachMessage` (not `eachBatch`) for anomaly detection?** Anomaly detection is
stateful per service. Processing one message at a time keeps the code simple. If throughput
is insufficient, switch to batch mode but process each message in the batch individually —
never aggregate before the Z-score check.

---

### 4.3 `AnomalyPublisher.java` — `@Service`

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class AnomalyPublisher {

    private final KafkaTemplate<String, AnomalyMessage> kafkaTemplate;

    public void publish(AnomalyEntity entity) {
        AnomalyMessage msg = AnomalyMessage.builder()
                .anomalyId(entity.getId())
                .serviceId(entity.getServiceId())
                .detectedAt(entity.getDetectedAt())
                .windowStart(entity.getWindowStart())
                .windowEnd(entity.getWindowEnd())
                .errorCount(entity.getErrorCount())
                .baselineMean(entity.getBaselineMean())
                .baselineStddev(entity.getBaselineStddev())
                .zScore(entity.getZScore())
                .severity(entity.getSeverity())
                .logSample(entity.getLogSample())
                .build();

        kafkaTemplate.send("anomalies", entity.getServiceId(), msg)
                .whenComplete((result, ex) -> {
                    if (ex != null)
                        log.error("Failed to publish anomaly {}: {}", entity.getId(), ex.getMessage());
                });
    }
}
```

---

## 5. Edge cases handled

| Edge case | Handling |
|-----------|---------|
| `< 20 buckets` (cold start) | `getStats()` returns `null`. No anomaly. 20 buckets = 20 min of baseline. |
| `stddev == 0` (flat line) | `getStats()` returns `null`. All buckets same count = no meaningful variance. |
| Consumer restart | `WindowState` rebuilt from zero. Cold start re-triggers. Phase 7: snapshot WindowState before shutdown. |
| Rebalance (new instance joins) | Partitions reassigned. Existing instance loses some. `onPartitionRevoked` hook fires — persist state. New instance gets partitions cold. |
| Out-of-order messages | Bucket key is derived from message `timestamp`, not arrival time. Late messages update the correct historical bucket. Eviction protects against very old messages. |
