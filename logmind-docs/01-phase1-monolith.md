# 01 — Phase 1: Monolith Core

> **Goal**: End-to-end working pipeline in a single JVM. No Kafka. No microservices.
>
> **Deliverable**: `POST /api/logs` saves to MySQL → anomaly detected → `GET /api/anomalies` returns it.
>
> **Estimated time**: 8–12 hours

---

## Table of Contents

1. [What to build](#1-what-to-build)
2. [Project setup](#2-project-setup)
3. [Package structure](#3-package-structure)
4. [Entity layer](#4-entity-layer)
5. [DTO layer](#5-dto-layer)
6. [Repository layer](#6-repository-layer)
7. [Service layer](#7-service-layer)
8. [Controller layer](#8-controller-layer)
9. [Exception handling](#9-exception-handling)
10. [Configuration](#10-configuration)
11. [Tests](#11-tests)
12. [Phase 1 → Phase 2 migration plan](#12-phase-1--phase-2-migration-plan)

---

## 1. What to build

```
HTTP POST /api/logs
    │
    ▼
LogController                  ← @Valid + delegate
    │
    ▼
LogIngestionService            ← orchestrate: resolve → save → anomaly check
    ├── ServiceRegistry        ← name→UUID cache (ConcurrentHashMap)
    ├── LogRepository          ← JPA bulk insert
    └── SlidingWindowAnalyzer  ← Z-score per service
            │
            ▼ if Z > 2.5
        AnomalyRepository      ← INSERT anomaly row
            │
            ▼
        (ready for Phase 4 AI analysis)

HTTP GET /api/logs
HTTP GET /api/anomalies
    │
    ▼
LogQueryService                ← all read operations, readOnly=true
```

---

## 2. Project setup

### `pom.xml` — key dependencies

```xml
<!-- Spring Boot parent -->
<parent>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-parent</artifactId>
  <version>3.2.4</version>
</parent>

<dependencies>
  <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
  <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-validation</artifactId></dependency>
  <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-jpa</artifactId></dependency>
  <dependency><groupId>com.mysql</groupId><artifactId>mysql-connector-j</artifactId><scope>runtime</scope></dependency>
  <dependency><groupId>org.flywaydb</groupId><artifactId>flyway-mysql</artifactId></dependency>
  <dependency><groupId>org.projectlombok</groupId><artifactId>lombok</artifactId><optional>true</optional></dependency>
  <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
  <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-test</artifactId><scope>test</scope></dependency>
  <dependency><groupId>com.h2database</groupId><artifactId>h2</artifactId><scope>test</scope></dependency>
</dependencies>
```

### `application.yml`

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/logmind?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
    username: ${MYSQL_USER:logmind}
    password: ${MYSQL_PASSWORD:logmind_secret}
    hikari:
      maximum-pool-size: 10
      minimum-idle: 2
      connection-timeout: 30000

  jpa:
    hibernate:
      ddl-auto: validate          # Flyway owns schema. JPA only validates.
    show-sql: false

  flyway:
    enabled: true
    locations: classpath:db/migration
    baseline-on-migrate: true

server:
  port: 8080

logmind:
  anomaly:
    z-score-threshold: 2.5
    window-size-minutes: 60
    bucket-size-seconds: 60
    min-data-points: 20
```

### `application-test.yml`

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=MySQL
    driver-class-name: org.h2.Driver
  jpa:
    hibernate:
      ddl-auto: create-drop
  flyway:
    enabled: false   # H2 uses ddl-auto instead
```

---

## 3. Package structure

```
com.logmind/
├── LogmindApplication.java          ← @SpringBootApplication + @EnableScheduling
│
├── config/
│   └── JacksonConfig.java           ← ObjectMapper with JavaTimeModule
│
├── entity/
│   ├── LogLevel.java                ← enum DEBUG INFO WARN ERROR FATAL
│   ├── ServiceEntity.java           ← services table
│   ├── LogEntity.java               ← logs table (partitioned)
│   └── AnomalyEntity.java           ← anomalies table
│
├── dto/
│   ├── IngestRequest.java           ← POST body + @Valid annotations
│   ├── IngestResponse.java          ← {accepted, serviceId, ingestedAt}
│   ├── LogResponse.java             ← GET /api/logs item
│   ├── AnomalyResponse.java         ← GET /api/anomalies item
│   └── PagedResponse.java           ← generic {content, page, total, totalPages}
│
├── repository/
│   ├── ServiceRepository.java       ← findByName()
│   ├── LogRepository.java           ← queries + native timeseries
│   └── AnomalyRepository.java       ← findByServiceId + date filters
│
├── service/
│   ├── ServiceRegistry.java         ← name→UUID cache (ConcurrentHashMap)
│   ├── WindowState.java             ← per-service bucket store + eviction + Z-score
│   ├── SlidingWindowAnalyzer.java   ← holds all WindowStates, classify severity
│   ├── LogIngestionService.java     ← ingest() + ingestBatch() + anomaly trigger
│   └── LogQueryService.java         ← getLogs() + getAnomalies() + timeseries
│
├── controller/
│   ├── LogController.java           ← POST /api/logs, POST /batch, GET /api/logs
│   └── AnomalyController.java       ← GET /api/anomalies
│
└── exception/
    └── GlobalExceptionHandler.java  ← RFC 7807 ProblemDetail for all errors
```

---

## 4. Entity layer

### 4.1 `LogLevel.java`

```java
package com.logmind.entity;

public enum LogLevel {
    DEBUG, INFO, WARN, ERROR, FATAL
}
```

**Why a separate enum file**: Shared by `LogEntity` (database) and `IngestRequest` (HTTP).
One definition. The anomaly detector checks `level == ERROR || level == FATAL` using this enum.

---

### 4.2 `ServiceEntity.java`

```java
@Entity
@Table(name = "services")
@Getter @Setter @NoArgsConstructor
public class ServiceEntity {

    @Id
    @Column(length = 36)
    private String id;

    @Column(nullable = false, unique = true, length = 100)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    private void prePersist() {
        if (this.id == null) this.id = UUID.randomUUID().toString();
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    private void preUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    public static ServiceEntity of(String name) {
        ServiceEntity s = new ServiceEntity();
        s.name = name;
        return s;
    }
}
```

**Method details**:

| Method | Purpose |
|--------|---------|
| `of(name)` | Static factory. `id` is null here — set by `@PrePersist` on save. Never use `new ServiceEntity()` directly in business code. |
| `prePersist()` | Runs before every `INSERT`. Sets UUID, timestamps. Guarded with `if id == null` so it doesn't overwrite an explicitly set id. |
| `preUpdate()` | Runs before every `UPDATE`. Refreshes `updatedAt`. |

---

### 4.3 `LogEntity.java`

```java
@Entity
@Table(name = "logs")
@Getter @Setter @NoArgsConstructor
public class LogEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "service_id", nullable = false, length = 36)
    private String serviceId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private LogLevel level;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String message;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "JSON")
    private Map<String, Object> metadata;

    @Column(nullable = false)
    private LocalDateTime timestamp;

    public static LogEntity of(String serviceId, LogLevel level, String message,
                               Map<String, Object> metadata, LocalDateTime timestamp) {
        LogEntity e = new LogEntity();
        e.serviceId = serviceId;
        e.level = level;
        e.message = message;
        e.metadata = metadata;
        e.timestamp = timestamp;
        return e;
    }
}
```

**Key design notes**:
- `@GeneratedValue(IDENTITY)` — MySQL `AUTO_INCREMENT`. Fast sequential inserts.
- `@JdbcTypeCode(SqlTypes.JSON)` — Hibernate 6 serializes `Map<String,Object>` to a JSON
  string automatically. No manual `ObjectMapper` call needed.
- **Why not `@Id` on (id, timestamp)?** JPA cannot manage composite PKs that include the
  partition column. The composite PK is enforced by Flyway DDL only. JPA only sees `id`.

---

### 4.4 `AnomalyEntity.java`

```java
@Entity
@Table(name = "anomalies")
@Getter @Setter @NoArgsConstructor
public class AnomalyEntity {

    @Id
    @Column(length = 36)
    private String id;

    @Column(name = "service_id", nullable = false, length = 36)
    private String serviceId;

    @Column(name = "detected_at", nullable = false)
    private LocalDateTime detectedAt;

    @Column(name = "window_start", nullable = false)
    private LocalDateTime windowStart;

    @Column(name = "window_end", nullable = false)
    private LocalDateTime windowEnd;

    @Column(name = "error_count", nullable = false)
    private int errorCount;

    @Column(name = "baseline_mean", nullable = false)
    private double baselineMean;

    @Column(name = "baseline_stddev", nullable = false)
    private double baselineStddev;

    @Column(name = "z_score", nullable = false)
    private double zScore;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Severity severity;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "log_sample", nullable = false, columnDefinition = "JSON")
    private List<Map<String, Object>> logSample;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private AnomalyStatus status = AnomalyStatus.OPEN;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    private void prePersist() {
        if (this.id == null) this.id = UUID.randomUUID().toString();
        this.createdAt = LocalDateTime.now();
    }

    public enum Severity         { LOW, MEDIUM, HIGH, CRITICAL }
    public enum AnomalyStatus    { OPEN, ANALYZING, RESOLVED, FALSE_POSITIVE }
}
```

**Why nested enums?** `Severity` and `AnomalyStatus` are semantically owned by `AnomalyEntity`.
Keeping them nested makes the relationship explicit. Reference them as
`AnomalyEntity.Severity.HIGH` — no ambiguity.

---

## 5. DTO layer

### 5.1 `IngestRequest.java`

```java
@Getter @Setter @NoArgsConstructor
public class IngestRequest {

    @NotBlank(message = "service name is required")
    @Size(max = 100, message = "service name must be 100 characters or fewer")
    @Pattern(regexp = "^[a-zA-Z0-9_-]+$",
             message = "service name may only contain letters, numbers, hyphens, underscores")
    private String service;

    @NotNull(message = "log level is required")
    private LogLevel level;

    @NotBlank(message = "message is required")
    @Size(max = 10000, message = "message must be 10,000 characters or fewer")
    private String message;

    private Map<String, Object> metadata;    // optional

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    private LocalDateTime timestamp;         // optional, defaults to now()
}
```

**Validation field-by-field**:

| Field | Rule | Why |
|-------|------|-----|
| `service` | `@Pattern([a-zA-Z0-9_-]+)` | Spaces and special chars break Kafka partition keys in Phase 2. Enforced from day one. |
| `level` | `@NotNull` | Spring auto-converts `"ERROR"` string → `LogLevel.ERROR`. If unknown value: 400 automatically. |
| `message` | `@Size(max=10000)` | Prevents 10MB log messages from filling the DB. |
| `metadata` | No validation | Any JSON object. Stored as-is. |
| `timestamp` | Optional | Allows clients to send historical timestamps. Defaults to `now()` in service. |

---

### 5.2 `IngestResponse.java`

```java
@Getter @Builder
public class IngestResponse {
    private int accepted;
    private String serviceId;
    private LocalDateTime ingestedAt;

    public static IngestResponse of(int accepted, String serviceId) {
        return IngestResponse.builder()
                .accepted(accepted)
                .serviceId(serviceId)
                .ingestedAt(LocalDateTime.now())
                .build();
    }
}
```

**Why 202 Accepted (not 200 OK)?** `202` means "accepted for processing" — the request was
received and will be processed, but processing may not be complete. This is semantically
correct even in Phase 1 (synchronous) and stays correct in Phase 2 (async Kafka).
**No HTTP status change needed when adding Kafka.**

---

### 5.3 `LogResponse.java`

```java
@Getter @Builder
public class LogResponse {
    private Long id;
    private String serviceId;
    private LogLevel level;
    private String message;
    private Map<String, Object> metadata;
    private LocalDateTime timestamp;

    public static LogResponse from(LogEntity entity) {
        return LogResponse.builder()
                .id(entity.getId())
                .serviceId(entity.getServiceId())
                .level(entity.getLevel())
                .message(entity.getMessage())
                .metadata(entity.getMetadata())
                .timestamp(entity.getTimestamp())
                .build();
    }
}
```

**Why `from()` factory?** Entity → DTO mapping in one place. If a DB column is renamed, only
`from()` changes. The controller never sees entity internals.

---

### 5.4 `AnomalyResponse.java`

```java
@Getter @Builder
public class AnomalyResponse {
    private String id;
    private String serviceId;
    private LocalDateTime detectedAt;
    private LocalDateTime windowStart;
    private LocalDateTime windowEnd;
    private int errorCount;
    private double baselineMean;
    private double baselineStddev;
    private double zScore;
    private AnomalyEntity.Severity severity;
    private AnomalyEntity.AnomalyStatus status;
    private List<Map<String, Object>> logSample;
    private LocalDateTime createdAt;

    public static AnomalyResponse from(AnomalyEntity entity) {
        return AnomalyResponse.builder()
                /* map all fields */
                .build();
    }
}
```

---

### 5.5 `PagedResponse<T>`

```java
@Getter @Builder
public class PagedResponse<T> {
    private List<T> content;
    private int page;
    private int size;
    private long totalElements;
    private int totalPages;

    public static <T> PagedResponse<T> of(List<T> content, int page, int size, long total) {
        return PagedResponse.<T>builder()
                .content(content)
                .page(page)
                .size(size)
                .totalElements(total)
                .totalPages((int) Math.ceil((double) total / size))
                .build();
    }
}
```

---

## 6. Repository layer

### 6.1 `ServiceRepository`

```java
@Repository
public interface ServiceRepository extends JpaRepository<ServiceEntity, String> {
    Optional<ServiceEntity> findByName(String name);
}
```

Used only by `ServiceRegistry.findOrCreate()`. One custom method.

---

### 6.2 `LogRepository`

```java
@Repository
public interface LogRepository extends JpaRepository<LogEntity, Long> {

    // Filter by service only
    Page<LogEntity> findByServiceIdOrderByTimestampDesc(String serviceId, Pageable pageable);

    // Filter by service + level
    Page<LogEntity> findByServiceIdAndLevelOrderByTimestampDesc(
            String serviceId, LogLevel level, Pageable pageable);

    // No filter — all services
    Page<LogEntity> findAllByOrderByTimestampDesc(Pageable pageable);

    // Used by anomaly detector to populate log_sample
    @Query("""
        SELECT l FROM LogEntity l
        WHERE l.serviceId = :serviceId
          AND l.level IN ('ERROR', 'FATAL')
          AND l.timestamp >= :since
        ORDER BY l.timestamp DESC
        """)
    List<LogEntity> findRecentErrors(
            @Param("serviceId") String serviceId,
            @Param("since") LocalDateTime since);

    // Native SQL for timeseries chart — GROUP BY minute bucket
    @Query(value = """
        SELECT
            DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00') AS bucket,
            COUNT(*) AS error_count
        FROM logs
        WHERE service_id = :serviceId
          AND level IN ('ERROR', 'FATAL')
          AND timestamp >= :since
        GROUP BY bucket
        ORDER BY bucket ASC
        """, nativeQuery = true)
    List<Object[]> findErrorRateTimeseries(
            @Param("serviceId") String serviceId,
            @Param("since") LocalDateTime since);
}
```

**Query method naming**: Spring Data JPA derives the SQL from the method name.
`findByServiceIdAndLevelOrderByTimestampDesc` → `WHERE service_id=? AND level=? ORDER BY timestamp DESC`.

**Native query for timeseries**: JPQL cannot use `DATE_FORMAT()`. Native SQL is the right
call here. Returns `List<Object[]>` — mapped to `Map` in `LogQueryService`.

---

### 6.3 `AnomalyRepository`

```java
@Repository
public interface AnomalyRepository extends JpaRepository<AnomalyEntity, String> {

    Page<AnomalyEntity> findAllByOrderByDetectedAtDesc(Pageable pageable);

    Page<AnomalyEntity> findByServiceIdOrderByDetectedAtDesc(
            String serviceId, Pageable pageable);

    // For Phase 4 dedup check
    List<AnomalyEntity> findByServiceIdAndDetectedAtAfterOrderByDetectedAtDesc(
            String serviceId, LocalDateTime after);
}
```

---

## 7. Service layer

### 7.1 `ServiceRegistry.java`

**Purpose**: Resolves service name → UUID with in-process cache.

**Why needed**: Without cache, every `POST /api/logs` fires a
`SELECT id FROM services WHERE name = ?`. At 100 req/s with 5 services = 100 DB reads/s
for data that never changes once registered.

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class ServiceRegistry {

    private final ServiceRepository serviceRepository;
    private final ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();

    @Transactional
    public String resolveId(String serviceName) {
        return cache.computeIfAbsent(serviceName, this::findOrCreate);
    }

    private String findOrCreate(String name) {
        return serviceRepository.findByName(name)
                .map(ServiceEntity::getId)
                .orElseGet(() -> {
                    log.info("Registering new service: {}", name);
                    return serviceRepository.save(ServiceEntity.of(name)).getId();
                });
    }
}
```

**Method details**:

| Method | Description |
|--------|-------------|
| `resolveId(serviceName)` | `@Transactional`. `ConcurrentHashMap.computeIfAbsent` is atomic — if two threads resolve the same new service name simultaneously, only one executes `findOrCreate`, the other waits and gets the cached result. |
| `findOrCreate(name)` | Called inside `computeIfAbsent` lambda. `findByName` → on miss: save new `ServiceEntity` → return new UUID. The `@Transactional` from `resolveId` covers this DB operation. |

---

### 7.2 `WindowState.java`

**Purpose**: Stores the sliding window for ONE service. Computes Z-score statistics.

**NOT a Spring bean** — instantiated directly by `SlidingWindowAnalyzer` per service.

```java
public class WindowState {

    private final String serviceId;
    private final long windowSizeMs;
    private final long bucketSizeMs;

    // bucketKey → error count
    private final LinkedHashMap<Long, Integer> buckets = new LinkedHashMap<>();

    // rolling sample of last 20 error logs (for AI prompt context)
    private final LinkedList<Map<String, Object>> logSample = new LinkedList<>();
    private static final int MAX_SAMPLE_SIZE = 20;

    public WindowState(String serviceId, int windowSizeMinutes, int bucketSizeSeconds) {
        this.serviceId   = serviceId;
        this.windowSizeMs = (long) windowSizeMinutes * 60 * 1000;
        this.bucketSizeMs = (long) bucketSizeSeconds * 1000;
    }

    public void addLog(LogLevel level, long timestampMs, Map<String, Object> logEntry) {
        if (level == LogLevel.ERROR || level == LogLevel.FATAL) {
            long bucketKey = timestampMs / bucketSizeMs;
            buckets.merge(bucketKey, 1, Integer::sum);

            // rolling sample: evict oldest if full
            if (logSample.size() >= MAX_SAMPLE_SIZE) logSample.removeFirst();
            logSample.addLast(logEntry);
        }
        evictStale(timestampMs);
    }

    private void evictStale(long nowMs) {
        long cutoffBucket = (nowMs - windowSizeMs) / bucketSizeMs;
        buckets.entrySet().removeIf(e -> e.getKey() < cutoffBucket);
    }

    public Stats getStats(int minDataPoints) {
        if (buckets.size() < minDataPoints) return null;   // cold start

        List<Integer> values = new ArrayList<>(buckets.values());
        int n = values.size();

        double sum = 0;
        for (int v : values) sum += v;
        double mean = sum / n;

        double variance = 0;
        for (int v : values) variance += Math.pow(v - mean, 2);
        double stddev = Math.sqrt(variance / n);

        if (stddev == 0) return null;   // flat line — no spike possible

        long latestKey = buckets.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
        int currentCount = buckets.getOrDefault(latestKey, 0);
        double zScore = (currentCount - mean) / stddev;

        long windowEndMs   = latestKey * bucketSizeMs + bucketSizeMs;
        long windowStartMs = windowEndMs - windowSizeMs;

        return new Stats(mean, stddev, currentCount, zScore, windowStartMs, windowEndMs);
    }

    public List<Map<String, Object>> getLogSample() {
        return new ArrayList<>(logSample);  // defensive copy
    }

    // Immutable value object — safe to pass across method boundaries
    public record Stats(
        double mean,
        double stddev,
        int    currentCount,
        double zScore,
        long   windowStartMs,
        long   windowEndMs
    ) {}
}
```

**Method details**:

| Method | Key behaviour |
|--------|--------------|
| `addLog(level, timestampMs, logEntry)` | Only `ERROR`/`FATAL` increment the bucket. `buckets.merge(key, 1, Integer::sum)` is cleaner than get-then-put. Always calls `evictStale()` — eviction is on every write, not on a background timer. |
| `evictStale(nowMs)` | `cutoffBucket = (nowMs - windowSizeMs) / bucketSizeMs`. Removes all entries with key below the cutoff. `LinkedHashMap` preserves insertion order so oldest keys are at the front — `removeIf` is efficient. |
| `getStats(minDataPoints)` | **Guard 1**: `buckets.size() < minDataPoints` → `null`. 20 buckets = 20 minutes of baseline. Before that, stddev is computed from too few points and Z-score is meaningless. **Guard 2**: `stddev == 0` → `null`. Flat line means no variance — division by zero. Z-score only makes sense when there is variance. |
| `getLogSample()` | Returns a `new ArrayList<>(logSample)` — defensive copy. Callers cannot mutate the internal `logSample`. |

**Bucket key formula**:
```
bucketKey = timestampMs / bucketSizeMs

Example with bucketSizeMs = 60_000 (1 minute):
  10:30:00.000 → 1736935800000 / 60000 = 28948930
  10:30:45.500 → 1736935845500 / 60000 = 28948930  ← same bucket!
  10:31:00.000 → 1736935860000 / 60000 = 28948931  ← new bucket

Integer division floors to the start of the bucket.
All errors within the same 60-second window map to the same key.
```

---

### 7.3 `SlidingWindowAnalyzer.java`

**Purpose**: Holds all `WindowState` instances. Routes logs to the correct window.
Returns `DetectedAnomaly` when Z-score threshold exceeded.

```java
@Slf4j
@Component
public class SlidingWindowAnalyzer {

    @Value("${logmind.anomaly.window-size-minutes:60}")
    private int windowSizeMinutes;

    @Value("${logmind.anomaly.bucket-size-seconds:60}")
    private int bucketSizeSeconds;

    @Value("${logmind.anomaly.z-score-threshold:2.5}")
    private double zScoreThreshold;

    @Value("${logmind.anomaly.min-data-points:20}")
    private int minDataPoints;

    private final ConcurrentHashMap<String, WindowState> windows = new ConcurrentHashMap<>();

    public Optional<DetectedAnomaly> analyze(String serviceId, LogLevel level,
                                             long timestampMs,
                                             Map<String, Object> logEntry) {

        WindowState window = windows.computeIfAbsent(
                serviceId,
                id -> new WindowState(id, windowSizeMinutes, bucketSizeSeconds)
        );

        window.addLog(level, timestampMs, logEntry);

        WindowState.Stats stats = window.getStats(minDataPoints);
        if (stats == null) return Optional.empty();
        if (stats.zScore() <= zScoreThreshold) return Optional.empty();

        log.warn("Anomaly: service={} zScore={} severity={}",
                serviceId, String.format("%.2f", stats.zScore()), classify(stats.zScore()));

        return Optional.of(new DetectedAnomaly(
                serviceId,
                stats,
                classify(stats.zScore()),
                window.getLogSample()
        ));
    }

    private AnomalyEntity.Severity classify(double zScore) {
        if (zScore >= 5.0) return AnomalyEntity.Severity.CRITICAL;
        if (zScore >= 3.5) return AnomalyEntity.Severity.HIGH;
        if (zScore >= 2.5) return AnomalyEntity.Severity.MEDIUM;
        return AnomalyEntity.Severity.LOW;
    }

    public record DetectedAnomaly(
        String serviceId,
        WindowState.Stats stats,
        AnomalyEntity.Severity severity,
        List<Map<String, Object>> logSample
    ) {}
}
```

**Severity thresholds**:

| Z-score | Severity | Probability under normal dist |
|---------|---------|-------------------------------|
| ≥ 5.0 | CRITICAL | ~1 in 3,500,000 |
| ≥ 3.5 | HIGH | ~1 in 4,300 |
| ≥ 2.5 | MEDIUM | ~1 in 160 |
| < 2.5 | (no anomaly) | — |

---

### 7.4 `LogIngestionService.java`

**Purpose**: Orchestrate full ingest flow. The single class that changes in Phase 2.

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class LogIngestionService {

    private final LogRepository logRepository;
    private final AnomalyRepository anomalyRepository;
    private final ServiceRegistry serviceRegistry;
    private final SlidingWindowAnalyzer analyzer;

    @Transactional
    public IngestResponse ingest(IngestRequest request) {
        return ingestBatch(List.of(request));
    }

    @Transactional
    public IngestResponse ingestBatch(List<IngestRequest> requests) {
        if (requests.isEmpty())
            throw new IllegalArgumentException("Batch must contain at least one entry");
        if (requests.size() > 1000)
            throw new IllegalArgumentException("Batch size cannot exceed 1000 entries");

        String serviceName = requests.get(0).getService();
        String serviceId   = serviceRegistry.resolveId(serviceName);

        List<LogEntity> entities = requests.stream()
                .map(req -> toEntity(req, serviceId))
                .toList();

        // ─── PHASE 2 CHANGE POINT ───────────────────────────────────────
        // Phase 1: logRepository.saveAll(entities)
        // Phase 2: replace with kafkaTemplate.send("raw-logs", serviceId, payload)
        // Everything below this line moves to RawLogConsumer
        // ────────────────────────────────────────────────────────────────
        logRepository.saveAll(entities);

        entities.forEach(log -> runAnomalyCheck(log, serviceId, serviceName));

        return IngestResponse.of(entities.size(), serviceId);
    }

    private LogEntity toEntity(IngestRequest req, String serviceId) {
        LocalDateTime ts = req.getTimestamp() != null
                ? req.getTimestamp()
                : LocalDateTime.now();
        return LogEntity.of(serviceId, req.getLevel(), req.getMessage(), req.getMetadata(), ts);
    }

    private void runAnomalyCheck(LogEntity log, String serviceId, String serviceName) {
        long tsMs = log.getTimestamp().toInstant(ZoneOffset.UTC).toEpochMilli();

        Map<String, Object> entry = new HashMap<>();
        entry.put("timestamp", log.getTimestamp().toString());
        entry.put("level",     log.getLevel().name());
        entry.put("message",   log.getMessage());
        if (log.getMetadata() != null) entry.put("metadata", log.getMetadata());

        analyzer.analyze(serviceId, log.getLevel(), tsMs, entry)
                .ifPresent(anomaly -> persistAnomaly(anomaly, serviceName));
    }

    private void persistAnomaly(SlidingWindowAnalyzer.DetectedAnomaly anomaly,
                                String serviceName) {
        WindowState.Stats stats = anomaly.stats();

        AnomalyEntity entity = new AnomalyEntity();
        entity.setServiceId(anomaly.serviceId());
        entity.setDetectedAt(LocalDateTime.now());
        entity.setWindowStart(toLocalDateTime(stats.windowStartMs()));
        entity.setWindowEnd(toLocalDateTime(stats.windowEndMs()));
        entity.setErrorCount(stats.currentCount());
        entity.setBaselineMean(stats.mean());
        entity.setBaselineStddev(stats.stddev());
        entity.setZScore(stats.zScore());
        entity.setSeverity(anomaly.severity());
        entity.setLogSample(anomaly.logSample());

        AnomalyEntity saved = anomalyRepository.save(entity);
        log.warn("Anomaly persisted id={} service={} severity={} z={}",
                saved.getId(), serviceName, saved.getSeverity(),
                String.format("%.2f", saved.getZScore()));
    }

    private LocalDateTime toLocalDateTime(long epochMs) {
        return LocalDateTime.ofEpochSecond(epochMs / 1000, 0, ZoneOffset.UTC);
    }
}
```

---

### 7.5 `LogQueryService.java`

```java
@Service
@RequiredArgsConstructor
public class LogQueryService {

    private final LogRepository logRepository;
    private final AnomalyRepository anomalyRepository;

    @Transactional(readOnly = true)
    public PagedResponse<LogResponse> getLogs(String serviceId, LogLevel level,
                                              int page, int size) {
        PageRequest pr = PageRequest.of(page, size, Sort.by("timestamp").descending());
        Page<LogResponse> result;

        if (serviceId != null && level != null)
            result = logRepository.findByServiceIdAndLevelOrderByTimestampDesc(serviceId, level, pr)
                                  .map(LogResponse::from);
        else if (serviceId != null)
            result = logRepository.findByServiceIdOrderByTimestampDesc(serviceId, pr)
                                  .map(LogResponse::from);
        else
            result = logRepository.findAllByOrderByTimestampDesc(pr)
                                  .map(LogResponse::from);

        return PagedResponse.of(result.getContent(), page, size, result.getTotalElements());
    }

    @Transactional(readOnly = true)
    public PagedResponse<AnomalyResponse> getAnomalies(String serviceId, int page, int size) {
        PageRequest pr = PageRequest.of(page, size, Sort.by("detectedAt").descending());
        Page<AnomalyResponse> result = serviceId != null
                ? anomalyRepository.findByServiceIdOrderByDetectedAtDesc(serviceId, pr)
                                   .map(AnomalyResponse::from)
                : anomalyRepository.findAllByOrderByDetectedAtDesc(pr)
                                   .map(AnomalyResponse::from);
        return PagedResponse.of(result.getContent(), page, size, result.getTotalElements());
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> getErrorRateTimeseries(String serviceId, int windowMins) {
        LocalDateTime since = LocalDateTime.now().minusMinutes(windowMins);
        return logRepository.findErrorRateTimeseries(serviceId, since)
                .stream()
                .map(row -> {
                    Map<String, Object> point = new LinkedHashMap<>();
                    point.put("bucket",     row[0].toString());
                    point.put("errorCount", ((Number) row[1]).longValue());
                    return point;
                })
                .toList();
    }
}
```

**Why `@Transactional(readOnly = true)`?**
1. Hibernate skips dirty checking — no need to snapshot entity state.
2. Spring signals the connection pool that read replicas can be used.
3. Some JDBC drivers/proxies optimise read-only connections.

---

## 8. Controller layer

### 8.1 `LogController.java`

```java
@RestController
@RequestMapping("/api/logs")
@RequiredArgsConstructor
public class LogController {

    private final LogIngestionService ingestionService;
    private final LogQueryService queryService;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public IngestResponse ingest(@Valid @RequestBody IngestRequest request) {
        return ingestionService.ingest(request);
    }

    @PostMapping("/batch")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public IngestResponse ingestBatch(@Valid @RequestBody List<IngestRequest> requests) {
        return ingestionService.ingestBatch(requests);
    }

    @GetMapping
    public PagedResponse<LogResponse> getLogs(
            @RequestParam(required = false) String serviceId,
            @RequestParam(required = false) LogLevel level,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {
        return queryService.getLogs(serviceId, level, page, Math.min(size, 200));
    }

    @GetMapping("/timeseries")
    public List<Map<String, Object>> getTimeseries(
            @RequestParam String serviceId,
            @RequestParam(defaultValue = "60") int window) {
        return queryService.getErrorRateTimeseries(serviceId, Math.min(window, 1440));
    }
}
```

### 8.2 `AnomalyController.java`

```java
@RestController
@RequestMapping("/api/anomalies")
@RequiredArgsConstructor
public class AnomalyController {

    private final LogQueryService queryService;

    @GetMapping
    public PagedResponse<AnomalyResponse> getAnomalies(
            @RequestParam(required = false) String serviceId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return queryService.getAnomalies(serviceId, page, Math.min(size, 100));
    }
}
```

---

## 9. Exception handling

### `GlobalExceptionHandler.java`

```java
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> errors = ex.getBindingResult().getFieldErrors()
                .stream()
                .collect(Collectors.toMap(
                        FieldError::getField,
                        fe -> fe.getDefaultMessage() != null ? fe.getDefaultMessage() : "invalid",
                        (a, b) -> a
                ));
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                errors.size() + " field(s) failed validation");
        pd.setType(URI.create("https://logmind.dev/errors/validation-failed"));
        pd.setTitle("Validation failed");
        pd.setProperty("timestamp", Instant.now());
        pd.setProperty("errors", errors);
        return pd;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgument(IllegalArgumentException ex) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        pd.setType(URI.create("https://logmind.dev/errors/bad-request"));
        pd.setTitle("Bad request");
        pd.setProperty("timestamp", Instant.now());
        return pd;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("Unhandled exception", ex);
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred");
        pd.setType(URI.create("https://logmind.dev/errors/internal-error"));
        pd.setTitle("Internal server error");
        pd.setProperty("timestamp", Instant.now());
        return pd;
    }
}
```

**Error response shape (RFC 7807)**:
```json
{
  "type":      "https://logmind.dev/errors/validation-failed",
  "title":     "Validation failed",
  "status":    400,
  "detail":    "2 field(s) failed validation",
  "timestamp": "2025-01-15T10:30:00Z",
  "errors":    { "service": "must not be blank", "level": "must not be null" }
}
```

---

## 10. Configuration

### `LogmindApplication.java`

```java
@SpringBootApplication
@EnableScheduling   // needed for Phase 2 LogBatcher @Scheduled flush
public class LogmindApplication {
    public static void main(String[] args) {
        SpringApplication.run(LogmindApplication.class, args);
    }
}
```

`@EnableScheduling` is a no-op in Phase 1 but avoids touching the entry point in Phase 2.

### `JacksonConfig.java`

```java
@Configuration
public class JacksonConfig {
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
```

Without this, `LocalDateTime` serializes as `[2025, 1, 15, 10, 30, 0]`. With it: `"2025-01-15T10:30:00"`.

---

## 11. Tests

### `WindowStateTest.java` — unit tests (no Spring context)

| Test | Verifies |
|------|---------|
| `coldStart_returnsNull` | `getStats()` returns null when < minDataPoints buckets |
| `flatLine_returnsNull` | `getStats()` returns null when stddev = 0 |
| `detectsSpike` | Z > 2.5 when current bucket >> baseline |
| `evictsStaleBuckets` | Old bucket counts excluded from mean after eviction |
| `nonErrorLevels_dontIncrement` | INFO/WARN don't affect bucket counts |
| `fatalLogs_areCountedAsErrors` | FATAL increments same bucket as ERROR |
| `logSample_cappedAt20` | Sample size never exceeds 20 |

### `LogControllerTest.java` — integration tests (H2)

| Test | Verifies |
|------|---------|
| `ingestSingle_returns202` | Valid body → 202, accepted=1, serviceId non-empty |
| `ingestSingle_missingFields_returns400` | Empty body → 400 with `errors` map |
| `ingestSingle_invalidServiceName_returns400` | `"my service!!"` → 400 |
| `ingestBatch_returnsCorrectAcceptedCount` | 5-entry batch → accepted=5 |
| `ingestBatch_tooLarge_returns400` | 1001-entry batch → 400 |
| `getLogs_returnsPaginatedResults` | POST then GET → content array non-empty |
| `getLogs_pageSizeIsCapped` | `?size=9999` → size=200 in response |

---

## 12. Phase 1 → Phase 2 migration plan

The monolith is designed so Phase 2 is a surgical extraction:

```
PHASE 1 — synchronous path:
  HTTP → LogController → LogIngestionService
    → serviceRegistry.resolveId()
    → logRepository.saveAll()           ← LINE THAT CHANGES
    → runAnomalyCheck()                 ← BLOCK THAT MOVES TO CONSUMER

PHASE 2 — async path:
  HTTP → LogController → LogIngestionService
    → serviceRegistry.resolveId()
    → kafkaTemplate.send("raw-logs")    ← replacement (1 line)
    ← return 202 immediately

  [separate process] RawLogConsumer
    → LogBatcher.add()
    → logRepository.saveAll()           ← moved here
    → runAnomalyCheck()                 ← moved here
```

**Files that change in Phase 2**:
- `LogIngestionService.ingestBatch()` — 1 line replaced
- `pom.xml` — add `spring-kafka`
- `application.yml` — add Kafka config
- `docker-compose.yml` — uncomment Kafka + Zookeeper

**Files with zero changes in Phase 2**:
`LogController`, `AnomalyController`, `LogQueryService`, all entities, all DTOs,
all repositories, `ServiceRegistry`, `WindowState`, `SlidingWindowAnalyzer`,
`GlobalExceptionHandler`, `JacksonConfig`, all tests.
