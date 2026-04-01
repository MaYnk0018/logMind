# LogMind — Phase 1 (Monolith)

Intelligent log aggregator with anomaly detection. Built with Spring Boot + MySQL.

## Stack
- Java 21 + Spring Boot 3.2
- MySQL 8.0 (partitioned by month)
- Flyway (schema migrations)
- Lombok

## Quick Start

### 1. Start MySQL
```bash
docker-compose up -d
# Wait ~10 seconds for MySQL to be ready
docker-compose ps   # confirm logmind-mysql is healthy
```

### 2. Run the app
```bash
./mvnw spring-boot:run
# Or in IntelliJ: Run > LogmindApplication
# Set VM option: -Xmx512m
```

Flyway runs automatically on startup and creates all tables.

### 3. Verify it's working
```bash
curl http://localhost:8080/actuator/health
# → {"status":"UP"}
```

---

## API Reference

### Ingest a single log
```bash
curl -X POST http://localhost:8080/api/logs \
  -H "Content-Type: application/json" \
  -d '{
    "service": "payment-service",
    "level": "ERROR",
    "message": "DB connection timeout after 30s",
    "metadata": { "host": "db-primary", "attempt": 3 }
  }'
```

### Ingest a batch
```bash
curl -X POST http://localhost:8080/api/logs/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"service":"payment-service","level":"ERROR","message":"timeout #1"},
    {"service":"payment-service","level":"ERROR","message":"timeout #2"},
    {"service":"payment-service","level":"INFO","message":"retry succeeded"}
  ]'
```

### Query logs
```bash
# All logs (newest first, paginated)
curl "http://localhost:8080/api/logs?page=0&size=20"

# Filter by service
curl "http://localhost:8080/api/logs?serviceId=<uuid>&size=10"

# Filter by service + level
curl "http://localhost:8080/api/logs?serviceId=<uuid>&level=ERROR"

# Error rate timeseries (for chart)
curl "http://localhost:8080/api/logs/timeseries?serviceId=<uuid>&window=60"
```

### Query anomalies
```bash
curl "http://localhost:8080/api/anomalies"
curl "http://localhost:8080/api/anomalies?serviceId=<uuid>"
```

---

## Trigger an Anomaly (demo script)

The anomaly detector needs 20 data points (buckets) before scoring.
This script sends a baseline of INFO logs, then a burst of ERRORs.

```bash
#!/bin/bash
SERVICE="payment-service"
URL="http://localhost:8080/api/logs"

echo "Phase 1: Sending baseline logs (this fills the 20-bucket minimum)..."
for i in $(seq 1 25); do
  curl -s -X POST $URL \
    -H "Content-Type: application/json" \
    -d "{\"service\":\"$SERVICE\",\"level\":\"INFO\",\"message\":\"Healthy request $i\"}" \
    > /dev/null
done
echo "Baseline done."

echo "Phase 2: Sending ERROR spike..."
for i in $(seq 1 50); do
  curl -s -X POST $URL \
    -H "Content-Type: application/json" \
    -d "{\"service\":\"$SERVICE\",\"level\":\"ERROR\",\"message\":\"DB connection refused attempt $i\",\"metadata\":{\"attempt\":$i}}" \
    > /dev/null &
done
wait
echo "Spike sent. Check anomalies:"
curl -s "http://localhost:8080/api/anomalies" | python3 -m json.tool
```

---

## Running Tests
```bash
./mvnw test
```
Tests use H2 in-memory database — no Docker required.

---

## Phase 2 Preview (Kafka)

When ready to add Kafka:
1. Uncomment the `kafka` and `zookeeper` blocks in `docker-compose.yml`
2. Add `spring-kafka` dependency to `pom.xml`
3. Replace the `logRepository.saveAll()` call in `LogIngestionService.ingestBatch()`
   with a `KafkaTemplate.send()` call
4. Create a `@KafkaListener` consumer that saves to DB and calls the analyzer

The `@EnableScheduling` annotation is already in place for the LogBatcher timer.

---

## Interview Talking Points

**On the schema:**
"I used MySQL range partitioning by `YEAR * 100 + MONTH`. When I need to purge logs older than 6 months, it's `ALTER TABLE logs DROP PARTITION p202501` — O(1). A `DELETE WHERE timestamp < ?` on an unpartitioned table would take minutes."

**On the anomaly detection:**
"The Z-score is calculated per service against its own rolling baseline. A service that normally generates 50 errors/min spiking to 60 has a Z-score of ~0.5 — not notable. A service that normally generates 0 errors spiking to 5 has a Z-score of >> 2.5 — that's the real alert. Fixed thresholds can't distinguish these two situations."

**On the cold start:**
"I require at least 20 data points before scoring. Before that, the stddev is unreliable — one bucket with 3 errors when the mean is 0 would generate an infinite Z-score. The 20-point minimum gives the baseline time to stabilize."

**On Phase 1 → Phase 2 evolution:**
"The ingestion and storage are currently synchronous in one method. When I add Kafka in Phase 2, I replace one line — `logRepository.saveAll()` — with `kafkaTemplate.send()`. The rest of the code stays the same. I designed the service layer boundaries with this extraction in mind."