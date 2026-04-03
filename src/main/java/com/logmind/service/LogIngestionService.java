package com.logmind.service;

import com.logmind.dto.IngestRequest;
import com.logmind.dto.IngestResponse;
import com.logmind.entity.AnomalyEntity;
import com.logmind.entity.LogEntity;
import com.logmind.repository.AnomalyRepository;
import com.logmind.repository.LogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Phase 1: synchronous ingestion.
 *
 * Flow:
 *   HTTP request → validate (controller) → ingest() → save to DB → run anomaly check → return
 *
 * In Phase 2 we'll introduce Kafka here:
 *   HTTP request → validate → publish to raw-logs topic → return immediately
 *   (separate consumer will save to DB and run anomaly detection asynchronously)
 *
 * Keeping this all in one method now makes the Phase 2 refactor a clear,
 * explainable extraction — good interview story.
 */
//Lambok
@Slf4j
@Service 
@RequiredArgsConstructor
public class LogIngestionService {

    private final LogRepository logRepository;
    private final AnomalyRepository anomalyRepository;
    private final ServiceRegistry serviceRegistry;
    private final SlidingWindowAnalyzer analyzer;

    /**
     * Ingest a single log entry.
     *
     * @return IngestResponse with accepted count and service ID
     */
    @Transactional
    public IngestResponse ingest(IngestRequest request) {
        return ingestBatch(List.of(request));
    }

    /**
     * Ingest a batch of log entries (up to 1000).
     *
     * All logs in the batch are saved in a single transaction.
     * Anomaly detection runs on each log after the batch is persisted.
     */
    @Transactional
    public IngestResponse ingestBatch(List<IngestRequest> requests) {
        if (requests.isEmpty()) {
            throw new IllegalArgumentException("Batch must contain at least one log entry");
        }
        if (requests.size() > 1000) {
            throw new IllegalArgumentException("Batch size cannot exceed 1000 entries");
        }

        // All logs in a batch must belong to the same service
        // (validated in the controller for single-service endpoint)
        String serviceName = requests.get(0).getService();
        String serviceId = serviceRegistry.resolveId(serviceName);

        List<LogEntity> entities = requests.stream()
                .map(req -> toEntity(req, serviceId))
                .toList();

        logRepository.saveAll(entities);
        log.debug("Saved {} logs for service={}", entities.size(), serviceName);

        // Run anomaly detection on each persisted log.
        // In Phase 1 this is synchronous — it adds ~1ms per log.
        // In Phase 2 this moves to the Kafka consumer so ingestion stays fast.
        entities.forEach(log -> runAnomalyCheck(log, serviceId, serviceName));

        return IngestResponse.of(entities.size(), serviceId);
    }

    // ── private helpers ──────────────────────────────────────

    private LogEntity toEntity(IngestRequest req, String serviceId) {
        LocalDateTime timestamp = req.getTimestamp() != null
                ? req.getTimestamp()
                : LocalDateTime.now();

        return LogEntity.of(serviceId, req.getLevel(), req.getMessage(), req.getMetadata(), timestamp);
    }

    private void runAnomalyCheck(LogEntity log, String serviceId, String serviceName) {
        long timestampMs = log.getTimestamp()
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();

        // Build a simple map representation of this log for the anomaly's log_sample JSON
        Map<String, Object> logEntry = new HashMap<>();
        logEntry.put("timestamp", log.getTimestamp().toString());
        logEntry.put("level", log.getLevel().name());
        logEntry.put("message", log.getMessage());
        if (log.getMetadata() != null) logEntry.put("metadata", log.getMetadata());

        analyzer.analyze(serviceId, log.getLevel(), timestampMs, logEntry)
                .ifPresent(anomaly -> persistAnomaly(anomaly, serviceName));
    }

    private void persistAnomaly(SlidingWindowAnalyzer.DetectedAnomaly anomaly, String serviceName) {
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
        entity.setStatus(AnomalyEntity.AnomalyStatus.OPEN);

        AnomalyEntity saved = anomalyRepository.save(entity);

        log.warn("Anomaly persisted: id={} service={} severity={} zScore={:.2f}",
                saved.getId(), serviceName, saved.getSeverity(), saved.getZScore());
    }

    private LocalDateTime toLocalDateTime(long epochMs) {
        return LocalDateTime.ofEpochSecond(epochMs / 1000, 0, ZoneOffset.UTC);
    }
}