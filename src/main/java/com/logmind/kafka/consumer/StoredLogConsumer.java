package com.logmind.kafka.consumer;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.logmind.dto.StoredLogMessage;
import com.logmind.entity.AnomalyEntity;
import com.logmind.repository.AnomalyRepository;
import com.logmind.service.SlidingWindowAnalyzer;
import com.logmind.service.WindowState;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Consumes persisted logs from {@code stored-logs} and runs Z-score anomaly detection (Phase 2).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StoredLogConsumer {

    private final SlidingWindowAnalyzer analyzer;
    private final AnomalyRepository anomalyRepository;

    @KafkaListener(
            topics = "stored-logs",
            groupId = "anomaly-group",
            containerFactory = "storedLogKafkaListenerContainerFactory")
    public void consume(StoredLogMessage msg, Acknowledgment ack) {
        try {
            long timestampMs = msg.getTimestamp()
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();

            Map<String, Object> logEntry = new HashMap<>();
            logEntry.put("timestamp", msg.getTimestamp().toString());
            logEntry.put("level", msg.getLevel().name());
            logEntry.put("message", msg.getMessage());
            if (msg.getMetadata() != null) {
                logEntry.put("metadata", msg.getMetadata());
            }

            String displayName = msg.getServiceName() != null ? msg.getServiceName() : msg.getServiceId();

            analyzer.analyze(msg.getServiceId(), msg.getLevel(), timestampMs, logEntry)
                    .ifPresent(anomaly -> persistAnomaly(anomaly, displayName));

            ack.acknowledge();
        } catch (Exception e) {
            log.error("stored-logs processing failed for serviceId={}: {}", msg.getServiceId(), e.getMessage(), e);
        }
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

        log.warn("Anomaly persisted: id={} service={} severity={} zScore={}",
                saved.getId(), serviceName, saved.getSeverity(), saved.getZScore());
    }

    private static LocalDateTime toLocalDateTime(long epochMs) {
        return LocalDateTime.ofEpochSecond(epochMs / 1000, 0, ZoneOffset.UTC);
    }
}
