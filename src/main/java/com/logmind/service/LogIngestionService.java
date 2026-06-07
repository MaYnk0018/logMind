package com.logmind.service;

import java.util.List;

import org.aspectj.apache.bcel.classfile.Module.Require;
import org.springframework.stereotype.Service;

import com.logmind.dto.IngestRequest;
import com.logmind.dto.IngestResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Phase 2: HTTP validates → resolve service → publish to {@code raw-logs} → return.
 * Storage runs asynchronously here; anomaly detection consumes {@code stored-logs}
 * in the anomaly-detection-service.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LogIngestionService {

    private final ServiceRegistry serviceRegistry;
    private final LogPublisher logPublisher;

    /**
     * Ingest a single log entry.
     *
     * @return IngestResponse with accepted count and service ID
     */
    public IngestResponse ingest(IngestRequest request) {
        return ingestBatch(List.of(request));
    }

    /**
     * Ingest a batch of log entries (up to 1000). Publishes each log to Kafka; persistence is async.
     */
    // Require One Service Per Batch-> this is current hindering factor for batch ingestion, we can relax this in future by grouping by service in LogBatcher

    public IngestResponse ingestBatch(List<IngestRequest> requests) {
        if (requests.isEmpty()) {
            throw new IllegalArgumentException("Batch must contain at least one log entry");
        }
        if (requests.size() > 1000) {
            throw new IllegalArgumentException("Batch size cannot exceed 1000 entries");
        }

        String serviceName = requests.get(0).getService();
        String serviceId = serviceRegistry.resolveId(serviceName);

        logPublisher.publishBatch(requests, serviceId);
        log.debug("Published {} logs to raw-logs for service={}", requests.size(), serviceName);

        
        return IngestResponse.of(requests.size(), serviceId);
    }
}
