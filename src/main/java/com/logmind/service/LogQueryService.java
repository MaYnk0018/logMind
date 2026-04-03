package com.logmind.service;

import com.logmind.dto.AnomalyResponse;
import com.logmind.dto.LogResponse;
import com.logmind.dto.PagedResponse;
import com.logmind.entity.LogLevel;
import com.logmind.repository.AnomalyRepository;
import com.logmind.repository.LogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class LogQueryService {

    private final LogRepository logRepository;
    private final AnomalyRepository anomalyRepository;

    @Transactional(readOnly = true)
    public PagedResponse<LogResponse> getLogs(String serviceId, LogLevel level, int page, int size) {
        PageRequest pageRequest = PageRequest.of(page, size, Sort.by("timestamp").descending());

        Page<LogResponse> result;

        if (serviceId != null && level != null) {
            result = logRepository
                    .findByServiceIdAndLevelOrderByTimestampDesc(serviceId, level, pageRequest)
                    .map(LogResponse::from);
        } else if (serviceId != null) {
            result = logRepository
                    .findByServiceIdOrderByTimestampDesc(serviceId, pageRequest)
                    .map(LogResponse::from);
        } else {
            result = logRepository
                    .findAllByOrderByTimestampDesc(pageRequest)
                    .map(LogResponse::from);
        }

        return PagedResponse.of(result.getContent(), page, size, result.getTotalElements());
    }

    @Transactional(readOnly = true)
    public PagedResponse<AnomalyResponse> getAnomalies(String serviceId, int page, int size) {
        PageRequest pageRequest = PageRequest.of(page, size, Sort.by("detectedAt").descending());

        Page<AnomalyResponse> result = serviceId != null
                ? anomalyRepository.findByServiceIdOrderByDetectedAtDesc(serviceId, pageRequest).map(AnomalyResponse::from)
                : anomalyRepository.findAllByOrderByDetectedAtDesc(pageRequest).map(AnomalyResponse::from);

        return PagedResponse.of(result.getContent(), page, size, result.getTotalElements());
    }

    /**
     * Error rate timeseries for dashboard chart.
     * Returns a list of { bucket: "2025-01-15 10:30:00", errorCount: 42 }
     */
    @Transactional(readOnly = true)
    public List<Map<String, Object>> getErrorRateTimeseries(String serviceId, int windowMinutes) {
        LocalDateTime since = LocalDateTime.now().minusMinutes(windowMinutes);

        return logRepository.findErrorRateTimeseries(serviceId, since)
                .stream()
                .map(row -> {
                    Map<String, Object> point = new LinkedHashMap<>();
                    point.put("bucket", row[0].toString());
                    point.put("errorCount", ((Number) row[1]).longValue());
                    return point;
                })
                .toList();
    }
}