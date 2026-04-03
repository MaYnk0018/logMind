package com.logmind.dto;

import com.logmind.entity.AnomalyEntity;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Getter
@Builder
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
                .id(entity.getId())
                .serviceId(entity.getServiceId())
                .detectedAt(entity.getDetectedAt())
                .windowStart(entity.getWindowStart())
                .windowEnd(entity.getWindowEnd())
                .errorCount(entity.getErrorCount())
                .baselineMean(entity.getBaselineMean())
                .baselineStddev(entity.getBaselineStddev())
                .zScore(entity.getZScore())
                .severity(entity.getSeverity())
                .status(entity.getStatus())
                .logSample(entity.getLogSample())
                .createdAt(entity.getCreatedAt())
                .build();
    }
}