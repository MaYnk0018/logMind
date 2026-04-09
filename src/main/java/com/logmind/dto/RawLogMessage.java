package com.logmind.dto;

import lombok.*;

import java.time.LocalDateTime;
import java.util.Map;

import com.logmind.entity.LogLevel;

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

