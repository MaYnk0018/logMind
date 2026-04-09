package com.logmind.dto;

import java.time.LocalDateTime;
import java.util.Map;

import com.logmind.entity.LogLevel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class StoredLogMessage {
    private Long logDbId;          // DB auto-increment ID
    private String serviceId;      // UUID
    private String serviceName;    // for anomaly consumer display
    private LogLevel level;
    private String message;
    private Map<String, Object> metadata;
    private LocalDateTime timestamp;
}