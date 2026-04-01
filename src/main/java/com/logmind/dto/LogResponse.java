package main.java.com.logmind.dto;

import com.logmind.entity.LogEntity;
import com.logmind.entity.LogLevel;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.Map;

@Getter
@Builder
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