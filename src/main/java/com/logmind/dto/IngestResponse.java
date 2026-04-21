package com.logmind.dto;

import com.logmind.entity.LogLevel;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Getter
@Builder
public class IngestResponse {
    private int accepted;
    private String serviceId;
    private LocalDateTime ingestedAt;

    // Factory for a batch
    public static IngestResponse of(int accepted, String serviceId) {
        return IngestResponse.builder()
                .accepted(accepted)
                .serviceId(serviceId)
                .ingestedAt(LocalDateTime.now())
                .build();
    }
}