package com.logmind.dto;

import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

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