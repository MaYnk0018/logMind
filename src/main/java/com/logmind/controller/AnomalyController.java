package com.logmind.controller;

import com.logmind.dto.AnomalyResponse;
import com.logmind.dto.PagedResponse;
import com.logmind.service.LogQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/anomalies")
@RequiredArgsConstructor
public class AnomalyController {

    private final LogQueryService queryService;

    /**
     * GET /api/anomalies?serviceId=&page=0&size=20
     *
     * curl http://localhost:8080/api/anomalies
     * curl http://localhost:8080/api/anomalies?serviceId=<uuid>&size=5
     */
    @GetMapping
    public PagedResponse<AnomalyResponse> getAnomalies(
            @RequestParam(required = false) String serviceId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        size = Math.min(size, 100);
        return queryService.getAnomalies(serviceId, page, size);
    }
}