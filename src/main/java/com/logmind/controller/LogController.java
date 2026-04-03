package com.logmind.controller;

import com.logmind.dto.IngestRequest;
import com.logmind.dto.IngestResponse;
import com.logmind.dto.LogResponse;
import com.logmind.dto.PagedResponse;
import com.logmind.entity.LogLevel;
import com.logmind.service.LogIngestionService;
import com.logmind.service.LogQueryService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController 
// equals@Controller
//@ResponseBody
//helps handle http request and return (json, xml)
@RequestMapping("/api/logs")
@RequiredArgsConstructor
public class LogController {

    private final LogIngestionService ingestionService;
    private final LogQueryService queryService;

    /**
     * POST /api/logs
     * Accepts a single log entry.
     *
     * curl -X POST http://localhost:8080/api/logs \
     *   -H "Content-Type: application/json" \
     *   -d '{"service":"payment-service","level":"ERROR","message":"DB timeout","metadata":{"host":"db-01"}}'
     */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public IngestResponse ingest(@Valid @RequestBody IngestRequest request) {
        return ingestionService.ingest(request);
    }

    /**
     * POST /api/logs/batch
     * Accepts up to 1000 log entries in one request.
     * All entries must belong to the same service.
     *
     * curl -X POST http://localhost:8080/api/logs/batch \
     *   -H "Content-Type: application/json" \
     *   -d '[{"service":"payment-service","level":"ERROR","message":"timeout #1"},...]'
     */
    @PostMapping("/batch")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public IngestResponse ingestBatch(@Valid @RequestBody List<IngestRequest> requests) {
        return ingestionService.ingestBatch(requests);
    }

    /**
     * GET /api/logs?serviceId=&level=&page=0&size=50
     * Returns paginated logs, newest first.
     */
    @GetMapping
    public PagedResponse<LogResponse> getLogs(
            @RequestParam(required = false) String serviceId,
            @RequestParam(required = false) LogLevel level,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size
    ) {
        size = Math.min(size, 200);
        return queryService.getLogs(serviceId, level, page, size);
    }

    /**
     * GET /api/logs/timeseries?serviceId=xxx&window=60
     * Returns per-minute error counts for the dashboard chart.
     */
    @GetMapping("/timeseries")
    public Object getTimeseries(
            @RequestParam String serviceId,
            @RequestParam(defaultValue = "60") int window
    ) {
        return queryService.getErrorRateTimeseries(serviceId, Math.min(window, 1440));
    }
}