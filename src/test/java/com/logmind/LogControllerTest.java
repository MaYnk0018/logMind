package com.logmind;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logmind.dto.IngestRequest;
import com.logmind.entity.LogLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = {"raw-logs", "stored-logs", "raw-logs-dlq"})
class LogControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("POST /api/logs — single log returns 202 ACCEPTED")
    void ingestSingle_returns202() throws Exception {
        IngestRequest request = new IngestRequest();
        request.setService("test-service");
        request.setLevel(LogLevel.ERROR);
        request.setMessage("Database connection timeout");

        mockMvc.perform(post("/api/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(1))
                .andExpect(jsonPath("$.serviceId").isNotEmpty())
                .andExpect(jsonPath("$.ingestedAt").isNotEmpty());
    }

    @Test
    @DisplayName("POST /api/logs — missing required fields returns 400 with error details")
    void ingestSingle_missingFields_returns400() throws Exception {
        mockMvc.perform(post("/api/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.errors").isMap())
                .andExpect(jsonPath("$.errors.service").isNotEmpty())
                .andExpect(jsonPath("$.errors.level").isNotEmpty());
    }

    @Test
    @DisplayName("POST /api/logs — invalid service name characters returns 400")
    void ingestSingle_invalidServiceName_returns400() throws Exception {
        IngestRequest request = new IngestRequest();
        request.setService("my service!!");
        request.setLevel(LogLevel.INFO);
        request.setMessage("Test");

        mockMvc.perform(post("/api/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.service").isNotEmpty());
    }

    @Test
    @DisplayName("POST /api/logs/batch — ingests multiple logs")
    void ingestBatch_returnsCorrectAcceptedCount() throws Exception {
        List<IngestRequest> batch = IntStream.range(0, 5)
                .mapToObj(i -> {
                    IngestRequest req = new IngestRequest();
                    req.setService("batch-service");
                    req.setLevel(LogLevel.WARN);
                    req.setMessage("Batch log " + i);
                    return req;
                })
                .toList();

        mockMvc.perform(post("/api/logs/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(batch)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(5));
    }

    @Test
    @DisplayName("POST /api/logs/batch — batch over 1000 returns 400")
    void ingestBatch_tooLarge_returns400() throws Exception {
        List<IngestRequest> hugeBatch = IntStream.range(0, 1001)
                .mapToObj(i -> {
                    IngestRequest req = new IngestRequest();
                    req.setService("big-service");
                    req.setLevel(LogLevel.INFO);
                    req.setMessage("Log " + i);
                    return req;
                })
                .toList();

        mockMvc.perform(post("/api/logs/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(hugeBatch)))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("GET /api/logs — returns paginated logs after ingest")
    void getLogs_returnsPaginatedResults() throws Exception {
        IngestRequest request = new IngestRequest();
        request.setService("query-test-service");
        request.setLevel(LogLevel.INFO);
        request.setMessage("Queryable log entry");

        mockMvc.perform(post("/api/logs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)));

        await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() ->
                mockMvc.perform(get("/api/logs").param("size", "10"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.content").isArray())
                        .andExpect(jsonPath("$.page").value(0))
                        .andExpect(jsonPath("$.size").value(10))
                        .andExpect(jsonPath("$.totalElements", greaterThanOrEqualTo(1))));
    }

    @Test
    @DisplayName("GET /api/logs — page size is capped at 200")
    void getLogs_pageSizeIsCapped() throws Exception {
        mockMvc.perform(get("/api/logs").param("size", "9999"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.size").value(200));
    }

    @Test
    @DisplayName("GET /api/anomalies — returns empty list when no anomalies exist")
    void getAnomalies_returnsEmptyList() throws Exception {
        mockMvc.perform(get("/api/anomalies"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content").isArray())
                .andExpect(jsonPath("$.totalElements").isNumber());
    }
}
