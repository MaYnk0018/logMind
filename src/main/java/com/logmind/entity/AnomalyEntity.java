package com.logmind.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "anomalies")
@Getter
@Setter
@NoArgsConstructor
public class AnomalyEntity {

    @Id
    @Column(length = 36)
    private String id;

    @Column(name = "service_id", nullable = false, length = 36)
    private String serviceId;

    @Column(name = "detected_at", nullable = false)
    private LocalDateTime detectedAt;

    @Column(name = "window_start", nullable = false)
    private LocalDateTime windowStart;

    @Column(name = "window_end", nullable = false)
    private LocalDateTime windowEnd;

    @Column(name = "error_count", nullable = false)
    private int errorCount;

    @Column(name = "baseline_mean", nullable = false)
    private double baselineMean;

    @Column(name = "baseline_stddev", nullable = false)
    private double baselineStddev;

    @Column(name = "z_score", nullable = false)
    private double zScore;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Severity severity;

    // Stores the last N error logs as a JSON array for AI context later
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "log_sample", nullable = false, columnDefinition = "JSON")
    private List<Map<String, Object>> logSample;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private AnomalyStatus status = AnomalyStatus.OPEN;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    private void prePersist() {
        if (this.id == null) this.id = UUID.randomUUID().toString();
        this.createdAt = LocalDateTime.now();
    }

    public enum Severity { LOW, MEDIUM, HIGH, CRITICAL }

    public enum AnomalyStatus { OPEN, ANALYZING, RESOLVED, FALSE_POSITIVE }
}