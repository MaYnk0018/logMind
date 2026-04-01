package main.java.com.logmind.service;

import com.logmind.entity.AnomalyEntity.Severity;
import com.logmind.entity.LogLevel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core anomaly detection engine.
 *
 * Holds one WindowState per service in memory.
 * On every log event, updates the relevant window and checks the Z-score.
 *
 * Interview talking point:
 * "Z-score is relative to each service's own baseline. A service that normally
 *  produces 50 errors/min spiking to 60 is fine (Z ≈ 0.5). A service that
 *  normally produces 0 errors spiking to 5 is a crisis (Z >> 2.5). A fixed
 *  threshold can't distinguish these — Z-score can."
 */
@Slf4j
@Component
public class SlidingWindowAnalyzer {

    @Value("${logmind.anomaly.window-size-minutes:60}")
    private int windowSizeMinutes;

    @Value("${logmind.anomaly.bucket-size-seconds:60}")
    private int bucketSizeSeconds;

    @Value("${logmind.anomaly.z-score-threshold:2.5}")
    private double zScoreThreshold;

    @Value("${logmind.anomaly.min-data-points:20}")
    private int minDataPoints;

    // serviceId → its sliding window
    private final ConcurrentHashMap<String, WindowState> windows = new ConcurrentHashMap<>();

    /**
     * Process a single log entry. Returns a DetectedAnomaly if the current
     * error rate is statistically significant, otherwise empty.
     *
     * @param serviceId  the service UUID
     * @param level      log level
     * @param timestampMs  epoch millis of the log event
     * @param logEntry   map representation of the log (for the sample stored with the anomaly)
     */
    public Optional<DetectedAnomaly> analyze(String serviceId, LogLevel level,
                                              long timestampMs, Map<String, Object> logEntry) {

        WindowState window = windows.computeIfAbsent(
                serviceId,
                id -> new WindowState(id, windowSizeMinutes, bucketSizeSeconds)
        );

        window.addLog(level, timestampMs, logEntry);

        WindowState.Stats stats = window.getStats(minDataPoints);

        // null = cold start or flat line — not enough data to score
        if (stats == null) return Optional.empty();

        if (stats.zScore() <= zScoreThreshold) return Optional.empty();

        log.warn("Anomaly detected: service={} zScore={:.2f} currentCount={} mean={:.2f}",
                serviceId, stats.zScore(), stats.currentCount(), stats.mean());

        return Optional.of(new DetectedAnomaly(
                serviceId,
                stats,
                classify(stats.zScore()),
                window.getLogSample()
        ));
    }

    /**
     * Map Z-score magnitude to a severity enum.
     *
     * Thresholds chosen so:
     *   CRITICAL (≥5.0) = roughly 1-in-3.5M event under a normal distribution
     *   HIGH     (≥3.5) = roughly 1-in-4300
     *   MEDIUM   (≥2.5) = roughly 1-in-160  ← our detection threshold
     */
    private Severity classify(double zScore) {
        if (zScore >= 5.0) return Severity.CRITICAL;
        if (zScore >= 3.5) return Severity.HIGH;
        if (zScore >= 2.5) return Severity.MEDIUM;
        return Severity.LOW;
    }

    /**
     * Value object returned when an anomaly is detected.
     * Passed to AnomalyService to persist and (later) to the AI layer.
     */
    public record DetectedAnomaly(
            String serviceId,
            WindowState.Stats stats,
            Severity severity,
            java.util.List<Map<String, Object>> logSample
    ) {}
}