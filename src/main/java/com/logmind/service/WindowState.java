package com.logmind.service;

import com.logmind.entity.LogLevel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Sliding window state for one service.
 *
 * Design:
 * - Time is divided into fixed-size buckets (default: 60 seconds each).
 * - Each bucket stores the count of ERROR + FATAL logs in that minute.
 * - The window holds the last N buckets (default: 60 → 60 minutes of history).
 * - On every addLog(), stale buckets outside the window are evicted.
 *
 * Why buckets instead of a raw list of timestamps?
 * A raw list grows unboundedly under high error rates.
 * Buckets cap memory at O(windowSize / bucketSize) regardless of traffic.
 */
public class WindowState {

    private final String serviceId;
    private final long windowSizeMs;
    private final long bucketSizeMs;

    // bucketKey → error count  (LinkedHashMap preserves insertion order for eviction)
    private final LinkedHashMap<Long, Integer> buckets = new LinkedHashMap<>();

    // Keep a rolling sample of the last 20 error log messages for AI context
    private final LinkedList<Map<String, Object>> logSample = new LinkedList<>();
    private static final int MAX_SAMPLE_SIZE = 20;

    public WindowState(String serviceId, int windowSizeMinutes, int bucketSizeSeconds) {
        this.serviceId = serviceId;
        this.windowSizeMs = (long) windowSizeMinutes * 60 * 1000;
        this.bucketSizeMs = (long) bucketSizeSeconds * 1000;
    }

    /**
     * Record a log entry. Only ERROR and FATAL increment the bucket count.
     * Always evicts expired buckets after updating.
     */
    public void addLog(LogLevel level, long timestampMs, Map<String, Object> logEntry) {
        if (level == LogLevel.ERROR || level == LogLevel.FATAL) {
            long bucketKey = timestampMs / bucketSizeMs;
            buckets.merge(bucketKey, 1, Integer::sum);

            // Keep a capped rolling sample of recent error logs for the AI prompt
            if (logSample.size() >= MAX_SAMPLE_SIZE) logSample.removeFirst();
            logSample.addLast(logEntry);
        }
        evictStale(timestampMs);
    }

    /**
     * Remove buckets that have fallen outside the window.
     * Called on every addLog so the window always reflects "last N minutes".
     */
    private void evictStale(long nowMs) {
        long cutoffBucket = (nowMs - windowSizeMs) / bucketSizeMs;
        buckets.entrySet().removeIf(e -> e.getKey() < cutoffBucket);
    }

    /**
     * Compute Z-score statistics over the current window.
     *
     * Returns null if:
     * - fewer than minDataPoints buckets exist (cold start — not enough baseline)
     * - stddev is 0 (flat line — all buckets have the same count, division by zero)
     */
    public Stats getStats(int minDataPoints) {
        if (buckets.size() < minDataPoints) return null;

        List<Integer> values = new ArrayList<>(buckets.values());
        int n = values.size();

        // Mean
        double sum = 0;
        for (int v : values) sum += v;
        double mean = sum / n;

        // Population standard deviation
        double variance = 0;
        for (int v : values) variance += Math.pow(v - mean, 2);
        double stddev = Math.sqrt(variance / n);

        if (stddev == 0) return null;

        // Most recent bucket is the "current" observation being scored
        long latestBucketKey = buckets.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
        int currentCount = buckets.getOrDefault(latestBucketKey, 0);

        double zScore = (currentCount - mean) / stddev;

        long windowEndMs = latestBucketKey * bucketSizeMs + bucketSizeMs;
        long windowStartMs = windowEndMs - windowSizeMs;

        return new Stats(mean, stddev, currentCount, zScore, windowStartMs, windowEndMs);
    }

    public List<Map<String, Object>> getLogSample() {
        return new ArrayList<>(logSample);
    }

    public String getServiceId() {
        return serviceId;
    }


    
    /**
     * Immutable value object returned by getStats().
     * All fields are public final — no getters needed for an internal record.
     */
    public record Stats(
            double mean,
            double stddev,
            int currentCount,
            double zScore,
            long windowStartMs,
            long windowEndMs
    ) {}
}