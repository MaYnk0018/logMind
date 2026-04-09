# 04 — Phase 4: AI Analysis Service (Claude API + RAG + Eval Loop)

> **Goal**: Every anomaly triggers a Claude analysis with RAG context from similar past
> incidents. Response stored as structured JSON. Thumbs-up/down eval loop measures accuracy.
>
> **New service**: `ai-analysis-service` on port `3004`.
>
> **Estimated time**: 20–24 hours

---

## Table of Contents

1. [Architecture of the AI pipeline](#1-architecture-of-the-ai-pipeline)
2. [New service structure](#2-new-service-structure)
3. [Complete LLD — every file, every method](#3-complete-lld--every-file-every-method)
4. [The complete prompt template](#4-the-complete-prompt-template)
5. [Token budget management](#5-token-budget-management)
6. [RAG design — fingerprint similarity](#6-rag-design--fingerprint-similarity)
7. [Eval loop — measuring AI quality](#7-eval-loop--measuring-ai-quality)
8. [Circuit breaker config](#8-circuit-breaker-config)

---

## 1. Architecture of the AI pipeline

```
Kafka: anomalies topic
    │
    ▼
AnomalyConsumer.consume()
    │
    ├─── isDuplicate(serviceId + severity)?
    │         YES → ack and return (skip Claude call)
    │         NO  → continue
    │
    ├─── FingerprintBuilder.build(anomaly)
    │         → normalize error patterns
    │         → extract time-of-day, day-of-week
    │
    ├─── IncidentMemory.getSimilar(fingerprint, limit=3)
    │         → SELECT recent embeddings from DB
    │         → score each by weighted similarity
    │         → return top 3 with hypothesis + actions
    │
    ├─── PromptBuilder.build(anomaly, similarIncidents)
    │         → assemble all sections
    │         → token budget check — truncate if needed
    │
    ├─── ClaudeClient.analyze(prompt)  [with @CircuitBreaker]
    │         → POST api.anthropic.com/v1/messages
    │         → fallback: queue for retry if circuit open
    │
    ├─── ResponseParser.parse(rawText)
    │         → strip markdown fences
    │         → Jackson parse JSON
    │         → validate + default missing fields
    │         → return ParsedIncident (never throws)
    │
    ├─── anomalyRepository.updateStatus(id, ANALYZING → RESOLVED)
    │
    ├─── IncidentRepository.save(incident)
    │
    ├─── EmbeddingRepository.save(fingerprint, summary)
    │
    └─── AiResultPublisher.publish(incident)
              → Kafka: ai-results topic
```

---

## 2. New service structure

```
ai-analysis-service/
├── AiAnalysisApplication.java
├── consumer/
│   └── AnomalyConsumer.java           ← orchestrates the full pipeline
├── claude/
│   ├── ClaudeClient.java              ← Anthropic API + circuit breaker
│   ├── PromptBuilder.java             ← builds the prompt with token budget
│   └── ResponseParser.java            ← parse + validate Claude JSON (never throws)
├── rag/
│   ├── FingerprintBuilder.java        ← normalize anomaly to fingerprint
│   └── IncidentMemory.java            ← retrieve similar past incidents
├── publisher/
│   └── AiResultPublisher.java         ← publish to ai-results topic
├── repository/
│   ├── IncidentRepository.java
│   └── EmbeddingRepository.java
├── controller/
│   └── FeedbackController.java        ← POST /incidents/{id}/feedback
└── dto/
    ├── AnomalyMessage.java
    ├── ParsedIncident.java
    ├── SimilarIncident.java
    └── AiResultMessage.java
```

---

## 3. Complete LLD — every file, every method

### 3.1 `AnomalyConsumer.java` — `@Component`

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class AnomalyConsumer {

    private final FingerprintBuilder fingerprintBuilder;
    private final IncidentMemory incidentMemory;
    private final PromptBuilder promptBuilder;
    private final ClaudeClient claudeClient;
    private final ResponseParser responseParser;
    private final IncidentRepository incidentRepository;
    private final EmbeddingRepository embeddingRepository;
    private final AiResultPublisher aiResultPublisher;
    private final AnomalyRepository anomalyRepository;

    // Dedup: track service+severity combos seen within 5 minutes
    private final ConcurrentHashMap<String, Instant> dedupCache = new ConcurrentHashMap<>();

    @KafkaListener(topics = "anomalies", groupId = "ai-group")
    public void consume(ConsumerRecord<String, AnomalyMessage> record, Acknowledgment ack) {
        AnomalyMessage anomaly = record.value();

        try {
            if (isDuplicate(anomaly)) {
                log.debug("Skipping duplicate anomaly: service={} severity={}",
                        anomaly.getServiceId(), anomaly.getSeverity());
                ack.acknowledge();
                return;
            }

            runAnalysis(anomaly);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("AI analysis failed: anomalyId={} error={}", anomaly.getAnomalyId(), e.getMessage());
            // Don't ack — retry
        }
    }

    private boolean isDuplicate(AnomalyMessage anomaly) {
        String key = anomaly.getServiceId() + ":" + anomaly.getSeverity();
        Instant prev = dedupCache.get(key);
        if (prev != null && Instant.now().isBefore(prev.plusSeconds(300))) {
            return true;
        }
        dedupCache.put(key, Instant.now());
        return false;
    }

    @Scheduled(fixedDelay = 60_000)
    public void cleanDedupCache() {
        Instant cutoff = Instant.now().minusSeconds(300);
        dedupCache.entrySet().removeIf(e -> e.getValue().isBefore(cutoff));
    }

    private void runAnalysis(AnomalyMessage anomaly) {
        // 1. Build fingerprint
        Map<String, Object> fingerprint = fingerprintBuilder.build(anomaly);

        // 2. Retrieve similar past incidents (RAG)
        List<SimilarIncident> similar = incidentMemory.getSimilar(fingerprint, 3);

        // 3. Build prompt
        String prompt = promptBuilder.build(anomaly, similar);

        // 4. Call Claude
        anomalyRepository.updateStatus(anomaly.getAnomalyId(), "ANALYZING");
        ClaudeClient.ClaudeResponse response = claudeClient.analyze(prompt);

        if (response.isCircuitOpen()) {
            log.warn("Circuit open — queuing anomaly {} for retry", anomaly.getAnomalyId());
            anomalyRepository.updateStatus(anomaly.getAnomalyId(), "OPEN");
            return;
        }

        // 5. Parse response
        ParsedIncident parsed = responseParser.parse(response.getText());

        // 6. Save incident
        IncidentEntity incident = incidentRepository.save(
                anomaly.getAnomalyId(), parsed,
                prompt, response.getText(),
                response.getInputTokens() + response.getOutputTokens(),
                "claude-sonnet-4-20250514"
        );
        anomalyRepository.updateStatus(anomaly.getAnomalyId(), "RESOLVED");

        // 7. Save embedding for future RAG
        String summary = buildSummary(anomaly, parsed);
        embeddingRepository.save(incident.getId(), fingerprint, summary);

        // 8. Publish to ai-results
        aiResultPublisher.publish(anomaly, incident);
    }

    private String buildSummary(AnomalyMessage anomaly, ParsedIncident parsed) {
        return String.format("[%s] %s — %s",
                anomaly.getSeverity(),
                anomaly.getServiceId(),
                parsed.getHypothesis().length() > 100
                        ? parsed.getHypothesis().substring(0, 100) + "..."
                        : parsed.getHypothesis());
    }
}
```

---

### 3.2 `FingerprintBuilder.java` — `@Service`

```java
@Service
public class FingerprintBuilder {

    // Regex patterns to strip dynamic parts from error messages
    private static final Pattern UUID_PATTERN =
            Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);
    private static final Pattern IP_PATTERN =
            Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
    private static final Pattern NUMBER_PATTERN =
            Pattern.compile("\\b\\d+\\b");
    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}");

    public Map<String, Object> build(AnomalyMessage anomaly) {
        Map<String, Object> fingerprint = new LinkedHashMap<>();
        fingerprint.put("serviceId",        anomaly.getServiceId());
        fingerprint.put("severity",         anomaly.getSeverity().name());
        fingerprint.put("topErrorPatterns", extractPatterns(anomaly.getLogSample()));
        fingerprint.put("timeOfDay",        getTimeOfDay(anomaly.getDetectedAt()));
        fingerprint.put("dayOfWeek",        anomaly.getDetectedAt().getDayOfWeek().getValue());
        return fingerprint;
    }

    private List<String> extractPatterns(List<Map<String, Object>> logSample) {
        if (logSample == null || logSample.isEmpty()) return List.of();

        Map<String, Long> patternCounts = logSample.stream()
                .map(log -> String.valueOf(log.getOrDefault("message", "")))
                .map(this::normalize)
                .collect(Collectors.groupingBy(s -> s, Collectors.counting()));

        return patternCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(5)
                .map(Map.Entry::getKey)
                .toList();
    }

    private String normalize(String message) {
        return TIMESTAMP_PATTERN.matcher(
               NUMBER_PATTERN.matcher(
               IP_PATTERN.matcher(
               UUID_PATTERN.matcher(
                       message.toLowerCase())
               .replaceAll("*"))
               .replaceAll("*"))
               .replaceAll("*"))
               .replaceAll("*")
               .trim();
    }

    private int getTimeOfDay(LocalDateTime dt) {
        int hour = dt.getHour();
        if (hour < 6)  return 0;   // night
        if (hour < 12) return 1;   // morning
        if (hour < 18) return 2;   // afternoon
        return 3;                   // evening
    }
}
```

**Pattern normalization examples**:

```
Input:  "Connection timeout to db-host-192.168.1.5 after 3000ms"
Output: "connection timeout to db-host-* after *ms"

Input:  "User abc123-uuid not found in 2025-01-15T10:30:00"
Output: "user * not found in *"

Input:  "Failed after 3 retry attempts"
Output: "failed after * retry attempts"
```

---

### 3.3 `IncidentMemory.java` — `@Service` (RAG engine)

```java
@Service
@RequiredArgsConstructor
public class IncidentMemory {

    private final EmbeddingRepository embeddingRepository;
    private final IncidentRepository incidentRepository;

    public List<SimilarIncident> getSimilar(Map<String, Object> fingerprint, int limit) {
        List<IncidentEmbedding> candidates =
                embeddingRepository.findRecentEmbeddings(90);   // last 90 days

        return candidates.stream()
                .map(e -> new ScoredEmbedding(e, score(fingerprint, e)))
                .filter(s -> s.score > 0.3)   // minimum relevance threshold
                .sorted(Comparator.comparingDouble(ScoredEmbedding::score).reversed())
                .limit(limit)
                .map(s -> toSimilarIncident(s.embedding))
                .toList();
    }

    private double score(Map<String, Object> current, IncidentEmbedding stored) {
        Map<String, Object> stored_fp = stored.getFingerprint();
        double score = 0.0;

        // Service ID match: +0.4
        if (Objects.equals(current.get("serviceId"), stored_fp.get("serviceId")))
            score += 0.4;

        // Severity match: +0.2
        if (Objects.equals(current.get("severity"), stored_fp.get("severity")))
            score += 0.2;

        // Overlapping error patterns: +0.1 each (max +0.3)
        @SuppressWarnings("unchecked")
        List<String> currentPatterns = (List<String>) current.getOrDefault("topErrorPatterns", List.of());
        @SuppressWarnings("unchecked")
        List<String> storedPatterns  = (List<String>) stored_fp.getOrDefault("topErrorPatterns", List.of());

        long overlap = currentPatterns.stream()
                .filter(storedPatterns::contains)
                .count();
        score += Math.min(overlap * 0.1, 0.3);

        // Recency: +0.1 if within 7 days
        if (stored.getCreatedAt().isAfter(LocalDateTime.now().minusDays(7)))
            score += 0.1;

        return score;
    }

    private SimilarIncident toSimilarIncident(IncidentEmbedding embedding) {
        return incidentRepository.findById(embedding.getIncidentId())
                .map(incident -> SimilarIncident.builder()
                        .incidentId(incident.getId())
                        .summary(embedding.getSummary())
                        .hypothesis(incident.getHypothesis())
                        .suggestedActions(incident.getSuggestedActions())
                        .createdAt(incident.getCreatedAt())
                        .build())
                .orElse(null);
    }

    private record ScoredEmbedding(IncidentEmbedding embedding, double score) {}
}
```

**Similarity scoring table**:

| Factor | Score | Rationale |
|--------|-------|-----------|
| Same service ID | +0.4 | Strongest signal — same service, same patterns |
| Same severity | +0.2 | Same urgency level |
| Each overlapping error pattern | +0.1 (max +0.3) | Structural similarity in errors |
| Created within 7 days | +0.1 | Recent incidents more relevant |
| **Max possible** | **1.0** | |

---

### 3.4 `PromptBuilder.java` — `@Service`

```java
@Service
public class PromptBuilder {

    private static final int MAX_PROMPT_TOKENS = 3500;
    private static final int LOG_SECTION_TOKEN_LIMIT = 2000;

    public String build(AnomalyMessage anomaly, List<SimilarIncident> similar) {
        StringBuilder sb = new StringBuilder();

        // Section 1: Statistics (quantified evidence first — highest weight)
        sb.append("You are an expert Site Reliability Engineer analyzing a production anomaly.\n\n");
        sb.append("=== ANOMALY REPORT ===\n");
        sb.append(String.format("Service: %s\n", anomaly.getServiceId()));
        sb.append(String.format("Detected At: %s\n", anomaly.getDetectedAt()));
        sb.append(String.format("Severity: %s\n", anomaly.getSeverity()));
        sb.append(String.format("Z-Score: %.2f (baseline mean: %.1f errors/min, current: %d errors/min)\n",
                anomaly.getZScore(), anomaly.getBaselineMean(), anomaly.getErrorCount()));
        sb.append(String.format("Window: %s to %s\n\n",
                anomaly.getWindowStart(), anomaly.getWindowEnd()));

        // Section 2: Log sample (raw evidence)
        String logSection = formatLogSample(anomaly.getLogSample());
        if (estimateTokens(logSection) > LOG_SECTION_TOKEN_LIMIT) {
            // Truncate to last 10 logs
            List<Map<String, Object>> truncated = anomaly.getLogSample()
                    .subList(Math.max(0, anomaly.getLogSample().size() - 10),
                             anomaly.getLogSample().size());
            logSection = formatLogSample(truncated);
        }
        sb.append(String.format("=== RECENT ERROR LOGS (last %d errors) ===\n",
                anomaly.getLogSample().size()));
        sb.append(logSection).append("\n");

        // Section 3: Similar past incidents (RAG context — only if available)
        if (!similar.isEmpty()) {
            sb.append("=== SIMILAR PAST INCIDENTS ===\n");
            List<SimilarIncident> context = similar;
            if (estimateTokens(sb.toString()) > MAX_PROMPT_TOKENS - 500) {
                context = similar.subList(0, 1);   // trim to 1 example if over budget
            }
            for (SimilarIncident s : context) {
                String timeAgo = formatTimeAgo(s.getCreatedAt());
                sb.append(String.format("[%s] %s\n", timeAgo, s.getSummary()));
                sb.append(String.format("Root cause then: %s\n", s.getHypothesis()));
                sb.append(String.format("Actions taken: %s\n\n",
                        String.join(", ", s.getSuggestedActions())));
            }
        }

        // Section 4: Instruction + JSON schema (last — closest to generation)
        sb.append("""
                === TASK ===
                Analyze the anomaly above and identify the most likely root cause.
                Respond ONLY with a valid JSON object. No text before or after it. No markdown fences.

                Required JSON schema:
                {
                  "hypothesis": "string (2-4 sentences explaining the root cause)",
                  "confidence": number (0.0 to 1.0),
                  "affected_components": ["string"],
                  "suggested_actions": ["string"],
                  "similar_incident_ids": ["string"] or []
                }
                """);

        return sb.toString();
    }

    private String formatLogSample(List<Map<String, Object>> logs) {
        return logs.stream()
                .map(log -> String.format("[%s] [%s] %s %s",
                        log.getOrDefault("timestamp", ""),
                        log.getOrDefault("level", ""),
                        log.getOrDefault("message", ""),
                        log.containsKey("metadata") ? log.get("metadata").toString() : ""))
                .collect(Collectors.joining("\n"));
    }

    private int estimateTokens(String text) {
        return text.length() / 4;   // rough approximation: 1 token ≈ 4 chars
    }

    private String formatTimeAgo(LocalDateTime dt) {
        long days = ChronoUnit.DAYS.between(dt, LocalDateTime.now());
        if (days == 0) return "today";
        if (days == 1) return "yesterday";
        return days + " days ago";
    }
}
```

---

### 3.5 `ClaudeClient.java` — `@Service`

```java
@Slf4j
@Service
public class ClaudeClient {

    @Value("${anthropic.api-key}")
    private String apiKey;

    @Value("${anthropic.model:claude-sonnet-4-20250514}")
    private String model;

    @Value("${anthropic.max-tokens:800}")
    private int maxTokens;

    private final WebClient webClient;

    public ClaudeClient() {
        this.webClient = WebClient.builder()
                .baseUrl("https://api.anthropic.com")
                .defaultHeader("Content-Type", "application/json")
                .defaultHeader("anthropic-version", "2023-06-01")
                .build();
    }

    @CircuitBreaker(name = "claudeApi", fallbackMethod = "fallback")
    public ClaudeResponse analyze(String prompt) {
        Map<String, Object> body = Map.of(
                "model", model,
                "max_tokens", maxTokens,
                "system", "You are an expert Site Reliability Engineer. " +
                           "Always respond with valid JSON only.",
                "messages", List.of(Map.of("role", "user", "content", prompt))
        );

        Map<String, Object> response = webClient.post()
                .uri("/v1/messages")
                .header("x-api-key", apiKey)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .block(Duration.ofSeconds(30));

        // Extract text from response.content[0].text
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> content = (List<Map<String, Object>>) response.get("content");
        String text = (String) content.get(0).get("text");

        @SuppressWarnings("unchecked")
        Map<String, Object> usage = (Map<String, Object>) response.get("usage");
        int inputTokens  = ((Number) usage.get("input_tokens")).intValue();
        int outputTokens = ((Number) usage.get("output_tokens")).intValue();

        log.info("Claude response: {} input tokens, {} output tokens", inputTokens, outputTokens);

        return ClaudeResponse.success(text, inputTokens, outputTokens);
    }

    public ClaudeResponse fallback(String prompt, Exception ex) {
        log.warn("Circuit open or Claude API error: {}", ex.getMessage());
        return ClaudeResponse.circuitOpen();
    }

    @Getter
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class ClaudeResponse {
        private final String text;
        private final int inputTokens;
        private final int outputTokens;
        private final boolean circuitOpen;

        static ClaudeResponse success(String text, int in, int out) {
            return new ClaudeResponse(text, in, out, false);
        }
        static ClaudeResponse circuitOpen() {
            return new ClaudeResponse(null, 0, 0, true);
        }
    }
}
```

---

### 3.6 `ResponseParser.java` — `@Service`

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class ResponseParser {

    private final ObjectMapper objectMapper;

    /**
     * Parse Claude's JSON response into a ParsedIncident.
     * NEVER throws — returns a fallback object on any error.
     */
    public ParsedIncident parse(String rawText) {
        if (rawText == null || rawText.isBlank()) {
            return fallback("Empty response from Claude");
        }

        try {
            String cleaned = stripFences(rawText);
            JsonNode root = objectMapper.readTree(cleaned);

            String hypothesis = getString(root, "hypothesis", "Analysis unavailable");
            double confidence = clamp(getDouble(root, "confidence", 0.5), 0.0, 1.0);
            List<String> affected  = getStringList(root, "affected_components");
            List<String> actions   = getStringList(root, "suggested_actions");
            List<String> similarIds = getStringList(root, "similar_incident_ids");

            return ParsedIncident.builder()
                    .hypothesis(hypothesis)
                    .confidence(confidence)
                    .affectedComponents(affected)
                    .suggestedActions(actions)
                    .similarIncidentIds(similarIds)
                    .parseSuccess(true)
                    .build();

        } catch (Exception e) {
            log.error("Failed to parse Claude response: {}", e.getMessage());
            log.debug("Raw response was: {}", rawText);
            return fallback("Parse error: " + e.getMessage());
        }
    }

    private String stripFences(String text) {
        return text.replaceAll("```json", "")
                   .replaceAll("```", "")
                   .trim();
    }

    private String getString(JsonNode node, String field, String defaultVal) {
        JsonNode f = node.get(field);
        return (f != null && f.isTextual()) ? f.asText() : defaultVal;
    }

    private double getDouble(JsonNode node, String field, double defaultVal) {
        JsonNode f = node.get(field);
        return (f != null && f.isNumber()) ? f.asDouble() : defaultVal;
    }

    private List<String> getStringList(JsonNode node, String field) {
        JsonNode f = node.get(field);
        if (f == null || !f.isArray()) return new ArrayList<>();
        List<String> result = new ArrayList<>();
        f.forEach(item -> { if (item.isTextual()) result.add(item.asText()); });
        return result;
    }

    private double clamp(double val, double min, double max) {
        return Math.max(min, Math.min(max, val));
    }

    private ParsedIncident fallback(String reason) {
        return ParsedIncident.builder()
                .hypothesis(reason)
                .confidence(0.0)
                .affectedComponents(List.of())
                .suggestedActions(List.of())
                .similarIncidentIds(List.of())
                .parseSuccess(false)
                .build();
    }
}
```

**Why never throw?** A parse failure should not crash the Kafka consumer. If Claude
returns malformed JSON, we store a fallback incident with `parseSuccess=false`. The
anomaly is still marked as `RESOLVED` (it was analyzed, just poorly). The dashboard shows
"Parse error" — visible and debuggable.

---

### 3.7 `FeedbackController.java` — `@RestController`

```java
@RestController
@RequestMapping("/incidents")
@RequiredArgsConstructor
public class FeedbackController {

    private final IncidentRepository incidentRepository;

    @PostMapping("/{id}/feedback")
    public ResponseEntity<FeedbackResponse> submitFeedback(
            @PathVariable String id,
            @Valid @RequestBody FeedbackRequest request) {

        if (request.getRating() != 1 && request.getRating() != -1) {
            throw new IllegalArgumentException("Rating must be 1 or -1");
        }

        IncidentFeedback feedback = IncidentFeedback.builder()
                .incidentId(id)
                .rating(request.getRating())
                .notes(request.getNotes())
                .build();

        incidentRepository.saveFeedback(feedback);

        AccuracyStats stats = incidentRepository.getAccuracyStats(30);

        return ResponseEntity.ok(FeedbackResponse.builder()
                .incidentId(id)
                .rating(request.getRating())
                .accuracy30Days(stats.getAccuracy())
                .totalRatings30Days(stats.getTotal())
                .build());
    }

    @GetMapping("/accuracy")
    public List<DailyAccuracy> getDailyAccuracy() {
        return incidentRepository.getDailyAccuracyTrend(30);
    }
}
```

---

## 4. The complete prompt template

```
You are an expert Site Reliability Engineer analyzing a production anomaly.

=== ANOMALY REPORT ===
Service: {serviceId}
Detected At: {detectedAt}
Severity: {severity}
Z-Score: {zScore:.2f} (baseline mean: {baselineMean:.1f} errors/min, current: {errorCount} errors/min)
Window: {windowStart} to {windowEnd}

=== RECENT ERROR LOGS (last {N} errors) ===
[2025-01-15T10:30:00.123] [ERROR] Connection timeout to postgres {host: "db-01"}
[2025-01-15T10:30:00.456] [ERROR] Connection timeout to postgres {host: "db-01"}
[2025-01-15T10:30:01.789] [FATAL] Max retry attempts exceeded {attempts: 3}
... (up to 20 lines; truncated to 10 if > 2000 tokens)

=== SIMILAR PAST INCIDENTS ===          ← only if similarIncidents.size() > 0
[3 days ago] [HIGH] payment-service — Database connection pool exhausted after deploy
Root cause then: Connection pool size was reduced from 20 to 5 during the 14:00 deployment.
Actions taken: Rolled back deployment, increased pool size to 20

=== TASK ===
Analyze the anomaly above and identify the most likely root cause.
Respond ONLY with a valid JSON object. No text before or after it. No markdown fences.

Required JSON schema:
{
  "hypothesis": "string (2-4 sentences explaining the root cause)",
  "confidence": number (0.0 to 1.0),
  "affected_components": ["string"],
  "suggested_actions": ["string"],
  "similar_incident_ids": ["string"] or []
}
```

**Why this section order?**

1. **Statistics first** — language models weight earlier tokens more. The Z-score,
   baseline, and severity give the model a quantified frame before reading raw text.
2. **Logs second** — raw evidence. The model builds a hypothesis after seeing the numbers.
3. **Historical context third** — RAG context. Pattern matching against past incidents.
4. **JSON schema last** — closest to where the model begins generating the response.
   The model "looks back" at context while generating — schema instruction nearest to
   output start = most likely to follow it precisely.

---

## 5. Token budget management

| Check | Threshold | Action |
|-------|-----------|--------|
| Log section token estimate | > 2,000 tokens | Truncate to last 10 logs |
| Full prompt token estimate | > 3,500 tokens | Trim similar incidents to 1 example |
| Max output tokens | Fixed at 800 | 2-4 sentence hypothesis + arrays = well within 800 |
| Dedup (same service+severity) | Within 5 minutes | Skip Claude call entirely |

**Token estimation**: `text.length() / 4` — rough but fast. 1 token ≈ 4 characters for
English text. Used for budget checks only, not billing.

---

## 6. RAG design — fingerprint similarity

### Why not real vector embeddings?

MySQL doesn't have native vector search. This fingerprint approach is:
- **Pragmatic** — works without pgvector, Pinecone, or any vector DB
- **Explainable** — every score component is auditable
- **Fast** — O(N) similarity check where N = incidents in last 90 days

**In production**: replace `IncidentMemory` with a `pgvector` or Pinecone implementation
behind the same interface. The `AnomalyConsumer` doesn't change.

---

## 7. Eval loop — measuring AI quality

```sql
-- Daily accuracy trend (surface as sparkline in dashboard)
SELECT
    DATE(created_at)  AS day,
    AVG(CASE WHEN rating = 1 THEN 1 ELSE 0 END) AS accuracy,
    COUNT(*)          AS total_ratings
FROM incident_feedback
GROUP BY DATE(created_at)
ORDER BY day DESC;
```

**What to watch**:
- **Declining accuracy** → prompt needs tuning. Check `raw_prompt` + `raw_response`
  columns in `incidents` table for patterns in low-rated responses.
- **Low total ratings** → add friction-free feedback UI. FeedbackButtons must be
  one click, not a form.
- **Accuracy < 50%** → model is worse than random. Root cause: bad prompt, wrong model,
  or anomaly detector producing false positives.

---

## 8. Circuit breaker config

```yaml
# application.yml
resilience4j:
  circuitbreaker:
    instances:
      claudeApi:
        slidingWindowType: COUNT_BASED
        slidingWindowSize: 10              # track last 10 calls
        failureRateThreshold: 50           # open if ≥50% fail
        waitDurationInOpenState: 30s       # wait 30s before half-open
        permittedNumberOfCallsInHalfOpenState: 3  # test 3 calls
        registerHealthIndicator: true      # visible at /actuator/health
```

**State transitions**:
```
CLOSED (normal)
  → 5 of last 10 calls fail
  → OPEN (circuit opens, all calls → fallback immediately)
  → wait 30 seconds
  → HALF-OPEN (allow 3 test calls)
  → if ≥2 succeed: CLOSED
  → if ≥2 fail: OPEN (another 30s wait)
```
