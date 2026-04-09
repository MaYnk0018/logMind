# 05 — Phase 5: Alert Service

> **Goal**: Configurable rules evaluated against anomalies. Multi-channel dispatch with retry.
>
> **New service**: `alert-service` on port `3005`.
>
> **Estimated time**: 12–16 hours

---

## 1. Architecture

```
Kafka: ai-results topic
    │
    ▼
AiResultConsumer
    │
    ▼
AlertEvaluator.evaluate(anomaly, incident)
    │
    ├─── SELECT active alert_rules WHERE service_id = X OR service_id IS NULL
    ├─── For each rule: check errorCount >= threshold AND severity >= minSeverity
    ├─── isRecentlyFired(ruleId, serviceId)? → skip if fired within 5 min
    │
    ▼ (for each matching rule)
NotificationDispatcher.dispatch(rule, anomaly, incident)
    │
    ├─── INSERT alert_events SET delivered=false    ← write BEFORE dispatch
    │
    ├─── switch notificationChannel:
    │     WEBHOOK → WebhookDispatcher.send()
    │     EMAIL   → EmailDispatcher.send()
    │     SLACK   → SlackDispatcher.send()
    │
    └─── on success: UPDATE alert_events SET delivered=true
         on failure: leave delivered=false (visible in dashboard)
```

---

## 2. Complete LLD — every file, every method

### 2.1 `AiResultConsumer.java` — `@Component`

```java
@KafkaListener(topics = "ai-results", groupId = "alert-group")
public void consume(ConsumerRecord<String, AiResultMessage> record, Acknowledgment ack) {
    AiResultMessage msg = record.value();
    try {
        List<MatchedAlert> matched = alertEvaluator.evaluate(msg);
        matched.forEach(alert -> notificationDispatcher.dispatch(alert.getRule(), msg));
        ack.acknowledge();
    } catch (Exception e) {
        log.error("Alert processing failed anomalyId={}: {}", msg.getAnomalyId(), e.getMessage());
        // Don't ack — retry
    }
}
```

Always ack after dispatch attempt — even failed dispatches are recorded with `delivered=false`.
The offset should advance so the alert service doesn't re-process the same anomaly forever.

---

### 2.2 `AlertEvaluator.java` — `@Service`

```java
@Service
@RequiredArgsConstructor
public class AlertEvaluator {

    private final AlertRuleRepository alertRuleRepository;
    private final AlertEventRepository alertEventRepository;

    public List<MatchedAlert> evaluate(AiResultMessage msg) {
        // Load active rules matching this service or global rules (NULL service_id)
        List<AlertRule> rules = alertRuleRepository.findActiveRulesForService(msg.getServiceId());

        return rules.stream()
                .filter(rule -> matchesSeverity(rule, msg.getSeverity()))
                .filter(rule -> matchesErrorCount(rule, msg.getErrorCount()))
                .filter(rule -> !isRecentlyFired(rule.getId(), msg.getServiceId()))
                .map(rule -> new MatchedAlert(rule, msg))
                .toList();
    }

    private boolean matchesSeverity(AlertRule rule, AnomalyEntity.Severity anomalySeverity) {
        return severityRank(anomalySeverity) >= severityRank(rule.getSeverity());
    }

    private boolean matchesErrorCount(AlertRule rule, int errorCount) {
        return errorCount >= rule.getThresholdCount();
    }

    private boolean isRecentlyFired(String ruleId, String serviceId) {
        // Check if this rule fired for this service in the last 5 minutes
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(5);
        return alertEventRepository.countRecentFirings(ruleId, serviceId, cutoff) > 0;
    }

    private int severityRank(AnomalyEntity.Severity s) {
        return switch (s) {
            case LOW      -> 1;
            case MEDIUM   -> 2;
            case HIGH     -> 3;
            case CRITICAL -> 4;
        };
    }
}
```

**`AlertRuleRepository.findActiveRulesForService(serviceId)`**:
```sql
SELECT * FROM alert_rules
WHERE is_active = TRUE
  AND (service_id = :serviceId OR service_id IS NULL)
```

**`AlertEventRepository.countRecentFirings(ruleId, serviceId, cutoff)`**:
```sql
SELECT COUNT(*) FROM alert_events ae
JOIN alert_rules ar ON ae.rule_id = ar.id
WHERE ae.rule_id = :ruleId
  AND ar.service_id = :serviceId
  AND ae.fired_at > :cutoff
```

---

### 2.3 `NotificationDispatcher.java` — `@Service`

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationDispatcher {

    private final AlertEventRepository alertEventRepository;
    private final WebhookDispatcher webhookDispatcher;
    private final EmailDispatcher emailDispatcher;
    private final SlackDispatcher slackDispatcher;

    public void dispatch(AlertRule rule, AiResultMessage msg) {
        // Write alert_event BEFORE dispatch — if service crashes mid-dispatch,
        // the event is visible with delivered=false
        AlertEvent event = AlertEvent.builder()
                .ruleId(rule.getId())
                .anomalyId(msg.getAnomalyId())
                .firedAt(LocalDateTime.now())
                .payload(buildPayload(rule, msg))
                .delivered(false)
                .build();
        alertEventRepository.save(event);

        try {
            switch (rule.getNotificationChannel()) {
                case WEBHOOK -> webhookDispatcher.send(rule.getNotificationTarget(), buildPayload(rule, msg));
                case EMAIL   -> emailDispatcher.send(rule.getNotificationTarget(), buildPayload(rule, msg));
                case SLACK   -> slackDispatcher.send(rule.getNotificationTarget(), buildPayload(rule, msg));
            }

            event.setDelivered(true);
            alertEventRepository.save(event);
            log.info("Alert dispatched: rule={} channel={} service={}",
                    rule.getId(), rule.getNotificationChannel(), msg.getServiceId());

        } catch (Exception e) {
            log.error("Alert dispatch failed: rule={} channel={} error={}",
                    rule.getId(), rule.getNotificationChannel(), e.getMessage());
            // leave delivered=false — visible in dashboard
        }
    }

    private Map<String, Object> buildPayload(AlertRule rule, AiResultMessage msg) {
        return Map.of(
                "anomalyId",         msg.getAnomalyId(),
                "serviceId",         msg.getServiceId(),
                "severity",          msg.getSeverity(),
                "zScore",            msg.getZScore(),
                "hypothesis",        msg.getHypothesis(),
                "suggestedActions",  msg.getSuggestedActions(),
                "firedAt",           LocalDateTime.now().toString(),
                "dashboardUrl",      "http://localhost:5173/incidents/" + msg.getAnomalyId()
        );
    }
}
```

---

### 2.4 `WebhookDispatcher.java` — `@Service`

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class WebhookDispatcher {

    private final RestTemplate restTemplate;

    public void send(String url, Map<String, Object> payload) {
        int maxAttempts = 3;
        long delayMs = 1000;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                ResponseEntity<String> response = restTemplate.postForEntity(
                        url, payload, String.class);

                if (response.getStatusCode().is2xxSuccessful()) {
                    log.debug("Webhook delivered to {} on attempt {}", url, attempt);
                    return;
                }

                // 4xx = client error (wrong URL, auth failure) — don't retry
                if (response.getStatusCode().is4xxClientError()) {
                    log.error("Webhook 4xx error at {}: {} — not retrying", url, response.getStatusCode());
                    throw new RuntimeException("4xx from webhook: " + response.getStatusCode());
                }

                // 5xx = server error — retry with backoff
                log.warn("Webhook 5xx attempt {}/{}: {}", attempt, maxAttempts, response.getStatusCode());

            } catch (ResourceAccessException e) {
                log.warn("Webhook timeout attempt {}/{}: {}", attempt, maxAttempts, e.getMessage());
            }

            if (attempt < maxAttempts) {
                try { Thread.sleep(delayMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                delayMs *= 2;   // exponential backoff: 1s, 2s, 4s
            }
        }

        throw new RuntimeException("Webhook failed after " + maxAttempts + " attempts: " + url);
    }
}
```

**Retry policy**:
```
Attempt 1: immediate
Attempt 2: 1 second later
Attempt 3: 2 seconds later (total: 3s max wait)
4xx errors: never retry (misconfigured URL/auth)
5xx errors + timeouts: retry up to 3x
After 3 failures: throw, NotificationDispatcher leaves delivered=false
```

---

### 2.5 `EmailDispatcher.java` — `@Service`

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class EmailDispatcher {

    private final JavaMailSender mailSender;

    @Value("${logmind.alert.from-address}")
    private String fromAddress;

    public void send(String toAddress, Map<String, Object> payload) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setFrom(fromAddress);
            helper.setTo(toAddress);
            helper.setSubject(String.format("[LogMind %s] Anomaly in %s",
                    payload.get("severity"), payload.get("serviceId")));
            helper.setText(buildHtmlBody(payload), true);

            mailSender.send(message);
        } catch (MessagingException e) {
            throw new RuntimeException("Email dispatch failed: " + e.getMessage(), e);
        }
    }

    private String buildHtmlBody(Map<String, Object> p) {
        List<?> actions = (List<?>) p.get("suggestedActions");
        String actionsList = actions.stream()
                .map(a -> "<li>" + a + "</li>")
                .collect(Collectors.joining());

        return """
            <html><body style="font-family: Arial, sans-serif;">
            <h2 style="color: #1E4D8C;">LogMind Anomaly Alert</h2>
            <table>
              <tr><td><b>Service</b></td><td>%s</td></tr>
              <tr><td><b>Severity</b></td><td>%s</td></tr>
              <tr><td><b>Z-Score</b></td><td>%.2f</td></tr>
              <tr><td><b>Detected</b></td><td>%s</td></tr>
            </table>
            <h3>Root Cause Hypothesis</h3>
            <p>%s</p>
            <h3>Suggested Actions</h3>
            <ol>%s</ol>
            <p><a href="%s">View in Dashboard</a></p>
            </body></html>
            """.formatted(
                p.get("serviceId"), p.get("severity"), p.get("zScore"),
                p.get("firedAt"), p.get("hypothesis"), actionsList, p.get("dashboardUrl"));
    }
}
```

**Spring Mail config**:
```yaml
spring:
  mail:
    host: ${SMTP_HOST:smtp.example.com}
    port: ${SMTP_PORT:587}
    username: ${SMTP_USER}
    password: ${SMTP_PASS}
    properties:
      mail.smtp.auth: true
      mail.smtp.starttls.enable: true
```

---

### 2.6 `SlackDispatcher.java` — `@Service`

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class SlackDispatcher {

    private final RestTemplate restTemplate;

    public void send(String webhookUrl, Map<String, Object> payload) {
        Map<String, Object> slackBody = buildBlocks(payload);

        ResponseEntity<String> response = restTemplate.postForEntity(
                webhookUrl, slackBody, String.class);

        // Slack returns "ok" or "no_service" as plain text, always HTTP 200
        if ("no_service".equals(response.getBody())) {
            throw new RuntimeException("Slack returned no_service — check webhook URL");
        }
    }

    private Map<String, Object> buildBlocks(Map<String, Object> p) {
        String emoji = severityEmoji(String.valueOf(p.get("severity")));
        List<?> actions = (List<?>) p.get("suggestedActions");

        String actionText = IntStream.range(0, actions.size())
                .mapToObj(i -> (i + 1) + ". " + actions.get(i))
                .collect(Collectors.joining("\n"));

        return Map.of("blocks", List.of(
            // Header
            Map.of("type", "header", "text",
                Map.of("type", "plain_text", "text",
                    emoji + " [" + p.get("severity") + "] Anomaly in " + p.get("serviceId"))),
            // Stats + Hypothesis
            Map.of("type", "section", "text",
                Map.of("type", "mrkdwn", "text",
                    "*Z-Score:* " + p.get("zScore") + "\n" +
                    "*Hypothesis:* " + p.get("hypothesis"))),
            Map.of("type", "divider"),
            // Suggested actions
            Map.of("type", "section", "text",
                Map.of("type", "mrkdwn", "text", "*Suggested Actions:*\n" + actionText)),
            // Context + dashboard link
            Map.of("type", "context", "elements", List.of(
                Map.of("type", "mrkdwn", "text",
                    "<" + p.get("dashboardUrl") + "|View in Dashboard>  •  " + p.get("firedAt"))))
        ));
    }

    private String severityEmoji(String severity) {
        return switch (severity) {
            case "CRITICAL" -> ":red_circle:";
            case "HIGH"     -> ":large_orange_circle:";
            case "MEDIUM"   -> ":large_yellow_circle:";
            default         -> ":large_green_circle:";
        };
    }
}
```

---

### 2.7 `AlertRuleController.java` — `@RestController("/alert-rules")`

```java
@RestController
@RequestMapping("/alert-rules")
@RequiredArgsConstructor
public class AlertRuleController {

    private final AlertRuleRepository alertRuleRepository;

    @GetMapping
    public List<AlertRuleResponse> listRules() {
        return alertRuleRepository.findAllWithServiceName()
                .stream().map(AlertRuleResponse::from).toList();
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public AlertRuleResponse createRule(@Valid @RequestBody AlertRuleRequest request) {
        AlertRule rule = AlertRule.builder()
                .serviceId(request.getServiceId())
                .logLevel(request.getLogLevel())
                .thresholdCount(request.getThresholdCount())
                .thresholdWindowS(request.getThresholdWindowS())
                .severity(request.getSeverity())
                .notificationChannel(request.getNotificationChannel())
                .notificationTarget(request.getNotificationTarget())
                .isActive(true)
                .build();
        return AlertRuleResponse.from(alertRuleRepository.save(rule));
    }

    @PutMapping("/{id}")
    public AlertRuleResponse updateRule(@PathVariable String id,
                                        @Valid @RequestBody AlertRuleRequest request) {
        AlertRule rule = alertRuleRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Rule not found: " + id));
        // update fields from request
        return AlertRuleResponse.from(alertRuleRepository.save(rule));
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteRule(@PathVariable String id) {
        alertRuleRepository.deleteById(id);
    }

    @PatchMapping("/{id}/toggle")
    public AlertRuleResponse toggleRule(@PathVariable String id) {
        AlertRule rule = alertRuleRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Rule not found: " + id));
        rule.setActive(!rule.isActive());
        return AlertRuleResponse.from(alertRuleRepository.save(rule));
    }
}
```

---

## 3. Two-layer dedup — alert storm prevention

```
LAYER 1 — AI Consumer dedup (AnomalyConsumer):
  Same service + severity within 5 minutes → skip Claude call entirely.
  Effect: at most 1 Claude analysis per service+severity per 5 minutes.

LAYER 2 — Alert Evaluator dedup:
  isRecentlyFired(ruleId, serviceId) → skip if already fired within 5 minutes.
  Effect: at most 1 notification per rule per service per 5 minutes.

Combined: A single error spike cannot produce more than:
  - 1 Claude call (Layer 1)
  - 1 notification per matching rule (Layer 2)

Without both layers: 20 anomaly messages in 30 seconds → 20 Claude calls → 20 Slack messages → on-call engineer furious.
```
