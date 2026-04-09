# 08 — API Reference

> All services behind API Gateway at `http://localhost:3000`.
> All responses: `Content-Type: application/json`

---

## Ingestion Service `:3001`

### `POST /api/logs` — ingest single log
```http
POST /api/logs
Content-Type: application/json

{
  "service":   "payment-service",
  "level":     "ERROR",
  "message":   "DB connection timeout after 30s",
  "metadata":  { "host": "db-primary", "attempt": 3 },
  "timestamp": "2025-01-15T10:30:00.000Z"
}
```
**Response 202**:
```json
{ "accepted": 1, "serviceId": "uuid", "ingestedAt": "2025-01-15T10:30:00" }
```

### `POST /api/logs/batch` — ingest up to 1000 logs
Body: JSON array of the same shape. All entries must be same service.

### `GET /api/logs` — query logs
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `serviceId` | string | — | Filter by service UUID |
| `level` | enum | — | DEBUG / INFO / WARN / ERROR / FATAL |
| `page` | int | 0 | Zero-indexed |
| `size` | int | 50 | Max 200 |

**Response**: `PagedResponse<LogResponse>` — `{ content, page, size, totalElements, totalPages }`

### `GET /api/logs/timeseries` — error rate chart data
| Param | Required | Description |
|-------|----------|-------------|
| `serviceId` | yes | Service UUID |
| `window` | no | Minutes of history (default 60, max 1440) |

**Response**: `[ { "bucket": "2025-01-15 10:30:00", "errorCount": 42 } ]`

---

## Anomaly Detection Service `:3003`

### `GET /api/anomalies` — list anomalies
| Param | Type | Default |
|-------|------|---------|
| `serviceId` | string | — |
| `page` | int | 0 |
| `size` | int | 20 (max 100) |

---

## AI Analysis Service `:3004`

### `GET /api/incidents/by-anomaly/{anomalyId}`
Returns the `IncidentResponse` for the given anomaly UUID. 404 if not yet analyzed.

### `POST /api/incidents/{id}/feedback`
```json
{ "rating": 1, "notes": "Correctly identified the DB issue" }
```
`rating` must be `1` or `-1`. Returns `{ incidentId, rating, accuracy30Days, totalRatings30Days }`.

### `GET /api/incidents/accuracy`
Returns `[ { "day": "2025-01-15", "accuracy": 0.83, "totalRatings": 6 } ]` for last 30 days.

---

## Alert Service `:3005`

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/alerts` | All alert rules with service name |
| `POST` | `/api/alerts` | Create rule. Body: `AlertRuleRequest` |
| `PUT` | `/api/alerts/{id}` | Update rule |
| `DELETE` | `/api/alerts/{id}` | Delete rule (204) |
| `PATCH` | `/api/alerts/{id}/toggle` | Toggle `is_active` |

---

## Error Response Shape (RFC 7807)

All errors return this shape:
```json
{
  "type":      "https://logmind.dev/errors/validation-failed",
  "title":     "Validation failed",
  "status":    400,
  "detail":    "2 field(s) failed validation",
  "timestamp": "2025-01-15T10:30:00Z",
  "errors":    { "service": "must not be blank", "level": "must not be null" }
}
```

---
---

# 10 — Kafka Event Contracts

## `raw-logs` topic

```json
{
  "logId":      "550e8400-e29b-41d4-a716-446655440000",
  "service":    "payment-service",
  "serviceId":  "uuid",
  "level":      "ERROR",
  "message":    "Connection timeout to postgres",
  "metadata":   { "host": "db-01", "port": 5432 },
  "timestamp":  "2025-01-15T10:30:00.000Z",
  "ingestedAt": "2025-01-15T10:30:00.050Z"
}
```

## `stored-logs` topic

```json
{
  "logDbId":    12345,
  "serviceId":  "uuid",
  "serviceName":"payment-service",
  "level":      "ERROR",
  "message":    "Connection timeout to postgres",
  "timestamp":  "2025-01-15T10:30:00.000Z"
}
```

## `anomalies` topic

```json
{
  "anomalyId":       "uuid",
  "serviceId":       "uuid",
  "serviceName":     "payment-service",
  "detectedAt":      "2025-01-15T10:30:00.000Z",
  "windowStart":     "2025-01-15T09:30:00.000Z",
  "windowEnd":       "2025-01-15T10:30:00.000Z",
  "errorCount":      145,
  "baselineMean":    12.4,
  "baselineStddev":  3.1,
  "zScore":          4.26,
  "severity":        "HIGH",
  "logSample": [
    { "timestamp": "2025-01-15T10:29:58.123Z", "level": "ERROR", "message": "timeout" }
  ]
}
```

## `ai-results` topic

```json
{
  "anomalyId":          "uuid",
  "incidentId":         "uuid",
  "serviceId":          "uuid",
  "hypothesis":         "The payment service is failing to connect...",
  "confidence":         0.87,
  "affectedComponents": ["payment-service", "db-primary"],
  "suggestedActions":   ["Check DB connection pool", "Review recent deploys"],
  "severity":           "HIGH"
}
```

---
---

# 11 — Configuration Reference

## All environment variables

### Shared
| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_PROFILES_ACTIVE` | `development` | `production` disables SQL logging |
| `LOG_LEVEL` | `info` | Application log level |

### MySQL
| Variable | Default |
|----------|---------|
| `MYSQL_HOST` | `localhost` |
| `MYSQL_PORT` | `3306` |
| `MYSQL_USER` | `logmind` |
| `MYSQL_PASSWORD` | `logmind_secret` |
| `MYSQL_DATABASE` | `logmind` |

### Kafka
| Variable | Default |
|----------|---------|
| `KAFKA_BROKERS` | `localhost:9092` |

### Ingestion Service
| Variable | Default |
|----------|---------|
| `INGESTION_PORT` | `3001` |
| `RATE_LIMIT_MAX_PER_SERVICE` | `100` |
| `RATE_LIMIT_WINDOW_SECONDS` | `10` |

### Storage Service
| Variable | Default |
|----------|---------|
| `STORAGE_PORT` | `3002` |
| `BATCH_MAX_SIZE` | `100` |
| `BATCH_MAX_WAIT_MS` | `500` |

### Anomaly Detection
| Variable | Default |
|----------|---------|
| `ANOMALY_PORT` | `3003` |
| `Z_SCORE_THRESHOLD` | `2.5` |
| `WINDOW_SIZE_MINUTES` | `60` |
| `BUCKET_SIZE_SECONDS` | `60` |
| `MIN_DATA_POINTS` | `20` |

### AI Analysis
| Variable | Default |
|----------|---------|
| `AI_PORT` | `3004` |
| `ANTHROPIC_API_KEY` | — (required) |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-20250514` |
| `ANTHROPIC_MAX_TOKENS` | `800` |

### Alert Service
| Variable | Default |
|----------|---------|
| `ALERT_PORT` | `3005` |
| `SMTP_HOST` | `smtp.example.com` |
| `SMTP_PORT` | `587` |
| `SMTP_USER` | — |
| `SMTP_PASS` | — |

### API Gateway
| Variable | Default |
|----------|---------|
| `GATEWAY_PORT` | `3000` |

### Frontend
| Variable | Default |
|----------|---------|
| `VITE_API_BASE_URL` | `http://localhost:3000` |
