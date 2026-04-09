# LogMind — Complete Technical Documentation

> **Intelligent Log Aggregator** | All 7 Phases | Production-Grade Build Guide
>
> Stack: `Java 21` · `Spring Boot 3.2` · `MySQL 8` · `Apache Kafka` · `React + Vite` · `Claude API` · `Resilience4j` · `Prometheus`

---

## How to use this documentation

This repository is the **single source of truth** for building LogMind end-to-end.
Every file is self-contained. An AI agent or engineer can read any file independently.

### For Cursor / AI agents

Open the entire `logmind-docs/` folder in Cursor. Every `.md` file is a complete
specification for its phase or component. Use `@` mentions to reference files in prompts.

---

## Document Map

| File | Contents | Read when |
|------|----------|-----------|
| `README.md` | This file — navigation guide | Start here |
| `00-architecture.md` | HLD, system design, database schema, Kafka topics | Before writing any code |
| `01-phase1-monolith.md` | Phase 1 LLD — every class, method, entity, DTO | Building the monolith |
| `02-phase2-kafka.md` | Phase 2 LLD — Kafka producer, consumer, LogBatcher | Adding async pipeline |
| `03-phase3-anomaly-service.md` | Phase 3 LLD — Anomaly detection as microservice | Extracting anomaly detection |
| `04-phase4-ai-analysis.md` | Phase 4 LLD — Claude API, RAG, prompt, eval loop | Building AI layer |
| `05-phase5-alert-service.md` | Phase 5 LLD — Alert rules, dispatchers, dedup | Building alert system |
| `06-phase6-gateway-frontend.md` | Phase 6 LLD — API Gateway, SSE, React components | Building the frontend |
| `07-phase7-hardening.md` | Phase 7 LLD — Circuit breakers, rate limiting, metrics | Hardening for production |
| `08-api-reference.md` | All REST endpoints, request/response schemas | API integration |
| `09-database-schema.md` | Complete SQL DDL, all tables, indexes, partitions | Schema reference |
| `10-kafka-contracts.md` | All Kafka topic schemas, consumer group design | Event-driven integration |
| `11-config-reference.md` | All environment variables, application.yml | Configuration |
| `12-interview-guide.md` | 25 interview Q&A covering all phases | Interview prep |

---

## Phase Evolution Summary

```
Phase 1 ── Monolith: Spring Boot + MySQL + Z-score anomaly detection in-process
    │
Phase 2 ── Add Kafka: decouple ingestion from storage, LogBatcher dual-trigger flush
    │
Phase 3 ── Extract anomaly detection into standalone microservice
    │
Phase 4 ── AI Analysis Service: Claude API + RAG + structured JSON + eval loop
    │
Phase 5 ── Alert Service: configurable rules + webhook/email/Slack dispatch
    │
Phase 6 ── API Gateway (Spring Cloud Gateway) + React dashboard + SSE live stream
    │
Phase 7 ── Hardening: circuit breakers + rate limiting + DLQ + Prometheus metrics
```

---

## Quick Start

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Run Phase 1 monolith
cd services/logmind && ./mvnw spring-boot:run

# 3. Send a test log
curl -X POST http://localhost:8080/api/logs \
  -H "Content-Type: application/json" \
  -d '{"service":"payment-service","level":"ERROR","message":"DB timeout"}'

# 4. Check anomalies (after 20+ logs to warm up the window)
curl http://localhost:8080/api/anomalies
```

---

## Interview One-liner

> *"I built LogMind in 7 phases — starting with a Spring Boot monolith with a partitioned
> MySQL schema and a from-scratch Z-score sliding window anomaly detector, evolving into
> a Kafka-driven microservices pipeline with AI-powered root cause analysis via Claude,
> configurable alerting, a React dashboard, and production hardening with circuit breakers,
> rate limiting, dead-letter queues, and Prometheus metrics on every service."*
