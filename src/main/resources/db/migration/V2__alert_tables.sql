-- ============================================================
-- V2__alert_tables.sql
-- LogMind Phase 5 — alert rules + alert events
-- ============================================================

CREATE TABLE IF NOT EXISTS alert_rules (
  id                   VARCHAR(36) NOT NULL DEFAULT (UUID()),
  service_id           VARCHAR(36),  -- NULL = all services
  log_level            ENUM('DEBUG','INFO','WARN','ERROR','FATAL'),  -- NULL = any level
  threshold_count      INT UNSIGNED NOT NULL,
  threshold_window_s   INT UNSIGNED NOT NULL,
  severity             ENUM('LOW','MEDIUM','HIGH','CRITICAL') NOT NULL,
  notification_channel ENUM('EMAIL','WEBHOOK','SLACK') NOT NULL,
  notification_target  TEXT NOT NULL,
  is_active            BOOLEAN NOT NULL DEFAULT TRUE,
  created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  CONSTRAINT fk_rules_service FOREIGN KEY (service_id) REFERENCES services(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS alert_events (
  id          VARCHAR(36) NOT NULL DEFAULT (UUID()),
  rule_id     VARCHAR(36) NOT NULL,
  anomaly_id  VARCHAR(36),
  fired_at    DATETIME(3) NOT NULL,
  payload     JSON        NOT NULL,
  delivered   BOOLEAN     NOT NULL DEFAULT FALSE,
  PRIMARY KEY (id),
  CONSTRAINT fk_events_rule    FOREIGN KEY (rule_id)    REFERENCES alert_rules(id),
  CONSTRAINT fk_events_anomaly FOREIGN KEY (anomaly_id) REFERENCES anomalies(id)
) ENGINE=InnoDB;

