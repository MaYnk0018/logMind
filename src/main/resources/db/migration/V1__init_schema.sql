-- ============================================================
-- V1__init_schema.sql
-- LogMind Phase 1 — all tables
-- Run automatically by Flyway on first startup
-- ============================================================

-- 1. services — one row per source service sending logs
CREATE TABLE IF NOT EXISTS services (
    id          VARCHAR(36)  NOT NULL DEFAULT (UUID()),
    name        VARCHAR(100) NOT NULL,
    description TEXT,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_services_name (name)
) ENGINE = InnoDB;

-- 2. logs — core time-series table
--    Partitioned by month so old data can be pruned by dropping a partition.
--    In Phase 1 we query this with: WHERE service_id = ? AND timestamp > ?
CREATE TABLE IF NOT EXISTS logs (
    id         BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    service_id VARCHAR(36)      NOT NULL,
    level      ENUM('DEBUG','INFO','WARN','ERROR','FATAL') NOT NULL,
    message    TEXT             NOT NULL,
    metadata   JSON,
    timestamp  DATETIME(3)      NOT NULL,
    PRIMARY KEY (id, timestamp),
    INDEX idx_logs_service_time (service_id, timestamp),
    INDEX idx_logs_level_time   (level, timestamp)
) ENGINE = InnoDB
PARTITION BY RANGE (YEAR(timestamp) * 100 + MONTH(timestamp)) (
    PARTITION p202501 VALUES LESS THAN (202502),
    PARTITION p202502 VALUES LESS THAN (202503),
    PARTITION p202503 VALUES LESS THAN (202504),
    PARTITION p202504 VALUES LESS THAN (202505),
    PARTITION p202505 VALUES LESS THAN (202506),
    PARTITION p202506 VALUES LESS THAN (202507),
    PARTITION p202507 VALUES LESS THAN (202508),
    PARTITION p202508 VALUES LESS THAN (202509),
    PARTITION p202509 VALUES LESS THAN (202510),
    PARTITION p202510 VALUES LESS THAN (202511),
    PARTITION p202511 VALUES LESS THAN (202512),
    PARTITION p202512 VALUES LESS THAN (202601),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- 3. anomalies — one row per detected spike
CREATE TABLE IF NOT EXISTS anomalies (
    id              VARCHAR(36)  NOT NULL DEFAULT (UUID()),
    service_id      VARCHAR(36)  NOT NULL,
    detected_at     DATETIME(3)  NOT NULL,
    window_start    DATETIME(3)  NOT NULL,
    window_end      DATETIME(3)  NOT NULL,
    error_count     INT UNSIGNED NOT NULL,
    baseline_mean   FLOAT        NOT NULL,
    baseline_stddev FLOAT        NOT NULL,
    z_score         FLOAT        NOT NULL,
    severity        ENUM('LOW','MEDIUM','HIGH','CRITICAL') NOT NULL,
    log_sample      JSON         NOT NULL,
    status          ENUM('OPEN','ANALYZING','RESOLVED','FALSE_POSITIVE') NOT NULL DEFAULT 'OPEN',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_anomalies_service  (service_id, detected_at),
    INDEX idx_anomalies_severity (severity, detected_at),
    CONSTRAINT fk_anomalies_service FOREIGN KEY (service_id) REFERENCES services (id)
) ENGINE = InnoDB;

-- 4. incidents — Claude's AI analysis (Phase 4, table created now so schema is complete)
CREATE TABLE IF NOT EXISTS incidents (
    id                   VARCHAR(36) NOT NULL DEFAULT (UUID()),
    anomaly_id           VARCHAR(36) NOT NULL,
    hypothesis           TEXT        NOT NULL,
    confidence           FLOAT       NOT NULL,
    affected_components  JSON        NOT NULL,
    suggested_actions    JSON        NOT NULL,
    similar_incident_ids JSON,
    raw_prompt           TEXT        NOT NULL,
    raw_response         TEXT        NOT NULL,
    tokens_used          INT UNSIGNED NOT NULL,
    model_version        VARCHAR(50) NOT NULL,
    created_at           DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_incidents_anomaly (anomaly_id),
    CONSTRAINT fk_incidents_anomaly FOREIGN KEY (anomaly_id) REFERENCES anomalies (id)
) ENGINE = InnoDB;

-- 5. incident_feedback — eval loop (Phase 4, created now)
CREATE TABLE IF NOT EXISTS incident_feedback (
    id          VARCHAR(36) NOT NULL DEFAULT (UUID()),
    incident_id VARCHAR(36) NOT NULL,
    user_id     VARCHAR(36),
    rating      TINYINT     NOT NULL,
    notes       TEXT,
    created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_feedback_incident FOREIGN KEY (incident_id) REFERENCES incidents (id),
    CONSTRAINT chk_rating CHECK (rating IN (-1, 1))
) ENGINE = InnoDB;