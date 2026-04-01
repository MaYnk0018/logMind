package main.java.com.logmind.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.Map;

@Entity
@Table(name = "logs")
@Getter
@Setter
@NoArgsConstructor
public class LogEntity {

    // Note: the PK is (id, timestamp) in MySQL for partitioning.
    // JPA needs a single @Id — we use id only here and let MySQL handle the composite PK via DDL.
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "service_id", nullable = false, length = 36)
    private String serviceId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private LogLevel level;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String message;

    // JSON column — stored as a JSON object in MySQL
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "JSON")
    private Map<String, Object> metadata;

    @Column(nullable = false)
    private LocalDateTime timestamp;

    public static LogEntity of(String serviceId, LogLevel level, String message,
                               Map<String, Object> metadata, LocalDateTime timestamp) {
        LogEntity log = new LogEntity();
        log.serviceId = serviceId;
        log.level = level;
        log.message = message;
        log.metadata = metadata;
        log.timestamp = timestamp;
        return log;
    }
}