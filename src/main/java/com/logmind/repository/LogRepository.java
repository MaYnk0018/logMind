package com.logmind.repository;

import com.logmind.entity.LogEntity;
import com.logmind.entity.LogLevel;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface LogRepository extends JpaRepository<LogEntity, Long> {

    // Used by the log query endpoint — filter by service and/or level
    Page<LogEntity> findByServiceIdOrderByTimestampDesc(String serviceId, Pageable pageable);

    Page<LogEntity> findByServiceIdAndLevelOrderByTimestampDesc(String serviceId, LogLevel level, Pageable pageable);

    Page<LogEntity> findAllByOrderByTimestampDesc(Pageable pageable);

    // Used by SlidingWindowAnalyzer — count errors per minute bucket
    @Query("""
        SELECT l FROM LogEntity l
        WHERE l.serviceId = :serviceId
          AND l.level IN ('ERROR', 'FATAL')
          AND l.timestamp >= :since
        ORDER BY l.timestamp DESC
        """)
    List<LogEntity> findRecentErrors(
            @Param("serviceId") String serviceId,
            @Param("since") LocalDateTime since
    );

    // Error rate timeseries — used by the dashboard chart (Phase 5)
    @Query(value = """
        SELECT
            DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00') AS bucket,
            COUNT(*) AS error_count
        FROM logs
        WHERE service_id = :serviceId
          AND level IN ('ERROR', 'FATAL')
          AND timestamp >= :since
        GROUP BY bucket
        ORDER BY bucket ASC
        """, nativeQuery = true)
    List<Object[]> findErrorRateTimeseries(
            @Param("serviceId") String serviceId,
            @Param("since") LocalDateTime since
    );
}