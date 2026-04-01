package main.java.com.logmind.repository;


import com.logmind.entity.AnomalyEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface AnomalyRepository extends JpaRepository<AnomalyEntity, String> {

    Page<AnomalyEntity> findAllByOrderByDetectedAtDesc(Pageable pageable);

    Page<AnomalyEntity> findByServiceIdOrderByDetectedAtDesc(String serviceId, Pageable pageable);

    List<AnomalyEntity> findByServiceIdAndDetectedAtAfterOrderByDetectedAtDesc(
            String serviceId, LocalDateTime after
    );
}