package main.java.com.logmind.repository;

import com.logmind.entity.ServiceEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ServiceRepository extends JpaRepository<ServiceEntity, String> {

    Optional<ServiceEntity> findByName(String name);
}