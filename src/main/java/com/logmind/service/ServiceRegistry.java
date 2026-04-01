package main.java.com.logmind.service;

import com.logmind.entity.ServiceEntity;
import com.logmind.repository.ServiceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves a service name (e.g. "payment-service") to its UUID.
 *
 * Why a cache: every log ingested would otherwise hit the DB with
 * "SELECT id FROM services WHERE name = ?". Under load (100 req/s)
 * that is 100 redundant reads per second for the same handful of services.
 * The cache makes this O(1) after the first call per service.
 *
 * Thread-safe: ConcurrentHashMap + computeIfAbsent give us atomicity.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ServiceRegistry {

    private final ServiceRepository serviceRepository;

    // name → UUID
    private final ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();

    /**
     * Returns the service UUID for the given name.
     * Creates the service row if it does not exist (upsert semantics).
     */
    @Transactional
    public String resolveId(String serviceName) {
        return cache.computeIfAbsent(serviceName, this::findOrCreate);
    }

    private String findOrCreate(String name) {
        return serviceRepository.findByName(name)
                .map(ServiceEntity::getId)
                .orElseGet(() -> {
                    log.info("Registering new service: {}", name);
                    ServiceEntity saved = serviceRepository.save(ServiceEntity.of(name));
                    return saved.getId();
                });
    }
}