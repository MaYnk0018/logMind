package com.logmind.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.logmind.dto.StoredLogMessage;
import com.logmind.entity.LogEntity;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class StoredLogPublisher {

    private final KafkaTemplate<String, StoredLogMessage> kafkaTemplate;

    /**
     * Key by {@code serviceId} so all logs for one service land on the same partition (ordering for anomaly windows).
     */
    public void publish(LogEntity entity, String serviceName) {
        StoredLogMessage msg = StoredLogMessage.builder()
                .logDbId(entity.getId())
                .serviceId(entity.getServiceId())
                .serviceName(serviceName)
                .level(entity.getLevel())
                .message(entity.getMessage())
                .metadata(entity.getMetadata())
                .timestamp(entity.getTimestamp())
                .build();

        kafkaTemplate.send("stored-logs", entity.getServiceId(), msg);
    }
}
