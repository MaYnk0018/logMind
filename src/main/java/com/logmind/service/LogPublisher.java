package com.logmind.service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.logmind.dto.IngestRequest;
import com.logmind.dto.RawLogMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class LogPublisher {
    //we need value serializer for RawLogMessage?? why and how??
    //spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
    private final KafkaTemplate<String, RawLogMessage> kafkaTemplate;

    public void publish(IngestRequest request, String serviceId){
        RawLogMessage msg = RawLogMessage.builder()
                .logId(UUID.randomUUID().toString())
                .service(request.getService())
                .serviceId(serviceId)
                .level(request.getLevel())
                .message(request.getMessage())
                .metadata(request.getMetadata())
                .timestamp(request.getTimestamp() != null
                        ? request.getTimestamp()
                        : LocalDateTime.now())
                .ingestedAt(LocalDateTime.now())
                .build();

        kafkaTemplate.send("raw-logs", request.getService(), msg).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish log for service={}: {}", request.getService(), ex.getMessage());
                publishToDlq(msg);
            }
        });
       
        //need of flush() here??
    }

    public void publishBatch(List<IngestRequest> requests, String serviceId) {
        requests.forEach(req -> publish(req, serviceId));
    }
    
    private void publishToDlq(RawLogMessage msg) {
        kafkaTemplate.send("raw-logs-dlq", msg.getService(), msg)
                .whenComplete((result, ex) -> {
                    if (ex != null) log.error("DLQ publish also failed: {}", ex.getMessage());
                });
    }
}
