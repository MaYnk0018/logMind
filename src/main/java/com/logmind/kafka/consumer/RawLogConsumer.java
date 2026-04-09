package com.logmind.kafka.consumer;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.logmind.dto.RawLogMessage;
import com.logmind.service.LogBatcher;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class RawLogConsumer {

    private final LogBatcher logBatcher;

    @KafkaListener(
            topics = "raw-logs",
            groupId = "storage-group",
            containerFactory = "batchKafkaListenerContainerFactory")
    public void consumeBatch(List<ConsumerRecord<String, RawLogMessage>> records, Acknowledgment ack) {
        try {
            logBatcher.addAll(records.stream()
                    .map(ConsumerRecord::value)
                    .toList());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Batch processing failed — NOT acknowledging. Will retry. error={}",
                    e.getMessage(), e);
        }
    }
}
