package com.logmind.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.logmind.dto.RawLogMessage;
import com.logmind.entity.LogEntity;
import com.logmind.repository.LogRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class LogBatcher {

    private final LogRepository logRepository;
    private final StoredLogPublisher storedLogPublisher;

    @Value("${logmind.batcher.max-size:100}")
    private int maxSize;

    private final List<RawLogMessage> buffer = new ArrayList<>();

    //why not ReentrantLock
    public synchronized void addAll(List<RawLogMessage> messages) {
        buffer.addAll(messages);
        if (buffer.size() >= maxSize) {
            flush();
        }
    }

    @Scheduled(fixedDelayString = "${logmind.batcher.max-wait-ms:500}")
    public synchronized void scheduledFlush() {
        if (!buffer.isEmpty()) {
            flush();
        }
    }

    private void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        List<RawLogMessage> batch = new ArrayList<>(buffer);
        buffer.clear();
        try {
            List<LogEntity> entities = batch.stream().map(this::toEntity).toList();
            // List<LogEntity> entities = batch.stream().
            List<LogEntity> saved = logRepository.saveAll(entities);
            log.debug("Flushed {} logs to DB", saved.size());
            for (int i = 0; i < saved.size(); i++) {
                storedLogPublisher.publish(saved.get(i), batch.get(i).getService());
            }
        } catch (Exception e) {
            log.error("Flush failed, re-queuing {} messages: {}", batch.size(), e.getMessage());
            buffer.addAll(0, batch);
            throw e;
        }
    }

    private LogEntity toEntity(RawLogMessage msg) {
        return LogEntity.of(
                msg.getServiceId(),
                msg.getLevel(),
                msg.getMessage(),
                msg.getMetadata(),
                msg.getTimestamp());
    }
}
