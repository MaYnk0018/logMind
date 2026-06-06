package com.logmind.consumer;
import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.Acknowledgment;
import com.logmind.dto.RawLogMessage;
import com.logmind.service.LogBatcher;
import lombok.RequiredArgsConstructor;

@Component     
@RequiredArgsConstructor 
public class RawLogConsumer {

    private final LogBatcher logBatcher;

    @KafkaListener(topics = "raw-logs", groupId = "logmind-consumers", containerFactory  = "kafkaListenerContainerFactory")
    public void consume(List<RawLogMessage> messages, Acknowledgment ack) {
        logBatcher.addAll(messages);
        ack.acknowledge();
    }
}
