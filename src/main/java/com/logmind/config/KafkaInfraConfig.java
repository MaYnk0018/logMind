package com.logmind.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.logmind.dto.RawLogMessage;
import com.logmind.dto.StoredLogMessage;

/**
 * Phase 2: separate consumer factories for {@link RawLogMessage} vs {@link StoredLogMessage}
 * (JSON deserialization targets different DTO types).
 */
@Configuration
@EnableKafka
public class KafkaInfraConfig {

    @Bean
    public ConsumerFactory<String, RawLogMessage> rawLogConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        Map<String, Object> props = consumerBaseProps(bootstrap, "storage-group");
        JsonDeserializer<RawLogMessage> deser = new JsonDeserializer<>(RawLogMessage.class);
        deser.addTrustedPackages("com.logmind.dto");
        deser.setUseTypeHeaders(false);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deser);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RawLogMessage> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, RawLogMessage> rawLogConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, RawLogMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(rawLogConsumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, StoredLogMessage> storedLogConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        Map<String, Object> props = consumerBaseProps(bootstrap, "anomaly-group");
        JsonDeserializer<StoredLogMessage> deser = new JsonDeserializer<>(StoredLogMessage.class);
        deser.addTrustedPackages("com.logmind.dto");
        deser.setUseTypeHeaders(false);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deser);
    }

    @Bean(name = "storedLogKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, StoredLogMessage> storedLogKafkaListenerContainerFactory(
            ConsumerFactory<String, StoredLogMessage> storedLogConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, StoredLogMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(storedLogConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    private static Map<String, Object> consumerBaseProps(String bootstrap, String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }
}
