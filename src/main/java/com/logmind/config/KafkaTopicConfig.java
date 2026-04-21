package com.logmind.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic rawLogsTopic() {
        return TopicBuilder.name("raw-logs")
                .partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic storedLogsTopic() {
        return TopicBuilder.name("stored-logs").partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic anomaliesTopic() {
        return TopicBuilder.name("anomalies")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic aiResultsTopic() {
        return TopicBuilder.name("ai-results")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic rawLogsDlqTopic() {
        return TopicBuilder.name("raw-logs-dlq")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
