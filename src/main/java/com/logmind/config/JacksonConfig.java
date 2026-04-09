package com.logmind.config;

import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Java 8 date/time JSON support. Uses a {@link Jackson2ObjectMapperBuilderCustomizer} so Spring Boot's
 * default {@link org.springframework.http.ProblemDetail} mixins stay registered (RFC 7807 + {@code errors} map).
 */
@Configuration
public class JacksonConfig {

    @Bean
    public Jackson2ObjectMapperBuilderCustomizer javaTimeJacksonCustomizer() {
        return (Jackson2ObjectMapperBuilder builder) -> {
            builder.featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        };
    }
}
