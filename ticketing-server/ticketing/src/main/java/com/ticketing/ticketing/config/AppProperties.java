package com.ticketing.ticketing.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(Jwt jwt, Kafka kafka) {

    public record Jwt(String issuer, String audience, long expirationSeconds) {
    }

    public record Kafka(String paymentTopic, int paymentConcurrency) {
    }
}
