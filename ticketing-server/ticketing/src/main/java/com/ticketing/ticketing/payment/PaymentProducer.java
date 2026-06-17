package com.ticketing.ticketing.payment;

import java.util.concurrent.ExecutionException;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.ticketing.ticketing.config.AppProperties;
import com.ticketing.ticketing.kafka.PaymentRequestedEvent;

import lombok.RequiredArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
public class PaymentProducer {

    private static final String PAYMENT_QUEUE_KEY = "payment-requested";

    private final KafkaTemplate<String, String> paymentKafkaTemplate;
    private final AppProperties appProperties;
    private final ObjectMapper objectMapper;

    public void send(PaymentRequestedEvent event) {
        String json;
        try {
            json = objectMapper.writeValueAsString(event);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize PaymentRequestedEvent", e);
        }
        try {
            paymentKafkaTemplate.send(appProperties.kafka().paymentTopic(), PAYMENT_QUEUE_KEY, json).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka payment event publish interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Kafka payment event publish failed", e.getCause());
        }
    }
}
