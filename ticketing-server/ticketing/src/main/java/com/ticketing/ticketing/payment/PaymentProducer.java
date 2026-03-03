package com.ticketing.ticketing.payment;

import java.util.concurrent.ExecutionException;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.ticketing.ticketing.config.AppProperties;
import com.ticketing.ticketing.kafka.PaymentRequestedEvent;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor // 생성자 주입
public class PaymentProducer {
    private static final String PAYMENT_QUEUE_KEY = "PAYMENT_QUEUE";
    private final KafkaTemplate<String, PaymentRequestedEvent> kafkaTemplate;
    private final AppProperties appProperties;

    public void send(PaymentRequestedEvent event) {
        try {
            kafkaTemplate.send(appProperties.kafka().paymentTopic(), PAYMENT_QUEUE_KEY, event).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka payment event publish interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Kafka payment event publish failed", e.getCause());
        }
    }
}
