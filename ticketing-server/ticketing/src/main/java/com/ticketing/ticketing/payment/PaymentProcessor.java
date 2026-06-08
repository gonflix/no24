package com.ticketing.ticketing.payment;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.ticketing.ticketing.kafka.PaymentRequestedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentProcessor {

    private final PaymentResultStore paymentResultStore;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.payment-topic}", groupId = "ticketing-payment", containerFactory = "paymentKafkaListenerContainerFactory")
    public void process(String message) {
        PaymentRequestedEvent event;
        try {
            event = objectMapper.readValue(message, PaymentRequestedEvent.class);
        } catch (JacksonException e) {
            log.error("Failed to deserialize PaymentRequestedEvent. message={}", message, e);
            return;
        }

        log.info("Payment processing started. reservationId={}", event.reservationId());

        try {
            // PG 결제 처리 시뮬레이션 (카드, 간편결제 보통 2~5초)
            int delayMs = 2000 + ThreadLocalRandom.current().nextInt(3000);
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Payment processing interrupted. reservationId={}", event.reservationId());
            return;
        }

        boolean success = ThreadLocalRandom.current().nextInt(100) < 85;
        PaymentResultEvent result = new PaymentResultEvent(
                event.reservationId(),
                event.userId(),
                success,
                success ? "결제가 성공했습니다." : "결제가 실패했습니다.",
                Instant.now());

        paymentResultStore.save(result);
        log.info("Payment processed. reservationId={}, success={}", event.reservationId(), success);
    }
}
