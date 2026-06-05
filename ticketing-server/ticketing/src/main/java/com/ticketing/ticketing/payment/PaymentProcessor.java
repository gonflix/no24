package com.ticketing.ticketing.payment;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.ticketing.ticketing.kafka.PaymentRequestedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentProcessor {

    private final PaymentResultStore paymentResultStore;

    @KafkaListener(topics = "${app.kafka.payment-topic}", groupId = "ticketing-payment", containerFactory = "paymentKafkaListenerContainerFactory")
    public void process(PaymentRequestedEvent event) {
        log.info("Payment processing started. reservationId={}", event.reservationId());

        try {
            // PG 결제 처리 시뮬레이션 (7~13초)
            int delayMs = 7000 + ThreadLocalRandom.current().nextInt(6000);
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Payment processing interrupted. reservationId={}", event.reservationId());
            return;
        }

        boolean success = ThreadLocalRandom.current().nextInt(100) < 85; // 결제 성공확률을 85%로 설정
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
