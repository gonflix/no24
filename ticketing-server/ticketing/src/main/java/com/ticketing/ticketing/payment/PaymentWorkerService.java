package com.ticketing.ticketing.payment;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.ticketing.ticketing.kafka.PaymentRequestedEvent;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentWorkerService {

    private final PaymentResultStore paymentResultStore;
    private final MeterRegistry meterRegistry;

    private Counter successCounter;
    private Counter failCounter;

    @PostConstruct
    void initMetrics() {
        successCounter = Counter.builder("payment.completed")
                .tag("success", "true")
                .description("Completed payment attempts")
                .register(meterRegistry);
        failCounter = Counter.builder("payment.completed")
                .tag("success", "false")
                .description("Completed payment attempts")
                .register(meterRegistry);
    }

    @Async("paymentWorkerExecutor")
    public void execute(PaymentRequestedEvent event) {
        // 실제 결제 처리 대신 랜덤한 지연과 성공 여부 시뮬레이션
        // log.info("Worker started. reservationId={}", event.reservationId());

        try {
            int delayMs = 2000 + ThreadLocalRandom.current().nextInt(3000);
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Worker interrupted. reservationId={}", event.reservationId());
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
        (success ? successCounter : failCounter).increment();
        // log.info("Worker done. reservationId={}, success={}", event.reservationId(),
        // success);
    }
}
