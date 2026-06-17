package com.ticketing.ticketing.payment;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.ticketing.ticketing.kafka.PaymentRequestedEvent;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class PaymentWorkerService {

    private final PaymentResultStore paymentResultStore;
    private final PaymentConfirmHelper paymentConfirmHelper;
    private final Counter completedCounter;

    public PaymentWorkerService(PaymentResultStore paymentResultStore,
            PaymentConfirmHelper paymentConfirmHelper,
            MeterRegistry meterRegistry) {
        this.paymentResultStore = paymentResultStore;
        this.paymentConfirmHelper = paymentConfirmHelper;
        this.completedCounter = Counter.builder("payment.completed")
                .description("Completed payment attempts")
                .register(meterRegistry);
    }

    @Async("paymentWorkerExecutor")
    public void execute(PaymentRequestedEvent event) {
        try {
            int delayMs = 1000 + ThreadLocalRandom.current().nextInt(1000); // 1~2초 랜덤 지연
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Worker interrupted. reservationId={}", event.reservationId());
            return;
        }

        boolean success = true; // 결제 성공률 100% 고정 (테스트용)
        Instant processedAt = Instant.now();

        PaymentResultEvent result = new PaymentResultEvent(
                event.reservationId(),
                event.userId(),
                success,
                success ? "결제가 성공했습니다." : "결제가 실패했습니다.",
                processedAt);

        paymentResultStore.save(result);
        completedCounter.increment();

        if (success) {
            paymentConfirmHelper.confirm(
                    UUID.fromString(event.reservationId()),
                    event.userId(),
                    event.amount(),
                    event.paymentMethod(),
                    processedAt);
        }
    }
}
