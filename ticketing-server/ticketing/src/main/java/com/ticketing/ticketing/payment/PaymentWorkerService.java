package com.ticketing.ticketing.payment;

import java.time.Instant;
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
    private final Counter completedCounter;

    public PaymentWorkerService(PaymentResultStore paymentResultStore, MeterRegistry meterRegistry) {
        this.paymentResultStore = paymentResultStore;
        this.completedCounter = Counter.builder("payment.completed")
                .description("Completed payment attempts")
                .register(meterRegistry);
    }

    @Async("paymentWorkerExecutor")
    public void execute(PaymentRequestedEvent event) {
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
        completedCounter.increment();
    }
}
