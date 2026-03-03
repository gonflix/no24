package com.ticketing.ticketing.payment;

import com.ticketing.ticketing.kafka.PaymentRequestedEvent;
import com.ticketing.ticketing.notification.PushNotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentProcessor {

    private final PushNotificationService pushNotificationService;

    @KafkaListener(topics = "${app.kafka.payment-topic}", groupId = "ticketing-payment", containerFactory = "paymentKafkaListenerContainerFactory")
    public void process(PaymentRequestedEvent event) {
        boolean success = ThreadLocalRandom.current().nextInt(100) < 85;
        PaymentResultEvent result = new PaymentResultEvent(
                event.reservationId(),
                event.userId(),
                success,
                success ? "결제가 성공했습니다." : "결제가 실패했습니다.",
                Instant.now()
        );

        log.info("Payment processed sequentially. reservationId={}, success={}", event.reservationId(), success);
        pushNotificationService.pushPaymentResult(result);
    }
}
