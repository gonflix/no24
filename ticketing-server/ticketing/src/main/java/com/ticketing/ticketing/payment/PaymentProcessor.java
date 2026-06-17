package com.ticketing.ticketing.payment;

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

    private final PaymentWorkerService paymentWorkerService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.payment-topic}",
            groupId = "ticketing-payment",
            containerFactory = "paymentKafkaListenerContainerFactory")
    public void process(String message) {
        PaymentRequestedEvent event;
        try {
            event = objectMapper.readValue(message, PaymentRequestedEvent.class);
        } catch (JacksonException e) {
            log.error("Failed to deserialize PaymentRequestedEvent. message={}", message, e);
            return;
        }

        log.info("Payment request received, dispatching to worker. reservationId={}", event.reservationId());
        paymentWorkerService.execute(event);
    }
}
