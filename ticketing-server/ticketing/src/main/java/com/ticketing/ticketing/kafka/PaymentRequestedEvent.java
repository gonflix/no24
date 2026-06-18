package com.ticketing.ticketing.kafka;

public record PaymentRequestedEvent(Long reservationId, String reservationEid, String userId, long amount,
                String paymentMethod) {
}
