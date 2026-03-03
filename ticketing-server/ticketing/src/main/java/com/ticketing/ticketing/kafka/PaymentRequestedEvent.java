package com.ticketing.ticketing.kafka;

public record PaymentRequestedEvent(String reservationId, String userId, long amount) {
}
