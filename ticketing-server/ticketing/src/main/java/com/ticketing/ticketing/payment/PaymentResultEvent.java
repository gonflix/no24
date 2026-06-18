package com.ticketing.ticketing.payment;

import java.time.Instant;

public record PaymentResultEvent(
        Long reservationId,
        String reservationEid,
        String userId,
        boolean success,
        String message,
        Instant processedAt) {
}
