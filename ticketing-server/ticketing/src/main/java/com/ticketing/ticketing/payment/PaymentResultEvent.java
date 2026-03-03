package com.ticketing.ticketing.payment;

import java.time.Instant;

public record PaymentResultEvent(
        String reservationId,
        String userId,
        boolean success,
        String message,
        Instant processedAt
) {
}
