package com.ticketing.ticketing.seat;

import java.io.Serializable;
import java.time.Instant;

public record SeatReservation(
        String reservationId,
        String userId,
        Long eventId,
        Long seatId,
        Instant reservedAt,
        Instant expiresAt)
        implements Serializable {
}
