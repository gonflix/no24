package com.ticketing.ticketing.seat;

import java.io.Serializable;
import java.time.Instant;

public record SeatReservation(String reservationId, String userId, String seatId, Instant reservedAt)
        implements Serializable {
}
