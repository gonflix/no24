package com.ticketing.ticketing.dto;

import jakarta.validation.constraints.NotNull;

public record SeatReservationRequest(
        @NotNull Long eventId,
        @NotNull Long seatId) {
}
