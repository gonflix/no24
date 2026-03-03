package com.ticketing.ticketing.dto;

import jakarta.validation.constraints.NotBlank;

public record SeatReservationRequest(@NotBlank String seatId) {
}
