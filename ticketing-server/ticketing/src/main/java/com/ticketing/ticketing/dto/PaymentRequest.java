package com.ticketing.ticketing.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record PaymentRequest(
        @NotBlank String reservationId,
        @Min(1) long amount
) {
}
