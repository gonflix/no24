package com.ticketing.ticketing.payment;

import java.time.Instant;
import java.util.UUID;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.ticketing.ticketing.reservation.ReservationRepository;
import com.ticketing.ticketing.reservation.ReservationStatus;
import com.ticketing.ticketing.seat.SeatStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentConfirmHelper {

    private final ReservationRepository reservationRepository;
    private final PaymentRepository paymentRepository;

    @Transactional
    public void confirm(UUID reservationId, String userId, long amount, String paymentMethod, Instant paidAt) {
        reservationRepository.findById(reservationId).ifPresentOrElse(reservation -> {
            reservation.setStatus(ReservationStatus.CONFIRMED);
            reservation.getSeat().setStatus(SeatStatus.SOLD);

            paymentRepository.save(Payment.builder()
                    .reservationId(reservationId)
                    .userId(userId)
                    .totAmount(amount)
                    .paymentMethod(paymentMethod)
                    .pgTid(UUID.randomUUID().toString())
                    .status(PaymentStatus.PAID)
                    .paidAt(paidAt)
                    .build());

            log.info("Payment confirmed in DB. reservationId={}", reservationId);
        }, () -> log.warn("Reservation not found for confirmation. reservationId={}", reservationId));
    }
}
