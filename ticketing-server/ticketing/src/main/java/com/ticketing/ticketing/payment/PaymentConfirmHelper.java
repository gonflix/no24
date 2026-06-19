package com.ticketing.ticketing.payment;

import java.time.Instant;
import java.util.UUID;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.ticketing.ticketing.reservation.Reservation;
import com.ticketing.ticketing.reservation.ReservationRepository;
import com.ticketing.ticketing.reservation.ReservationStatus;
import com.ticketing.ticketing.seat.SeatStatus;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentConfirmHelper {

    private final ReservationRepository reservationRepository;
    private final PaymentRepository paymentRepository;

    @Transactional
    public void confirm(Long reservationid, String userId, long amount, String paymentMethod, Instant paidAt) {
        Reservation reservation = reservationRepository.findById(reservationid)
                .orElseThrow(() -> new EntityNotFoundException("Reservation not found: " + reservationid));

        reservation.setStatus(ReservationStatus.CONFIRMED);
        reservation.getSeat().setStatus(SeatStatus.SOLD);

        paymentRepository.save(Payment.builder()
                .reservationId(reservationid)
                .userId(userId)
                .totAmount(amount)
                .paymentMethod(paymentMethod)
                .pgTid(UUID.randomUUID().toString())
                .status(PaymentStatus.PAID)
                .paidAt(paidAt)
                .build());

        log.info("Payment confirmed in DB. reservationId={}", reservation.getId());
    }

    @Transactional
    public void updateReservationOngoing(Long reservationid) {
        Reservation reservation = reservationRepository.findById(reservationid)
                .orElseThrow(() -> new EntityNotFoundException("Reservation not found: " + reservationid));

        reservation.setStatus(ReservationStatus.ONGOING);
    }
}
