package com.ticketing.ticketing.payment;

import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, Long> {

    Optional<Payment> findByReservationId(UUID reservationId);

    boolean existsByReservationId(UUID reservationId);
}
