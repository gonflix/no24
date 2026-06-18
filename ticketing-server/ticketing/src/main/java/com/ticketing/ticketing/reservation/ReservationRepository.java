package com.ticketing.ticketing.reservation;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import jakarta.persistence.LockModeType;

public interface ReservationRepository extends JpaRepository<Reservation, Long> {

    // 결제 시작 시 비관적 쓰기 락으로 스케줄러와의 경합 방지
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT r FROM Reservation r WHERE r.id = :id")
    Optional<Reservation> findByIdForUpdate(@Param("id") Long id);

    // 만료 스케줄러용: PENDING 상태이고 만료 시각이 지난 예약 일괄 조회 (N+1 방지 JOIN FETCH)
    @Query("SELECT r FROM Reservation r JOIN FETCH r.seat WHERE r.status = 'PENDING' AND r.expiresAt < :now")
    List<Reservation> findExpiredPendingReservations(@Param("now") Instant now);
}
