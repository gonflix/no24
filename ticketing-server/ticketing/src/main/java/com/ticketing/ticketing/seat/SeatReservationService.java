package com.ticketing.ticketing.seat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ticketing.ticketing.reservation.Reservation;
import com.ticketing.ticketing.reservation.ReservationRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SeatReservationService {

    private static final String RESERVATION_PREFIX = "reservation:";
    private static final String LOCK_PREFIX = "seatlock:";
    private static final long RESERVATION_TTL_MINUTES = 10;

    private final RedissonClient redissonClient;
    private final SeatRepository seatRepository;
    private final ReservationRepository reservationRepository;

    public SeatReservationService(RedissonClient redissonClient,
            SeatRepository seatRepository,
            ReservationRepository reservationRepository) {
        this.redissonClient = redissonClient;
        this.seatRepository = seatRepository;
        this.reservationRepository = reservationRepository;
    }

    /**
     * 좌석 예약 흐름:
     * Redisson 분산락 획득 시도 (non-blocking)
     * - 락 실패 → 409
     * - 락 성공 → Redis 저장 + DB 저장 → 락 해제
     */
    @Transactional
    public Optional<SeatReservation> reserve(String userId, Long eventId, Long seatId) {

        // 분산락 획득 시도 (좌석 단위)
        RLock lock = redissonClient.getLock(seatLockKey(eventId, seatId));
        if (!lock.tryLock()) {
            return Optional.empty();
        }

        try {
            Seat seat = seatRepository.findById(seatId)
                    .orElseThrow(() -> new RuntimeException("Seat not found: " + seatId));

            if (seat.getStatus() != SeatStatus.AVAILABLE) {
                return Optional.empty();
            }
            Instant now = Instant.now();
            Instant expiresAt = now.plus(RESERVATION_TTL_MINUTES, ChronoUnit.MINUTES);

            // Reservation 테이블 저장
            Reservation reservation = Reservation.create(userId, seat, now, expiresAt);
            reservationRepository.save(reservation);

            String eid = reservation.getEid().toString();
            Long id = reservation.getId();

            // Redis 저장 (키: Reservation.eid, 값: SeatReservation)
            SeatReservation seatReservation = new SeatReservation(id, eid, userId, eventId, seatId,
                    reservation.getStatus(), now, expiresAt);

            RBucket<SeatReservation> bucket = redissonClient.getBucket(reservationKey(eid));
            bucket.set(seatReservation, Duration.ofMinutes(RESERVATION_TTL_MINUTES));

            // Seat 테이블 상태 업데이트
            seat.setStatus(SeatStatus.RESERVED);
            seatRepository.save(seat);

            return Optional.of(seatReservation);
        } catch (Exception e) {
            log.error("Error reserving seat {} for event {} by user {}: {}", seatId, eventId, userId, e.getMessage(),
                    e);
            throw e;

        } finally {
            lock.unlock();
        }
    }

    public Optional<SeatReservation> findByReservationUuid(String reservationEid) {
        RBucket<SeatReservation> bucket = redissonClient.getBucket(reservationKey(reservationEid));
        return Optional.ofNullable(bucket.get());
    }

    private String reservationKey(String reservationEid) {
        return RESERVATION_PREFIX + reservationEid;
    }

    private String seatLockKey(Long eventId, Long seatId) {
        return LOCK_PREFIX + eventId + ":" + seatId;
    }
}
