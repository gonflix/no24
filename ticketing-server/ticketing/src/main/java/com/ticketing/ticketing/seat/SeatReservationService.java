package com.ticketing.ticketing.seat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ticketing.ticketing.reservation.Reservation;
import com.ticketing.ticketing.reservation.ReservationRepository;
import com.ticketing.ticketing.reservation.ReservationStatus;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SeatReservationService {

    private static final String CACHE_PREFIX = "reserve:";
    private static final String RESERVATION_PREFIX = "reservation:";
    private static final String LOCK_PREFIX = "ticketing:lock:seat:";
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
     * 1. Redis 캐시 확인 (reserve:{eventId}:{seatId}) → 이미 있으면 409
     * 2. Redisson 분산락 획득 시도 (non-blocking)
     *    - 락 실패 → 409
     *    - 락 성공 → 이중 체크 후 Redis 저장 + DB 저장 → 락 해제
     */
    @Transactional
    public Optional<SeatReservation> reserve(String userId, Long eventId, Long seatId) {
        RBucket<SeatReservation> seatBucket = redissonClient.getBucket(seatCacheKey(eventId, seatId));

        // 1. 캐시 히트 시 즉시 409 반환 (락 없이 빠르게 처리)
        if (seatBucket.isExists()) {
            return Optional.empty();
        }

        // 2. 분산락 획득 시도
        RLock lock = redissonClient.getLock(LOCK_PREFIX + eventId + ":" + seatId);
        boolean acquired;
        try {
            acquired = lock.tryLock(0, 0, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }

        if (!acquired) {
            return Optional.empty();
        }

        try {
            // 3. 락 획득 후 이중 체크 (TOCTOU 방지)
            if (seatBucket.isExists()) {
                return Optional.empty();
            }

            Seat seat = seatRepository.findById(seatId)
                    .orElseThrow(() -> new RuntimeException("Seat not found: " + seatId));

            if (seat.getStatus() != SeatStatus.AVAILABLE) {
                return Optional.empty();
            }

            Instant now = Instant.now();
            Instant expiresAt = now.plus(RESERVATION_TTL_MINUTES, ChronoUnit.MINUTES);
            UUID reservationId = UUID.randomUUID();
            SeatReservation seatReservation = new SeatReservation(
                    reservationId.toString(),
                    userId,
                    eventId,
                    seatId,
                    now,
                    expiresAt);

            // 4. Redis 캐시 저장 (좌석 키 + 예약ID 키)
            seatBucket.set(seatReservation, RESERVATION_TTL_MINUTES, TimeUnit.MINUTES);
            redissonClient.<SeatReservation>getBucket(reservationKey(seatReservation.reservationId()))
                    .set(seatReservation, RESERVATION_TTL_MINUTES, TimeUnit.MINUTES);

            // 5. DB 직접 저장
            Reservation reservation = new Reservation();
            reservation.setId(reservationId);
            reservation.setUserId(userId);
            reservation.setSeat(seat);
            reservation.setStatus(ReservationStatus.PENDING);
            reservation.setReservedAt(now);
            reservation.setExpiresAt(expiresAt);
            reservationRepository.save(reservation);

            seat.setStatus(SeatStatus.RESERVED);
            seatRepository.save(seat);

            return Optional.of(seatReservation);
        } finally {
            lock.unlock();
        }
    }

    public Optional<SeatReservation> findByReservationId(String reservationId) {
        RBucket<SeatReservation> bucket = redissonClient.getBucket(reservationKey(reservationId));
        return Optional.ofNullable(bucket.get());
    }

    private String seatCacheKey(Long eventId, Long seatId) {
        return CACHE_PREFIX + eventId + ":" + seatId;
    }

    private String reservationKey(String reservationId) {
        return RESERVATION_PREFIX + reservationId;
    }
}
