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
     * - 락 실패 → 409
     * - 락 성공 → 이중 체크 후 Redis 저장 + DB 저장 → 락 해제
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
        if (!lock.tryLock()) {
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

            // 4. DB 저장
            Reservation reservation = Reservation.create(userId, seat, now, expiresAt);
            reservationRepository.save(reservation);

            String eid = reservation.getEid().toString();
            Long id = reservation.getId();

            // 5. Redis 캐시 저장 (좌석 키 + 예약UUID 키)
            SeatReservation seatReservation = new SeatReservation(
                    id, eid, userId, eventId, seatId, now, expiresAt);
            seatBucket.set(seatReservation, Duration.ofMinutes(RESERVATION_TTL_MINUTES));
            redissonClient.<SeatReservation>getBucket(reservationKey(eid))
                    .set(seatReservation, Duration.ofMinutes(RESERVATION_TTL_MINUTES));

            seat.setStatus(SeatStatus.RESERVED);
            seatRepository.save(seat);

            return Optional.of(seatReservation);
        } finally {
            lock.unlock();
        }
    }

    public Optional<SeatReservation> findByReservationUuid(String reservationEid) {
        RBucket<SeatReservation> bucket = redissonClient.getBucket(reservationKey(reservationEid));
        return Optional.ofNullable(bucket.get());
    }

    private String seatCacheKey(Long eventId, Long seatId) {
        return CACHE_PREFIX + eventId + ":" + seatId;
    }

    private String reservationKey(String reservationEid) {
        return RESERVATION_PREFIX + reservationEid;
    }
}
