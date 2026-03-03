package com.ticketing.ticketing.seat;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RLock;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

@Service
public class SeatReservationService {

    private static final String RESERVATION_MAP = "ticketing:seat:reservations";
    private static final String LOCK_PREFIX = "ticketing:lock:seat:";
    private static final long RESERVATION_TTL = 10; // 예약 유지 시간 (분)

    private final RedissonClient redissonClient;

    public SeatReservationService(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    public Optional<SeatReservation> reserve(String userId, String seatId) {
        RLock lock = redissonClient.getLock(LOCK_PREFIX + seatId);
        boolean locked;
        try {
            locked = lock.tryLock(0, 0, TimeUnit.SECONDS); // Non-blocking, 즉시 반환
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // for Debug
            return Optional.empty();
        }

        // 락을 획득하지 못한 경우, 즉 다른 사용자가 이미 좌석을 예약 중인 경우
        if (!locked) {
            return Optional.empty();
        }

        try {
            RMapCache<String, SeatReservation> map = redissonClient.getMapCache(RESERVATION_MAP);
            // 이미 좌석이 예약된 경우
            if (map.containsKey(seatId)) {
                return Optional.empty();
            }

            // 좌석 예약 성공
            SeatReservation reservation = new SeatReservation(
                    UUID.randomUUID().toString(), // reservationId
                    userId,
                    seatId,
                    Instant.now());
            map.put(seatId, reservation, RESERVATION_TTL, TimeUnit.MINUTES);
            return Optional.of(reservation);
        } finally {
            lock.unlock();
        }
    }

    public Optional<SeatReservation> findByReservationId(String reservationId) {
        RMapCache<String, SeatReservation> map = redissonClient.getMapCache(RESERVATION_MAP);
        return map.values().stream()
                .filter(item -> item.reservationId().equals(reservationId))
                .findFirst();
    }
}
