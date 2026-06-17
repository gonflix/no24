package com.ticketing.ticketing.seat;

import java.time.Instant;
import java.util.List;

import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.ticketing.ticketing.reservation.Reservation;
import com.ticketing.ticketing.reservation.ReservationRepository;
import com.ticketing.ticketing.reservation.ReservationStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class SeatExpiryScheduler {

    private final ReservationRepository reservationRepository;
    private final RedissonClient redissonClient;

    /**
     * 매 분마다 PENDING 상태이면서 만료 시각이 지난 예약을 EXPIRED 처리하고
     * 해당 좌석을 AVAILABLE 로 되돌린다.
     * JOIN FETCH 로 N+1 방지.
     */
    @Scheduled(fixedDelay = 60_000)
    @Transactional
    public void expireReservations() {
        List<Reservation> expired = reservationRepository.findExpiredPendingReservations(Instant.now());
        if (expired.isEmpty()) {
            return;
        }

        log.info("Expiring {} pending reservations", expired.size());
        for (Reservation r : expired) {
            r.setStatus(ReservationStatus.EXPIRED);

            Seat seat = r.getSeat();
            seat.setStatus(SeatStatus.AVAILABLE);

            // Redis 캐시도 즉시 제거 (TTL 만료 전 명시적 삭제)
            String cacheKey = "reserve:" + seat.getEventId() + ":" + seat.getId();
            redissonClient.getBucket(cacheKey).delete();

            log.info("Reservation {} expired, seat {} → AVAILABLE", r.getId(), seat.getId());
        }
    }
}
