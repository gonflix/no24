package com.ticketing.ticketing.seat;

import java.time.Instant;
import java.util.List;

import org.redisson.api.RBucket;
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

    private static final String RESERVATION_PREFIX = "reservation:";

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

        int cycle = expired.size();
        for (Reservation r : expired) {

            // 결제 진행 중인 예약은 만료 처리하지 않음
            RBucket<SeatReservation> bucket = redissonClient.getBucket(reservationKey(r.getEid().toString()));
            if (bucket.isExists()) {
                if (bucket.get().getReservationStatus() == ReservationStatus.ONGOING) {
                    log.warn("Reservation {} is in ONGOING status in Redis, skipping expiration", r.getId());

                    cycle--;
                    continue;
                }

                bucket.delete(); // 예약 만료 처리 1: Redis 캐시 즉시 제거 (TTL 만료 전 명시적 삭제)
            }

            // 예약 만료 처리 2: 예약을 EXPIRED로 변경하고 좌석을 AVAILABLE로 되돌림
            r.setStatus(ReservationStatus.EXPIRED);

            Seat seat = r.getSeat();
            seat.setStatus(SeatStatus.AVAILABLE);

            log.info("Reservation {} expired, seat {} → AVAILABLE", r.getId(), seat.getId());
        }

        log.info("Expired {} pending reservations", cycle);
    }

    private String reservationKey(String reservationEid) {
        return RESERVATION_PREFIX + reservationEid;
    }
}
