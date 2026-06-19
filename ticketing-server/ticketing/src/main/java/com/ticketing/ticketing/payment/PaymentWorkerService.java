package com.ticketing.ticketing.payment;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.ticketing.ticketing.kafka.PaymentRequestedEvent;
import com.ticketing.ticketing.reservation.ReservationStatus;
import com.ticketing.ticketing.seat.SeatReservation;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class PaymentWorkerService {

    private static final String RESERVATION_PREFIX = "reservation:";
    private static final String PAYMENT_LOCK_PREFIX = "paymentlock:";
    private static final long PAYMENT_TTL_MINUTES = 10;

    private final RedissonClient redissonClient;
    private final PaymentResultStore paymentResultStore;
    private final PaymentConfirmHelper paymentConfirmHelper;
    private final Counter completedCounter;

    public PaymentWorkerService(
            RedissonClient redissonClient,
            PaymentResultStore paymentResultStore,
            PaymentConfirmHelper paymentConfirmHelper,
            MeterRegistry meterRegistry) {
        this.redissonClient = redissonClient;
        this.paymentResultStore = paymentResultStore;
        this.paymentConfirmHelper = paymentConfirmHelper;
        this.completedCounter = Counter.builder("payment.completed")
                .description("Completed payment attempts")
                .register(meterRegistry);
    }

    @Async("paymentWorkerExecutor")
    public void execute(PaymentRequestedEvent event) {

        // 결제 처리 중복 방지: 예약 단위로 락 획득 시도 (락 범위는 Redis 조회 + 상태 체크 + ONGOING 상태 업데이트까지 최소화)
        RLock lock = redissonClient.getLock(paymentLockKey(event.reservationEid()));
        boolean locked;
        try {
            // Kafka 재전달 이벤트가 첫 번째 락 해제 직후 도착해도 락 획득 후 ONGOING 상태 확인하고 정상 종료
            locked = lock.tryLock(3, -1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while acquiring payment lock. reservationEid={}", event.reservationEid());
            return;
        }
        if (!locked) {
            log.warn("Could not acquire payment lock — another worker is processing. reservationEid={}",
                    event.reservationEid());
            return;
        }

        SeatReservation seatReservation;
        try {
            // Redis에서 예약 정보 조회 (락 안에서 수행 — TOCTOU 방지)
            RBucket<SeatReservation> bucket = redissonClient.getBucket(reservationKey(event.reservationEid()));
            if (!bucket.isExists()) {
                log.error("Reservation not found for payment processing. reservationEid={}", event.reservationEid());
                return;
            }

            // 단일 읽기 — check와 mutation 모두 동일 객체 사용
            // TODO: 캐시 기간을 얼마나로 정해야 하나? => 결제 모듈 평균 응답 시간 + 여유 시간 (예: 10분)??
            seatReservation = bucket.get();
            if (seatReservation.getReservationStatus() != ReservationStatus.PENDING) {
                log.error("Reservation is not in PENDING status for payment processing. reservationEid={}",
                        event.reservationEid());
                return;
            }

            // ONGOING 상태로 업데이트 + TTL 설정 (ONGOING 유령화 방지)
            // 참고: DB Reservation 상태는 Pending 상태로 유지
            seatReservation.setReservationStatus(ReservationStatus.ONGOING);
            bucket.set(seatReservation, Duration.ofMinutes(PAYMENT_TTL_MINUTES));

        } finally {
            lock.unlock(); // 결제 I/O 전에 즉시 해제 — 락 범위를 최소화
        }

        // 결제모듈 딜레이 시뮬레이션 (Stripe, PayPal, etc..)
        try {
            int delayMs = 1000 + ThreadLocalRandom.current().nextInt(1000); // 1~2초 랜덤 지연
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Worker interrupted. reservationId={}", event.reservationId());
            return;
        }
        boolean success = true; // 결제모듈 결제 성공률 100% 고정 (테스트용)
        Instant processedAt = Instant.now();

        if (success) {
            // 결제모듈 성공 시 DB에 최종 확정 처리
            try {
                paymentConfirmHelper.confirm(
                        event.reservationId(),
                        event.userId(),
                        event.amount(),
                        event.paymentMethod(),
                        processedAt);

                // 결제 성공 + DB 저장 성공: Redis에서 예약 정보 제거 => 결제 재요청 방지
                redissonClient.<SeatReservation>getBucket(reservationKey(event.reservationEid())).delete();
                log.info("Redis reservation bucket removed. reservationEid={}", event.reservationEid());
                completedCounter.increment(); // 처리를? 아니면 결제를?

            } catch (Exception e) {
                // DB 저장 실패: 보상 트랜젝션
                // : 결제모듈에 취소 요청 (실제 결제 모듈 연동 전까지 pseudocode)
                log.error("DB 저장 실패 - 결제 취소 처리 시작. reservationEid={}", event.reservationEid(), e);
                cancelPaymentOnDbFailure(event.reservationEid(), event.amount());
                success = false;
            }

        } else {
            log.error("Payment API failed. reservationEid={}", event.reservationEid());
        }

        PaymentResultEvent result = new PaymentResultEvent(
                event.reservationId(),
                event.reservationEid(),
                event.userId(),
                success,
                success ? "결제가 성공했습니다." : "결제가 실패했습니다.",
                processedAt);

        paymentResultStore.save(result); // Redis에 결제 결과 저장
    }

    /**
     * DB 저장 실패 시 결제모듈 취소 API를 호출한다.
     * TODO: 실제 결제모듈(Stripe, PayPal 등) 연동 시 아래 pseudocode를 실제 API 호출로 대체할 것.
     */
    private void cancelPaymentOnDbFailure(String reservationEid, long amount) {
        // [PSEUDOCODE] ── 실제 결제모듈 연동 시 이 블록을 구현 ────────────────────────────
        //
        // 1. 결제모듈에서 해당 예약의 pgTid(거래 ID) 조회
        // String pgTid = paymentModuleClient.findTransactionId(reservationEid);
        //
        // 2. 결제모듈 취소 API 호출
        // PaymentCancelResponse response = paymentModuleClient.cancel(
        // pgTid, amount, "DB 저장 실패로 인한 자동 취소");
        //
        // 3. 취소 결과 처리
        // if (response.isSuccess()) {
        // log.info("결제 취소 성공. reservationEid={}, pgTid={}", reservationEid, pgTid);
        // } else {
        // log.error("결제 취소 실패 - 수동 처리 필요. reservationEid={}, pgTid={}", reservationEid,
        // pgTid);
        // // 운영팀 알림 발송 (Slack / PagerDuty 등)
        // // alertService.sendCriticalAlert("결제 취소 실패", reservationEid);
        // }
        // ────────────────────────────────────────────────────────────────────────────────

        log.warn("[STUB] 결제 취소 API 미구현 - 수동 확인 필요. reservationEid={}, amount={}", reservationEid, amount);
    }

    private String reservationKey(String reservationEid) {
        return RESERVATION_PREFIX + reservationEid;
    }

    private String paymentLockKey(String reservationEid) {
        return PAYMENT_LOCK_PREFIX + reservationEid;
    }

}
