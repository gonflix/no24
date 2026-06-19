package com.ticketing.ticketing.controller;

import java.util.Map;
import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ticketing.ticketing.dto.ApiResponse;
import com.ticketing.ticketing.dto.PaymentRequest;
import com.ticketing.ticketing.dto.SeatReservationRequest;
import com.ticketing.ticketing.kafka.PaymentRequestedEvent;
import com.ticketing.ticketing.payment.PaymentProducer;
import com.ticketing.ticketing.payment.PaymentResultEvent;
import com.ticketing.ticketing.payment.PaymentResultStore;
import com.ticketing.ticketing.seat.SeatReservation;
import com.ticketing.ticketing.seat.SeatReservationService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class TicketingController {

        private final SeatReservationService seatReservationService;
        private final PaymentProducer paymentProducer;
        private final PaymentResultStore paymentResultStore;

        @PostMapping("/seats/reserve")
        public ResponseEntity<ApiResponse> reserve(@Valid @RequestBody SeatReservationRequest request,
                        Authentication authentication) {
                String userId = authentication.getName();
                Optional<SeatReservation> result = seatReservationService.reserve(
                                userId, request.eventId(), request.seatId());

                if (result.isEmpty()) {
                        return ResponseEntity.status(HttpStatus.CONFLICT)
                                        .body(ApiResponse.fail("좌석 확보에 실패했습니다."));
                }

                SeatReservation reservation = result.get();
                return ResponseEntity.ok(ApiResponse.success("좌석 확보에 성공했습니다.", Map.of(
                                "reservationEid", reservation.getReservationEid(),
                                "seatId", reservation.getSeatId(),
                                "expiresAt", reservation.getExpiresAt().toString())));
        }

        @PostMapping("/payments/request")
        public ResponseEntity<ApiResponse> requestPayment(@Valid @RequestBody PaymentRequest request,
                        Authentication authentication) {
                String userId = authentication.getName();

                SeatReservation reservation = seatReservationService
                                .findByReservationUuid(request.reservationEid())
                                .orElseThrow(() -> new IllegalArgumentException("Invalid reservation UUID"));
                if (!reservation.getUserId().equals(userId)) {
                        log.warn("requestPayment: userId mismatch. expected={}, actual={}", reservation.getUserId(),
                                        userId);
                        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                                        .body(ApiResponse.fail("접근 권한이 없습니다."));
                }
                try {
                        paymentProducer.send(new PaymentRequestedEvent(reservation.getId(), request.reservationEid(),
                                        userId, request.amount(), request.paymentMethod()));
                } catch (IllegalStateException e) {
                        log.error("requestPayment: failed to publish event. reservationEid={}",
                                        request.reservationEid(), e);
                        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                                        .body(ApiResponse.fail("결제 요청 처리 중 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해주세요."));
                }

                return ResponseEntity.accepted().body(ApiResponse.success("결제 요청이 접수되었습니다.", Map.of(
                                "reservationEid", request.reservationEid())));
        }

        // 결제 결과 폴링 — 클라이언트가 3초 간격으로 조회, 30초 초과 시 클라이언트에서 실패 처리
        @GetMapping("/payments/status/{reservationEid}")
        public ResponseEntity<ApiResponse> getPaymentStatus(@PathVariable String reservationEid,
                        Authentication authentication) {
                String userId = authentication.getName();

                Optional<PaymentResultEvent> result = paymentResultStore.get(reservationEid);
                if (result.isEmpty()) {
                        return ResponseEntity.accepted()
                                        .body(ApiResponse.success("결제 처리 중입니다.", null));
                }

                PaymentResultEvent paymentResult = result.get();
                if (!paymentResult.userId().equals(userId)) {
                        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                                        .body(ApiResponse.fail("접근 권한이 없습니다."));
                }

                if (paymentResult.success()) {
                        return ResponseEntity.ok(ApiResponse.success("결제가 완료되었습니다.", Map.of(
                                        "reservationEid", paymentResult.reservationEid(),
                                        "processedAt", paymentResult.processedAt().toString())));
                }
                return ResponseEntity.ok(ApiResponse.fail("결제가 실패했습니다."));
        }
}
