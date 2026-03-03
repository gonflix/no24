package com.ticketing.ticketing.controller;

import java.util.Map;
import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ticketing.ticketing.dto.ApiResponse;
import com.ticketing.ticketing.dto.PaymentRequest;
import com.ticketing.ticketing.dto.SeatReservationRequest;
import com.ticketing.ticketing.kafka.PaymentRequestedEvent;
import com.ticketing.ticketing.notification.PushNotificationService;
import com.ticketing.ticketing.payment.PaymentProducer;
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
    private final PushNotificationService pushNotificationService;

    // 좌석 예약 API
    @PostMapping("/seats/reserve")
    public ResponseEntity<ApiResponse> reserve(@Valid @RequestBody SeatReservationRequest request,
            Authentication authentication) {
        String userId = authentication.getName();
        Optional<SeatReservation> result = seatReservationService.reserve(userId, request.seatId());
        if (result.isEmpty()) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(ApiResponse.fail("좌석 확보에 실패했습니다."));
        }

        SeatReservation reservation = result.get();
        return ResponseEntity.ok(ApiResponse.success("좌석 확보에 성공했습니다.", Map.of(
                "reservationId", reservation.reservationId(),
                "seatId", reservation.seatId())));
    }

    // 결제 API(좌석 예약 성공)
    @PostMapping("/payments/request")
    public ResponseEntity<ApiResponse> requestPayment(@Valid @RequestBody PaymentRequest request,
            Authentication authentication) {
        String userId = authentication.getName();
        Optional<SeatReservation> reservation = seatReservationService.findByReservationId(request.reservationId());
        if (reservation.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ApiResponse.fail("유효하지 않은 예약입니다."));
        }
        if (!reservation.get().userId().equals(userId)) {
            log.error("requestPayment: userId {} not equal {}", userId, reservation.get().userId());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ApiResponse.fail("유효하지 않은 예약입니다."));
        }

        try {
            paymentProducer.send(new PaymentRequestedEvent(request.reservationId(), userId, request.amount()));
        } catch (IllegalStateException e) {
            log.error("requestPayment: failed to publish payment request for reservationId={}", request.reservationId(),
                    e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(ApiResponse.fail("결제 요청 처리 중 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해주세요."));
        }

        return ResponseEntity.ok(ApiResponse.success("결제 요청이 접수되었습니다.", Map.of(
                "reservationId", request.reservationId())));
    }

    @GetMapping("/notifications/subscribe")
    public SseEmitter subscribe(Authentication authentication) {
        return pushNotificationService.subscribe(authentication.getName());
    }
}
