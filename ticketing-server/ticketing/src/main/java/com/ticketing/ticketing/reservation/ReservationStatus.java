package com.ticketing.ticketing.reservation;

public enum ReservationStatus {
    PENDING,    // 결제 대기
    ONGOING,    // 결제 중
    CONFIRMED,  // 결제 완료
    EXPIRED,    // 만료
    CANCELED    // 취소
}
