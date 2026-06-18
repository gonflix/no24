package com.ticketing.ticketing.reservation;

import java.time.Instant;
import java.util.UUID;

import com.ticketing.ticketing.event.Event;
import com.ticketing.ticketing.seat.Seat;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "reservation")
@Getter
@Setter
@NoArgsConstructor
public class Reservation {

    @Id // 내부적으로 사용, 외부 노출 X
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, length = 36) // UUID (보안) / 외부 노출용 ID
    private UUID eid;

    @Column(nullable = false)
    private String userId;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "event_id", nullable = false)
    private Event event;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "seat_id", nullable = false)
    private Seat seat;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ReservationStatus status = ReservationStatus.PENDING;

    @Column(nullable = false)
    private Instant reservedAt;

    @Column(nullable = false)
    private Instant expiresAt;

    public static Reservation create(String userId, Seat seat, Instant now, Instant expiresAt) {
        Reservation r = new Reservation();
        r.userId = userId;
        r.eid = UUID.randomUUID(); // 외부 노출용 ID는 생성 시점에 UUID로 세팅
        r.seat = seat;
        r.event = seat.getEvent();
        r.status = ReservationStatus.PENDING;
        r.reservedAt = now;
        r.expiresAt = expiresAt;
        return r;
    }
}
