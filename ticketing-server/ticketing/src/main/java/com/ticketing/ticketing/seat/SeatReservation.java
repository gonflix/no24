package com.ticketing.ticketing.seat;

import java.io.Serializable;
import java.time.Instant;

import com.ticketing.ticketing.reservation.ReservationStatus;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SeatReservation implements Serializable {

        private Long id;
        private String reservationEid;
        private String userId;
        private Long eventId;
        private Long seatId;
        private ReservationStatus reservationStatus;
        private Instant reservedAt;
        private Instant expiresAt;
}
