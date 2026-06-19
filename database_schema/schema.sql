CREATE DATABASE IF NOT EXISTS ticketing
DEFAULT CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE ticketing;

CREATE TABLE events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    status ENUM('A', 'N') DEFAULT 'A',              -- A: Active, N: Not Active
    start_at TIMESTAMP NOT NULL,
    end_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- INSERT INTO events (name, status, start_at, end_at) VALUES ('bts_2026_seoul', 'A', NOW(), NOW());
-- INSERT INTO events (name, status, start_at, end_at) VALUES ('newjeans_2027_seoul', 'N', NOW(), NOW());
-- INSERT INTO events (name, status, start_at, end_at) VALUES ('ive_2026_seoul', 'A', NOW(), NOW());

CREATE TABLE IF NOT EXISTS seat (
    id         BIGINT       NOT NULL AUTO_INCREMENT,
    event_id   BIGINT       NOT NULL,
    grade      VARCHAR(50)  NOT NULL,
    section    VARCHAR(50)  NOT NULL,
    status     VARCHAR(20)  NOT NULL DEFAULT 'AVAILABLE',
    PRIMARY KEY (id),
    CONSTRAINT chk_seat_status CHECK (status IN ('AVAILABLE', 'RESERVED', 'SOLD'))
) ENGINE=InnoDB;

CREATE INDEX idx_seat_event_id     ON seat (event_id);
CREATE INDEX idx_seat_event_status ON seat (event_id, status);

-- =============================================

CREATE TABLE IF NOT EXISTS reservation (
    id          BIGINT       NOT NULL AUTO_INCREMENT,                   -- clustered index (성능 최적화) / 내부적으로 사용, 외부 노출 X
    eid        CHAR(36)     NOT NULL ,                    -- exposible ID UUID (보안) / 외부 노출용 ID
    user_id     VARCHAR(255) NOT NULL,
    seat_id     BIGINT       NOT NULL,
    event_id    INT         NOT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
    reserved_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at  TIMESTAMP    NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT chk_reservation_status CHECK (status IN ('PENDING', 'ONGOING', 'CONFIRMED', 'EXPIRED', 'CANCELED')),
    CONSTRAINT fk_reservation_seat FOREIGN KEY (seat_id) REFERENCES seat (id),
    CONSTRAINT fk_reservation_event FOREIGN KEY (event_id) REFERENCES events (id)
) ENGINE=InnoDB;

CREATE INDEX idx_reservation_seat_id  ON reservation (seat_id);
CREATE INDEX idx_reservation_event_id ON reservation (event_id);
CREATE INDEX idx_reservation_user_id ON reservation (user_id);
CREATE INDEX idx_reservation_status  ON reservation (status);
CREATE INDEX idx_reservation_eid  ON reservation (eid);
-- 만료 스케줄러가 PENDING 상태의 만료 예약을 조회할 때 사용 (MySQL은 부분 인덱스 미지원으로 전체 인덱스로 대체)
CREATE INDEX idx_reservation_expires ON reservation (expires_at);

-- =============================================

CREATE TABLE IF NOT EXISTS payment (
    id              BIGINT       NOT NULL AUTO_INCREMENT,
    reservation_id  BIGINT       NOT NULL,
    user_id         VARCHAR(255) NOT NULL,
    tot_amount      BIGINT       NOT NULL,
    payment_method  VARCHAR(50),
    pg_tid          VARCHAR(255),
    pg_provider     VARCHAR(100),
    status          VARCHAR(20)  NOT NULL,
    paid_at         TIMESTAMP    NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_payment_reservation_id (reservation_id),
    CONSTRAINT chk_payment_status CHECK (status IN ('PAID', 'REFUNDED')),
    CONSTRAINT fk_payment_reservation FOREIGN KEY (reservation_id) REFERENCES reservation (id)
) ENGINE=InnoDB;

CREATE INDEX idx_payment_user_id ON payment (user_id);
