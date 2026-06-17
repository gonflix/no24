# NO24 - Concert Ticketing System

대규모 동시 접속을 처리하는 콘서트 티켓팅 시스템입니다. 대기열 관리, 좌석 예약, 결제 처리를 마이크로서비스 아키텍처로 구현했습니다.

---

## 아키텍처 개요
![ex_screenshot](ticketing-server/ticketing/src/main/resources/static/no24_architecture.png)
---

## 전체 처리 흐름

### Step 1 — 대기열 입장
유저가 `GET /enter?event_id=&user_id=` 로 API Server에 접속한다. SSE 연결이 수립되고, Redis Sorted Set(`waiting:{event_id}`)에 삽입 시각(UnixNano)을 score로 유저를 등록한다.

### Step 2 — 대기 순위 전송 (2-Tier 최적화)
SSE 연결 중 3초 간격으로 현재 순위를 유저에게 전송한다. Redis 부하를 줄이기 위해 순위 조회에 2-Tier 전략을 적용한다.

| Tier | 대상 | 순위 조회 방법 |
|---|---|---|
| **Tier 1** | 상위 100명 또는 상위 1% | Redis 직접 조회 → 정확한 순위 |
| **Tier 2** | 나머지 | 1분 주기 인메모리 스냅샷 → 근사 순위 |

### Step 3 — JWT 토큰 발급
`waitingQueueWorker`가 Redis ZSet에서 FIFO(ZPopMin)로 유저를 꺼낸다. `user_id`·`event_id` 클레임이 담긴 RS256 JWT(유효 10분)를 생성한 뒤 SSE로 전달한다. 해당 유저가 다른 replica에 연결된 경우 Kafka(`wait-queue-events`)로 브로드캐스트하여 해당 replica가 SSE로 전달하도록 한다. 토큰 수신 후 SSE 연결은 종료된다.

### Step 4 — 좌석 예약
유저는 JWT를 `Authorization: Bearer` 헤더에 담아 `POST /api/seats/reserve`를 호출한다.

- Ticketing Server는 Spring Security OAuth2 Resource Server로 JWT를 검증한다.
- Redisson `tryLock(0, 0, SECONDS)` — 비차단 분산 락으로 좌석을 선점한다.
  - 선점 실패(이미 예약된 좌석) → 409 반환, 재시도 불가
  - 선점 성공 → Redis에 10분 TTL로 예약 캐시 저장 + DB에 `Reservation(PENDING)` 및 `Seat(RESERVED)` 즉시 저장 → `reservationId`(UUID) 반환

### Step 5 — 결제 요청 (Kafka 발행)
유저가 `POST /api/payments/request`를 호출하면 예약 유효성을 검증한 후 `PaymentRequestedEvent`를 Kafka 토픽(`ticketing.payment.requested`)에 발행하고 202 Accepted를 즉시 반환한다.

### Step 6 — 결제 처리 (워커 풀)
`PaymentProcessor`(Kafka consumer, concurrency=5)가 이벤트를 소비하여 `PaymentWorkerService`에 위임한다. 워커는 별도 스레드 풀(`paymentWorkerExecutor`, threads=100)에서 실행된다.

```
1. PG 호출 시뮬레이션 (1~2초 랜덤 지연)
2. 결과를 Redis에 저장 (payment:results, 10분 TTL)
3. 성공 시 DB 업데이트 — Reservation CONFIRMED + Seat SOLD + Payment 저장
```

### Step 7 — 결제 결과 폴링
유저는 `GET /api/payments/status/{reservationId}`를 **3초 간격**으로 폴링한다.

| 응답 | 의미 |
|---|---|
| `202 Accepted` | 아직 처리 중 → 계속 폴링 |
| `200 OK` + `success: true` | 결제 완료 |
| `200 OK` + `success: false` | 결제 실패 |

**30초 이상 202가 지속되면 클라이언트에서 타임아웃 실패 처리**한다.

---

## 서비스 구성 (Monorepo)

| 서비스 | 언어/프레임워크 | 역할 |
|---|---|---|
| `api-server` | Go 1.25 + Echo v5 | 대기열 관리, 순번 발급, JWT 발행 |
| `ticketing-server` | Java 21 + Spring Boot 4.0 | 좌석 예약, 결제 처리 |

---

## 기술 스택

### Infrastructure
- **Kafka** — 결제 이벤트 비동기 처리 (`ticketing.payment.requested`) 및 API Server replica 간 SSE 토큰 브로드캐스트
- **Redis** — 대기열 Sorted Set, 분산 락(Redisson), 좌석 예약 캐시
- **PostgreSQL** — 좌석·예약·결제 영구 저장
- **Kubernetes** — 전체 인프라 오케스트레이션

### API Server (Go)
- Echo v5 — HTTP 라우터
- SSE — 대기 순위 및 JWT 토큰 실시간 전송
- Kafka Go client — 토큰 브로드캐스트 (다중 replica 동기화)
- Redis Sorted Set — 대기열 FIFO 관리 + 2-Tier 스냅샷 캐시
- AWS Secrets Manager — 프로덕션 환경 RSA 키 로딩

### Ticketing Server (Java)
- Spring Security + OAuth2 Resource Server — Bearer JWT 검증 (JWKS 원격 조회)
- Redisson — 좌석별 RLock 분산 락 + RBucket 예약 캐시 + RMapCache 결제 결과 저장
- Spring Kafka — 결제 이벤트 Producer / Consumer (concurrency=5, 파티션 수 일치)
- `PaymentWorkerService` — `@Async` 워커 풀 (threads=100, CallerRunsPolicy backpressure)
- Spring Data JPA (PostgreSQL) — 예약·결제 영구 저장
- `SeatExpiryScheduler` — 60초 간격으로 만료된 PENDING 예약을 EXPIRED 처리, 좌석 AVAILABLE 복원 및 Redis 캐시 삭제

---

## API Reference

### API Server

| Method | Path | 설명 |
|---|---|---|
| `GET` | `/enter` | 대기열 입장 + SSE 연결 (`?event_id=&user_id=`) |
| `GET` | `/.well-known/jwks.json` | RSA 공개 키 (JWKS 형식) |

**SSE 이벤트 포맷 (3초 간격 순위 전송):**
```json
{"seq": 42}
```

**SSE 이벤트 포맷 (대기열 통과 시 — 연결 종료):**
```json
{"seq": 1, "token": "<jwt_token>"}
```

---

### Ticketing Server

모든 엔드포인트는 `Authorization: Bearer {jwt_token}` 헤더가 필요합니다.

| Method | Path | 설명 |
|---|---|---|
| `POST` | `/api/seats/reserve` | 좌석 예약 (분산 락 선점 + 즉시 DB 저장) |
| `POST` | `/api/payments/request` | 결제 요청 접수 (Kafka 발행 → 202 반환) |
| `GET` | `/api/payments/status/{reservationId}` | 결제 결과 폴링 |

**좌석 예약 응답 (200 OK):**
```json
{
  "success": true,
  "message": "좌석 확보에 성공했습니다.",
  "data": {
    "reservationId": "550e8400-e29b-41d4-a716-446655440000",
    "seatId": 1,
    "expiresAt": "2026-06-18T12:44:56.789Z"
  }
}
```

**좌석 예약 실패 (409 Conflict):**
```json
{"success": false, "message": "좌석 확보에 실패했습니다.", "data": null}
```

**결제 요청 응답 (202 Accepted):**
```json
{
  "success": true,
  "message": "결제 요청이 접수되었습니다.",
  "data": {"reservationId": "550e8400-..."}
}
```

**결제 결과 폴링 — 처리 중 (202 Accepted):**
```json
{"success": true, "message": "결제 처리 중입니다.", "data": null}
```

**결제 결과 폴링 — 완료 (200 OK):**
```json
{
  "success": true,
  "message": "결제가 완료되었습니다.",
  "data": {
    "reservationId": "550e8400-...",
    "processedAt": "2026-06-18T12:34:56.789Z"
  }
}
```

---

## 주요 설계 결정

### 대기열 2-Tier 스냅샷
- 수십만 명이 대기 중일 때 3초마다 전원 Redis 조회 시 부하 폭증
- 상위 소수(Tier 1)만 실시간 조회하고, 나머지(Tier 2)는 1분 주기 인메모리 스냅샷 활용
- 클라이언트 체감 지연 없이 Redis 부하를 대폭 절감

### 좌석 예약 동기 DB 저장
- 좌석 선점 시 Redis 캐시와 DB를 같은 요청에서 저장하여 일관성 보장
- Redisson 분산 락 내에서 `Reservation(PENDING)` + `Seat(RESERVED)` 를 단일 트랜잭션으로 커밋
- Kafka 비동기 전달 지연·유실로 인한 Redis-DB 불일치 제거

### 결제 비동기 처리 + 워커 풀
- 결제 요청량이 순간적으로 급증할 수 있으므로 Kafka로 요청을 버퍼링하여 워커 처리량에 맞게 소비
- Kafka consumer concurrency=5 (파티션 수와 일치), 실제 PG 호출은 별도 스레드 풀(threads=100)에서 처리
- `CallerRunsPolicy`: 워커 큐가 포화되면 Kafka consumer 스레드가 직접 실행 → backpressure로 자연스러운 속도 조절
- 처리 결과는 Redis(`payment:results`, 10분 TTL)에 저장, 클라이언트가 3초 간격 폴링으로 수신

### 분산 락 (Redisson RLock)
- 좌석별 `ticketing:lock:seat:{eventId}:{seatId}` 키로 락 적용
- `tryLock(0, 0, SECONDS)` — 대기 없이 즉시 실패, 재시도 없음
- 이중 예약 완전 방지, 동시 요청 수백 건에도 단 한 건만 선점 성공

### 예약 만료 스케줄러 (SeatExpiryScheduler)
- 60초 간격으로 PENDING 상태이고 `expiresAt`이 지난 예약을 일괄 조회 (JOIN FETCH로 N+1 방지)
- Reservation → EXPIRED, Seat → AVAILABLE 상태 전환 + Redis 예약 캐시 즉시 삭제
- 워커가 결제 성공 시 Reservation을 CONFIRMED로 전환하므로 정상 결제된 예약은 만료 대상에서 자동 제외

### Kafka 토큰 브로드캐스트
- API Server replica가 여러 개일 때 JWT 발급 인스턴스 ≠ SSE 연결 인스턴스 가능
- 발급 인스턴스가 `wait-queue-events` 토픽에 발행 → 모든 replica가 소비하여 자신의 SSE 허브 검색
- hostname 기반 GroupID로 각 replica가 동일 메시지를 모두 수신 (fan-out)

---

## 데이터베이스 스키마

```sql
CREATE TABLE seat (
    id       BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    event_id BIGINT NOT NULL,
    grade    VARCHAR(50) NOT NULL,
    section  VARCHAR(50) NOT NULL,
    status   VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE'  -- AVAILABLE | RESERVED | SOLD
);

CREATE TABLE reservation (
    id          UUID PRIMARY KEY,
    user_id     VARCHAR(255) NOT NULL,
    seat_id     BIGINT NOT NULL REFERENCES seat(id),
    status      VARCHAR(20) NOT NULL,                  -- PENDING | ONGOING | CONFIRMED | EXPIRED
    reserved_at TIMESTAMP NOT NULL,
    expires_at  TIMESTAMP NOT NULL
);

CREATE TABLE payment (
    id             UUID PRIMARY KEY,
    reservation_id UUID NOT NULL REFERENCES reservation(id),
    user_id        VARCHAR(255) NOT NULL,
    amount         BIGINT NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    pg_tid         VARCHAR(255),
    status         VARCHAR(20) NOT NULL,               -- PAID | REFUNDED
    paid_at        TIMESTAMP NOT NULL
);
```

---

## Redis 키 목록

| 키 패턴 | 자료구조 | TTL | 용도 |
|---|---|---|---|
| `waiting:{event_id}` | Sorted Set | 없음 | 대기열 (score = 삽입 시각 UnixNano) |
| `reserve:{eventId}:{seatId}` | RBucket | 10분 | 좌석 선점 캐시 (중복 예약 빠른 차단) |
| `reservation:{reservationId}` | RBucket | 10분 | reservationId 기반 예약 조회 캐시 |
| `ticketing:lock:seat:{eventId}:{seatId}` | RLock | 없음 | 좌석 분산 락 |
| `payment:results` | RMapCache | 10분 | 결제 결과 (key: reservationId, value: JSON) |
